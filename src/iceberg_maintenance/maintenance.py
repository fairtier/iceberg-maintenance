"""Nightly Iceberg maintenance for a box warehouse: compaction + expiry.

Walks every table in the warehouse and
  1. where a table has accumulated enough small data files, rewrites it by
     streaming the scan straight into fresh data files — no whole-table
     materialization, so peak memory is O(1) in table size and there is no
     size cap (stream_batches -> chunked write; note that PyIceberg's own
     scan().to_arrow_batch_reader() is NOT lazy — see stream_batches);
  2. expires snapshots past the time-travel window (metadata-only: branch
     heads and tags are never expired, and a retention floor of the newest
     snapshots is kept).

Safe against concurrent dlt loads: Iceberg's optimistic concurrency turns a
clash into a failed commit here (logged, retried next night), never corrupted
data.

Deliberately NOT handled here:
  - orphan-file removal — no OSS option exists. Lakekeeper's
    remove_orphan_files task queue is Enterprise-only, and PyIceberg's
    implementation (PR #1958) died unmerged. Until an orphan sweep exists,
    files unreferenced by expired snapshots stay in object storage (expiry
    above only trims metadata);
  - tables with delete files (DuckFlight/DuckDB writes are merge-on-read) —
    a COW rewrite through PyIceberg's scan would need delete-file semantics
    PyIceberg only partially applies, so those tables are skipped loudly
    (snapshot expiry still runs — it never touches data files).

The full design lives in docs/plans/iceberg-maintenance.md in the FairTier
platform repo.
"""

import gc
import itertools
import logging
import time
import uuid

import pyarrow
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import AlwaysTrue

# NOTE: `_dataframe_to_data_files` and `_record_batches_from_scan_tasks_and_deletes`
# (used via ArrowScan in stream_batches) are PyIceberg-internal (underscore)
# helpers. Depending on them is deliberate and safe *because the version is
# pinned* (pyproject.toml pins pyiceberg==0.11.1) — internals can't shift under
# a frozen pin. `_dataframe_to_data_files` is the same function
# `Table.overwrite` calls; we drive it directly only to feed it a streamed
# reader in chunks instead of one materialized table, which the released 0.11.1
# public API refuses (see AWAITING-UPSTREAM in compact_table). Revisit on every
# pyiceberg bump.
from pyiceberg.io.pyarrow import (
    ArrowScan,
    PyArrowFileIO,
    _dataframe_to_data_files,
    schema_to_pyarrow,
)

from .config import MIB, Config, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("maintenance")


def direct_s3_io(cfg: Config) -> PyArrowFileIO:
    """FileIO with the box's own storage credentials (fairtier-storage).

    The catalog-configured IO can't be used: Lakekeeper forces
    py-io-impl=FsspecFileIO + s3.signer=S3V4RestSigner as server-side
    overrides, and PyIceberg's S3V4RestSigner event handler never fires in
    async s3fs — a known PyIceberg bug. Replacing table.io with a
    direct-credential PyArrowFileIO after load_table() is the established
    workaround (dlt-worker sidesteps the same bug via AWS env vars).
    """
    return PyArrowFileIO(
        {
            "s3.endpoint": cfg.aws_endpoint_url,
            "s3.access-key-id": cfg.aws_access_key_id,
            "s3.secret-access-key": cfg.aws_secret_access_key,
            "s3.region": cfg.aws_region,
        }
    )


def release_memory() -> None:
    """Return arena memory to the OS between tables.

    The per-table rewrite streams (O(1) in table size), but pyarrow still
    parks freed buffers in its memory pool and Python holds onto arena pages,
    so absent this the run's RSS creeps up as a high-water mark across a
    many-table warehouse rather than settling back down (the dlt-worker hit
    the same post-run high-water). Cheap enough to call unconditionally after
    every table.
    """
    gc.collect()
    try:
        pyarrow.default_memory_pool().release_unused()
    except Exception:
        # Best-effort — release_unused is an optimization, never fatal.
        log.debug("pyarrow release_unused failed (ignored)", exc_info=True)


def all_namespaces(catalog):
    """Namespaces at every depth (Lakekeeper allows nesting; dlt stays flat)."""
    out = []
    stack = list(catalog.list_namespaces())
    while stack:
        ns = stack.pop()
        out.append(ns)
        stack.extend(catalog.list_namespaces(ns))
    return out


def stream_batches(scan, tasks):
    """Yield the scan's record batches lazily — one data file, one batch at a time.

    Deliberately NOT `scan.to_arrow_batch_reader()`. Despite the name (and its
    docstring promising "less memory ... a RecordBatch is read one at a time"),
    0.11.1's `ArrowScan.to_record_batches` does:

        executor.map(lambda task: list(batches_of(task)), tasks)

    `Executor.map` submits EVERY file up front, each worker materializes one
    whole data file's batches into a list, and the results of finished futures
    sit in memory until the consumer reaches them. Our consumer — parquet
    encode + upload to object storage — is far slower than the readers, so
    those buffered lists pile up toward the whole table: peak memory is O(table
    size), not O(batch), and no chunk size can bound it. That is what
    OOM-killed the nightly job on a box at its 1Gi limit on 2026-07-31 (RSS
    pinned at the cap, node into swapless thrash, no log line naming the table
    it died on).

    Driving `_record_batches_from_scan_tasks_and_deletes` gives byte-identical
    batches (same projection, same delete handling, same `.cast` to the
    projected schema as `to_arrow_batch_reader` applies) from a plain
    generator: one file open at a time, one batch in flight, and the reader
    only advances when we ask for the next batch.

    Passing `deletes_per_file={}` is correct, not a shortcut: `compact_table`
    returns early for any table that has delete files, so there are none to
    apply here.
    """
    arrow_scan = ArrowScan(
        scan.table_metadata,
        scan.io,
        scan.projection(),
        scan.row_filter,
        scan.case_sensitive,
        scan.limit,
    )
    batches = arrow_scan._record_batches_from_scan_tasks_and_deletes(tasks, {})
    target_schema = schema_to_pyarrow(scan.projection())
    # The cast is what makes batches read from files with different physical
    # types (e.g. string vs large_string across dlt schema evolution) concat
    # into one chunk — DataScan.to_arrow_batch_reader wraps its batches exactly
    # the same way.
    reader = pyarrow.RecordBatchReader.from_batches(target_schema, batches).cast(
        target_schema
    )
    yield from reader


def compact_table(table, s3_io, cfg: Config) -> str:
    """Compact one table if it needs it; returns a human-readable outcome."""
    snapshot = table.current_snapshot()
    if snapshot is None:
        return "empty (no snapshot)"

    # Merge-on-read delete files (DuckFlight UPDATE/DELETE/MERGE) make a
    # naive COW rewrite unsafe — skip and say so.
    summary = snapshot.summary or {}
    if int(summary.get("total-delete-files", 0) or 0) > 0:
        return "SKIPPED: has delete files (merge-on-read writes) — compaction not supported yet"

    table.io = s3_io

    # One scan object for both the plan and the read below — planning twice
    # would re-fetch the manifests for nothing.
    scan = table.scan()
    tasks = list(scan.plan_files())
    if any(task.delete_files for task in tasks):
        return "SKIPPED: has delete files (merge-on-read writes) — compaction not supported yet"

    sizes = [task.file.file_size_in_bytes for task in tasks]
    total = sum(sizes)
    small = sum(1 for s in sizes if s < cfg.small_file_max_bytes)
    small_bytes = sum(s for s in sizes if s < cfg.small_file_max_bytes)

    if small < cfg.min_input_files:
        return f"healthy ({len(sizes)} files, {small} small, {total / MIB:.1f} MiB)"

    # Write-amplification gate (NOT a memory gate — the rewrite below is O(1) in
    # table size). Whole-table overwrite rewrites *every* file, so skip when the
    # small files are only a sliver of a big, already-compacted table: rewriting
    # gigabytes to fold in a few small files isn't worth the object-storage
    # churn. When a per-file swap primitive exists we'll bin-pack just the small
    # files instead.
    fraction = small_bytes / total if total else 0.0
    if fraction < cfg.rewrite_min_small_fraction:
        return (
            f"skipped: {small} small files are only {fraction:.0%} of {total / MIB:.1f} MiB "
            f"(< {cfg.rewrite_min_small_fraction:.0%}) — not worth a whole-table rewrite; "
            "waiting on per-file bin-packing"
        )

    # Streamed, atomic rewrite — peak memory is O(1) in table size (bounded by
    # cfg.rewrite_chunk_bytes, constant), no whole-table buffer, no size cap.
    #
    # AWAITING-UPSTREAM: the one-liner this *wants* to be is
    # `table.overwrite(table.scan().to_arrow_batch_reader())` — PyIceberg
    # consuming a RecordBatchReader lazily in a single atomic overwrite. That
    # landed on pyiceberg `main` (Table/Transaction.overwrite/append widened to
    # `pa.Table | pa.RecordBatchReader`, streamed via _dataframe_to_data_files)
    # but is UNRELEASED: the latest release, 0.11.1 (our pin), still raises
    # `ValueError("Expected PyArrow table")` on a reader. When a release ships
    # with it, delete this whole block and use that one call, dropping the
    # `_dataframe_to_data_files` import — but keep feeding it `stream_batches`,
    # NOT `scan.to_arrow_batch_reader()`: the reader that method hands back
    # buffers whole files behind an executor (see stream_batches), so the
    # tidy-looking one-liner would quietly restore the OOM. Until then we
    # reproduce exactly what 0.11.1's own Transaction.overwrite does
    # internally, but feed the reader in chunks instead of one materialized
    # table.
    #
    # Phase 1 (heavy; NO transaction open): stream the scan, accumulate batches
    # to ~cfg.rewrite_chunk_bytes, write each chunk to fresh parquet data files
    # in object storage and keep only the DataFile metadata (KB each). Peak
    # memory = one chunk. A crash/OOM here leaves the table *completely
    # untouched* — nothing is committed; only orphan parquet lands in storage
    # (swept by the orphan-file follow-up).
    write_uuid = uuid.uuid4()
    counter = itertools.count(0)
    data_files = []
    pending = []
    pending_bytes = 0

    def _flush():
        nonlocal pending, pending_bytes
        if not pending:
            return
        chunk = pyarrow.Table.from_batches(pending)
        pending = []
        pending_bytes = 0
        data_files.extend(
            _dataframe_to_data_files(
                table_metadata=table.metadata,
                df=chunk,
                io=table.io,
                write_uuid=write_uuid,
                counter=counter,
            )
        )
        del chunk
        release_memory()
        log.info("%s: %d data file(s) written", ".".join(table.name()), len(data_files))

    # Progress, not decoration: the whole rewrite is one long silent stretch,
    # and when the 2026-07-31 run was OOM-killed mid-table the log did not even
    # say which table it had moved on to.
    log.info(
        "%s: rewriting %d files (%.1f MiB, %d small) in ~%d MiB chunks",
        ".".join(table.name()),
        len(sizes),
        total / MIB,
        small,
        cfg.rewrite_chunk_bytes // MIB,
    )
    for batch in stream_batches(scan, tasks):
        pending.append(batch)
        pending_bytes += batch.nbytes
        if pending_bytes >= cfg.rewrite_chunk_bytes:
            _flush()
    _flush()

    if not data_files:
        # The scan yielded no rows despite planned files — never empty the table
        # on a surprise; leave it as-is and say so.
        return f"skipped: scan produced no rows ({len(sizes)} files planned) — table left untouched"

    # Phase 2 (fast; metadata-only): one transaction that atomically drops every
    # old data file and adds the new ones. delete(AlwaysTrue()) on a delete-free
    # table is a whole-file metadata delete (no copy-on-write read). The
    # transaction commits BOTH the delete and the append together, and — since
    # Transaction.__exit__ commits only when no exception propagates — a crash
    # here also leaves the table untouched. A concurrent dlt load that advanced
    # the branch loses the commit race (CommitFailedException, caught by the
    # caller and retried next night), exactly as the stock overwrite would.
    with table.transaction() as tx:
        tx.delete(delete_filter=AlwaysTrue())
        with tx._append_snapshot_producer({}) as append_files:
            for data_file in data_files:
                append_files.append_data_file(data_file)

    return (
        f"compacted {len(sizes)} files ({small} small, {total / MIB:.1f} MiB) into "
        f"{len(data_files)} file(s) via a streamed rewrite"
    )


def expire_snapshots(table, cfg: Config) -> str:
    """Expire snapshots past the time-travel window; metadata-only.

    Never expires branch heads or tags (PyIceberg protects those too), and
    always keeps the min_snapshots_to_keep newest snapshots regardless of
    age, so a rarely-written table retains some history. Data/manifest files
    of expired snapshots are NOT deleted — see the module docstring on the
    missing orphan sweep.
    """
    snaps = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)
    if not snaps:
        return "nothing to expire (no snapshots)"

    protected = {ref.snapshot_id for ref in table.metadata.refs.values()}
    keep_floor = {s.snapshot_id for s in snaps[: cfg.min_snapshots_to_keep]}
    cutoff_ms = time.time() * 1000 - cfg.max_snapshot_age_ms
    expirable = [
        s.snapshot_id
        for s in snaps
        if s.timestamp_ms < cutoff_ms
        and s.snapshot_id not in protected
        and s.snapshot_id not in keep_floor
    ]
    if not expirable:
        return f"nothing to expire ({len(snaps)} snapshots, all within window/floor)"

    table.maintenance.expire_snapshots().by_ids(expirable).commit()
    return f"expired {len(expirable)} of {len(snaps)} snapshots"


def main() -> int:
    cfg = load_config()
    catalog = load_catalog(
        "box",
        **{
            "type": "rest",
            "uri": cfg.catalog_uri,
            "warehouse": cfg.warehouse,
            "credential": cfg.credential,
            "oauth2-server-uri": cfg.oidc_token_url,
        },
    )
    s3_io = direct_s3_io(cfg)

    tables = 0
    errors = 0
    for ns in all_namespaces(catalog):
        for ident in catalog.list_tables(ns):
            tables += 1
            name = ".".join(ident)
            try:
                table = catalog.load_table(ident)
            except Exception:
                log.exception("%s: load failed", name)
                errors += 1
                continue

            try:
                log.info("%s: compaction: %s", name, compact_table(table, s3_io, cfg))
            except CommitFailedException as exc:
                # Concurrent write won the race — fine, next night retries.
                log.warning(
                    "%s: compaction commit conflict (concurrent write?), retrying next run: %s",
                    name,
                    exc,
                )
            except Exception:
                log.exception("%s: compaction failed", name)
                errors += 1

            # Expiry runs even when compaction skipped or failed — it is
            # metadata-only and safe for merge-on-read tables too.
            try:
                log.info("%s: snapshot expiry: %s", name, expire_snapshots(table, cfg))
            except CommitFailedException as exc:
                log.warning(
                    "%s: expiry commit conflict (concurrent write?), retrying next run: %s",
                    name,
                    exc,
                )
            except Exception:
                log.exception("%s: snapshot expiry failed", name)
                errors += 1

            # Bound the run's RSS high-water to the single largest table, not
            # the sum across the loop, before moving on.
            del table
            release_memory()

    log.info("done: %d tables scanned, %d errors", tables, errors)
    return 1 if errors else 0
