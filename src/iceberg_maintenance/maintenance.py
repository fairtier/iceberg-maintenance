"""Nightly Iceberg maintenance for a box warehouse: compaction + expiry.

Walks every table in the warehouse and
  1. where a table has accumulated enough small data files, rewrites it by
     streaming the scan straight into fresh data files — no whole-table
     materialization, so peak memory is O(1) in table size and there is no
     size cap (stream_batches -> chunked write; note that PyIceberg's own
     scan().to_arrow_batch_reader() is NOT lazy — see stream_batches);
  2. expires snapshots past the time-travel window (metadata-only: branch
     heads and tags are never expired, and a retention floor of the newest
     snapshots is kept);
  3. sweeps the object storage those two leave behind — files under the
     table's location that no retained snapshot can reach any more (see
     orphans.py; reports by default, deletes only when armed).

Safe against concurrent dlt loads: Iceberg's optimistic concurrency turns a
clash into a failed commit here (logged, retried next night), never corrupted
data. A catalog that is merely *unavailable* is a different thing and is
retried on the spot rather than costing the whole rewrite behind it — see
commit_swap.

Every per-table operation reports an `Outcome` — a label a metric can count
plus the sentence a human reads — and the whole run is one trace when a
collector is configured (see telemetry.py).

Deliberately NOT handled here:
  - tables with delete files (DuckFlight/DuckDB writes are merge-on-read) —
    a COW rewrite through PyIceberg's scan would need delete-file semantics
    PyIceberg only partially applies, so those tables are skipped loudly
    (snapshot expiry still runs — it never touches data files);
  - tables whose manifests PyIceberg cannot decode faithfully (DuckDB's
    Iceberg writer) — skipped loudly for the same reason, see
    unrewritable_manifest_reason.

The full design lives in docs/plans/iceberg-maintenance.md in the FairTier
platform repo.
"""

import gc
import itertools
import logging
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial

import pyarrow
import requests
from opentelemetry.trace import Span, StatusCode
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    ServerError,
    ServiceUnavailableError,
)
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
from pyiceberg.manifest import ManifestContent, ManifestEntryStatus

from . import orphans, telemetry
from .config import MIB, Config, load_config
from .telemetry import tracer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("maintenance")


@dataclass(frozen=True)
class Outcome:
    """What one per-table operation did.

    Two audiences, one object: `kind` is the low-cardinality label a dashboard
    counts (`iceberg.outcome` on spans and metrics), `message` is the sentence
    an operator reads in the log. Split so neither has to be derived from the
    other — no dashboard parsing prose, and no prose flattened to fit a label.

    `kind` is a closed vocabulary:

      compacted    the table was rewritten and the swap committed
      healthy      nothing to do — too few small files to bother
      skipped      compaction declined this table for now (write amplification,
                   or a scan that surprised us by yielding no rows)
      unsupported  compaction can never run here as things stand: delete files,
                   or manifests PyIceberg cannot decode
      empty        no snapshot at all
      expired      snapshots were expired
      nothing      expiry had nothing outside the window and floor
      clean        the orphan sweep found nothing unreachable
      orphans      the orphan sweep found files but is not armed to delete
      swept        the orphan sweep deleted unreachable files
      disabled     the orphan sweep is switched off
      conflict     lost the commit race to a concurrent write; retried next run
      failed       raised
    """

    kind: str
    message: str

    def __str__(self) -> str:
        return self.message


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


def unrewritable_manifest_reason(table, snapshot) -> str | None:
    """Why the commit phase would reject this table's manifests, or None.

    The atomic swap rewrites every live manifest entry as DELETED/EXISTING, and
    `ManifestWriterV2.prepare_entry` refuses any such entry with a null
    sequence number ("Only entries with status ADDED can have null sequence
    number"). Some manifests can't satisfy that, because PyIceberg does not
    decode them faithfully in the first place: on tables written by DuckDB's
    Iceberg writer (the dbt/DuckFlight path — `<uuidv7>.parquet` data files, an
    Avro schema that spells types as `{"type":"int"}` / `["null",{"type":
    "long"}]` rather than `"int"` / `["null","long"]`), the entry header comes
    back shifted: `status` holds the snapshot id, `snapshot_id` and
    `sequence_number` are None. The `data_file` half still decodes correctly,
    which is why reads and the scan are fine and only the rewrite trips.

    Checking up front is what makes the failure cheap. Discovered at commit
    time — as it was on 2026-07-31 — the job has already streamed the entire
    table into fresh parquet, so a table it can never compact costs a full
    rewrite of orphan files in object storage every single night.

    Skipping is the conservative answer, not a workaround: entries we cannot
    read faithfully are entries we must not rewrite. Snapshot expiry still runs
    (it commits through the catalog and never touches manifests). The interop
    bug itself belongs upstream.

    Asked of every table with a snapshot, not only of the ones tonight's size
    gates elected — see the call site in `_compact_table` for why that
    distinction cost a production signal.
    """
    for manifest in snapshot.manifests(io=table.io):
        if manifest.content != ManifestContent.DATA:
            continue
        for entry in manifest.fetch_manifest_entry(io=table.io, discard_deleted=True):
            if not isinstance(entry.status, ManifestEntryStatus):
                return (
                    f"manifest entry status is {entry.status!r}, not a status — "
                    f"PyIceberg cannot decode {manifest.manifest_path.rsplit('/', 1)[-1]}"
                )
            if entry.sequence_number is None:
                return (
                    "manifest entry has no sequence number "
                    f"({manifest.manifest_path.rsplit('/', 1)[-1]})"
                )
    return None


# Commit failures that mean "the catalog was not there", as opposed to "someone
# else got there first" (CommitFailedException — yield, retry tomorrow) or "this
# request is wrong" (4xx — retrying changes nothing).
_UNAVAILABLE_CATALOG = (
    # Nothing answered. Lakekeeper crashlooping, CoreDNS restarted, the box
    # mid-recovery — the 2026-08-27 failure verbatim ("Connection refused"),
    # which threw away a completed 1.4 GiB rewrite.
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    # 503: up, not serving yet.
    ServiceUnavailableError,
    # Other 5xx.
    ServerError,
    # 500/502/504 on the commit endpoint specifically. AMBIGUOUS BY
    # CONSTRUCTION: PyIceberg raises this exact type to say the commit may or
    # may not have been applied, which is why the retry re-reads the table
    # before believing anything.
    CommitStateUnknownException,
)


def _swap_already_landed(table, data_files) -> bool:
    """Is the table's live file set exactly the one this rewrite wrote?

    The question a lost response leaves behind. If the answer is yes, the
    commit we never heard back from was applied and there is nothing to redo;
    retrying it would only mint a second, identical snapshot.

    Path equality, not a snapshot-id or row count: the data files are the swap,
    and comparing them is the only check that cannot be fooled by a concurrent
    writer that happens to have produced the same number of rows.
    """
    written = {data_file.file_path for data_file in data_files}
    live = {task.file.file_path for task in table.scan().plan_files()}
    return live == written


def commit_swap(table, data_files, base_snapshot_id: int, cfg: Config) -> int:
    """Commit the rewrite; retry a catalog that is merely unavailable.

    Returns the attempt number that succeeded, so the caller can say so.

    The asymmetry this exists for: the rewrite above is the expensive half
    (minutes of streaming, gigabytes through object storage) and the commit is
    one HTTP call. On 2026-08-27 a box lost a completed 1.4 GiB rewrite of
    `nyc_taxi.yellow_trips` — all 107 chunks written — to a single
    `Connection refused` while Lakekeeper was crashlooping, and turned the
    whole thing into 1.4 GiB of orphans that no OSS tool can sweep. Redoing
    that tomorrow costs strictly more than waiting a couple of minutes today.

    Two things it refuses to do, both of which would be worse than the loss it
    prevents:

    - **It does not retry a lost race.** `CommitFailedException` means a
      concurrent dlt load advanced the branch; the rewrite is stale by
      definition and yielding is correct (the caller reports `conflict` and
      the next night retries).
    - **It does not re-aim the swap at a table that moved.** The swap is
      `delete(ALWAYS_TRUE) + append(our files)`, which is only safe against the
      snapshot it was planned on. Refreshing onto a snapshot a *concurrent*
      writer produced would make that delete succeed — and silently drop their
      rows. So a refresh that finds an unfamiliar snapshot ends the attempt as
      a conflict, exactly as the un-retried commit would have.

    (If our own swap landed and a concurrent write followed it, the file sets
    no longer match and this reports a conflict for work that in fact
    succeeded. Harmless — the table is correct, and the next run sees it.)
    """
    last: Exception | None = None
    for attempt in range(1, max(cfg.commit_max_attempts, 1) + 1):
        try:
            with table.transaction() as tx:
                tx.delete(delete_filter=AlwaysTrue())
                with tx._append_snapshot_producer({}) as append_files:
                    for data_file in data_files:
                        append_files.append_data_file(data_file)
            return attempt
        except CommitFailedException:
            raise
        except _UNAVAILABLE_CATALOG as exc:
            last = exc
            if attempt >= max(cfg.commit_max_attempts, 1):
                break
            delay = cfg.commit_retry_backoff_seconds * 2 ** (attempt - 1)
            log.warning(
                "%s: commit attempt %d/%d failed (%s: %s) — retrying in %.0fs",
                ".".join(table.name()),
                attempt,
                cfg.commit_max_attempts,
                type(exc).__name__,
                exc,
                delay,
            )
            time.sleep(delay)
            try:
                table.refresh()
            except _UNAVAILABLE_CATALOG as refresh_exc:
                # Still down. Nothing to learn from the table yet — go straight
                # to the next attempt, which will fail the same way and sleep
                # longer.
                log.info(
                    "%s: refresh after attempt %d also failed (%s) — retrying anyway",
                    ".".join(table.name()),
                    attempt,
                    type(refresh_exc).__name__,
                )
                continue
            current = table.current_snapshot()
            if current is not None and current.snapshot_id != base_snapshot_id:
                if _swap_already_landed(table, data_files):
                    log.info(
                        "%s: commit had landed after all (attempt %d) — "
                        "the response was lost, not the write",
                        ".".join(table.name()),
                        attempt,
                    )
                    return attempt
                raise CommitFailedException(
                    "table advanced to snapshot "
                    f"{current.snapshot_id} while the commit was being retried "
                    "— the rewrite is stale, yielding to the concurrent writer"
                ) from exc
    if last is None:  # unreachable: the loop runs at least once
        raise RuntimeError("commit_swap made no attempt")
    raise last


def compact_table(table, s3_io, cfg: Config) -> Outcome:
    """Compact one table if it needs it; returns a structured outcome.

    The span wrapper lives out here so that *every* return path below — each
    gate, each skip — lands its `iceberg.outcome` on the span without the body
    having to remember to.
    """
    with tracer.start_as_current_span(
        "iceberg.compact", attributes={"iceberg.table": ".".join(table.name())}
    ) as span:
        outcome = _compact_table(span, table, s3_io, cfg)
        span.set_attribute("iceberg.outcome", outcome.kind)
        return outcome


def _compact_table(span: Span, table, s3_io, cfg: Config) -> Outcome:
    snapshot = table.current_snapshot()
    if snapshot is None:
        return Outcome("empty", "empty (no snapshot)")

    # Merge-on-read delete files (DuckFlight UPDATE/DELETE/MERGE) make a
    # naive COW rewrite unsafe — skip and say so.
    summary = snapshot.summary or {}
    if int(summary.get("total-delete-files", 0) or 0) > 0:
        return Outcome(
            "unsupported",
            "SKIPPED: has delete files (merge-on-read writes) — compaction not supported yet",
        )

    table.io = s3_io

    # One scan object for both the plan and the read below — planning twice
    # would re-fetch the manifests for nothing.
    with tracer.start_as_current_span("iceberg.compact.plan"):
        scan = table.scan()
        tasks = list(scan.plan_files())
    if any(task.delete_files for task in tasks):
        return Outcome(
            "unsupported",
            "SKIPPED: has delete files (merge-on-read writes) — compaction not supported yet",
        )

    sizes = [task.file.file_size_in_bytes for task in tasks]
    total = sum(sizes)
    small = sum(1 for s in sizes if s < cfg.small_file_max_bytes)
    small_bytes = sum(s for s in sizes if s < cfg.small_file_max_bytes)

    # The shape of the table as the gates below see it — recorded whether or
    # not a rewrite follows, so a table that never gets compacted still shows
    # *why* in the trace.
    span.set_attributes(
        {
            "iceberg.compaction.input.files": len(sizes),
            "iceberg.compaction.input.bytes": total,
            "iceberg.compaction.small.files": small,
            "iceberg.compaction.small.bytes": small_bytes,
        }
    )

    # Can the swap even commit? Asked of every table, and asked *before* the
    # size gates below rather than after them.
    #
    # Whether PyIceberg can decode a table's manifests is a property of the
    # table — of which engine wrote it — not of how many small files it happens
    # to be carrying tonight. Behind the gates the question was only ever put
    # to tables that were already compaction candidates, so `unsupported`
    # quietly meant "un-compactable AND currently a candidate", and tuning a
    # gate moved a signal that has nothing to do with tuning. It did: lowering
    # SMALL_FILE_MAX_BYTES 32 MiB -> 8 MiB on 2026-08-27 took fokume's
    # iceberg_maintenance_tables_unsupported from 2 to 0 without repairing
    # anything, because staging.stg_trips and marts.fct_trips (~8.5 MiB/file)
    # stopped clearing min_input_files and returned "healthy" one gate earlier.
    # BoxIcebergTablesUnsupported would have resolved on a box where the
    # interop bug was untouched — the same "a skipped table counts as a clean
    # run" silence that RunSummary.unsupported exists to end.
    #
    # It costs a manifest-entry read on every table, but not a new one:
    # plan_files above already fetched every data manifest's entries to build
    # `tasks`; this re-reads the same avro for the entry-header halves the plan
    # discards. The file counts ride along in the message because they are what
    # says whether this table is merely un-compactable or actively rotting.
    with tracer.start_as_current_span("iceberg.compact.check_manifests"):
        reason = unrewritable_manifest_reason(table, snapshot)
    if reason:
        return Outcome(
            "unsupported",
            f"SKIPPED: {reason} — written by another engine, so a rewrite would "
            f"fail at commit ({len(sizes)} files, {small} small, "
            f"{total / MIB:.1f} MiB); snapshot expiry still runs",
        )

    if small < cfg.min_input_files:
        return Outcome(
            "healthy",
            f"healthy ({len(sizes)} files, {small} small, {total / MIB:.1f} MiB)",
        )

    # Write-amplification gate (NOT a memory gate — the rewrite below is O(1) in
    # table size). Whole-table overwrite rewrites *every* file, so skip when the
    # small files are only a sliver of a big, already-compacted table: rewriting
    # gigabytes to fold in a few small files isn't worth the object-storage
    # churn. When a per-file swap primitive exists we'll bin-pack just the small
    # files instead.
    fraction = small_bytes / total if total else 0.0
    span.set_attribute("iceberg.compaction.small.fraction", fraction)
    if fraction < cfg.rewrite_min_small_fraction:
        return Outcome(
            "skipped",
            f"skipped: {small} small files are only {fraction:.0%} of {total / MIB:.1f} MiB "
            f"(< {cfg.rewrite_min_small_fraction:.0%}) — not worth a whole-table rewrite; "
            "waiting on per-file bin-packing",
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

    # Progress, not decoration: the whole rewrite is one long silent stretch,
    # and when the 2026-07-31 run was OOM-killed mid-table the log did not even
    # say which table it had moved on to. The span is the same story told to a
    # collector — it is where the run's wall time actually goes — and the chunk
    # events inside it are what turns "killed mid-rewrite" into "killed after
    # chunk 7 of a 12 GiB table".
    log.info(
        "%s: rewriting %d files (%.1f MiB, %d small) in ~%d MiB chunks",
        ".".join(table.name()),
        len(sizes),
        total / MIB,
        small,
        cfg.rewrite_chunk_bytes // MIB,
    )
    with tracer.start_as_current_span(
        "iceberg.compact.rewrite",
        attributes={"iceberg.compaction.chunk.bytes": cfg.rewrite_chunk_bytes},
    ) as rewrite_span:

        def _flush():
            nonlocal pending, pending_bytes
            if not pending:
                return
            chunk = pyarrow.Table.from_batches(pending)
            chunk_rows, chunk_bytes = chunk.num_rows, pending_bytes
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
            log.info(
                "%s: %d data file(s) written", ".".join(table.name()), len(data_files)
            )
            # An event, not a span: a chunk has no interesting internal
            # structure, and a 12 GiB table would otherwise mint a hundred
            # near-identical spans.
            rewrite_span.add_event(
                "chunk written",
                {
                    "iceberg.compaction.chunk.rows": chunk_rows,
                    "iceberg.compaction.chunk.bytes": chunk_bytes,
                    "iceberg.compaction.output.files": len(data_files),
                },
            )

        for batch in stream_batches(scan, tasks):
            pending.append(batch)
            pending_bytes += batch.nbytes
            if pending_bytes >= cfg.rewrite_chunk_bytes:
                _flush()
        _flush()

        written_bytes = sum(data_file.file_size_in_bytes for data_file in data_files)
        rewrite_span.set_attributes(
            {
                "iceberg.compaction.output.files": len(data_files),
                "iceberg.compaction.output.bytes": written_bytes,
            }
        )

    if not data_files:
        # The scan yielded no rows despite planned files — never empty the table
        # on a surprise; leave it as-is and say so.
        return Outcome(
            "skipped",
            f"skipped: scan produced no rows ({len(sizes)} files planned) — table left untouched",
        )

    # Phase 2 (fast; metadata-only): one transaction that atomically drops every
    # old data file and adds the new ones. delete(AlwaysTrue()) on a delete-free
    # table is a whole-file metadata delete (no copy-on-write read). The
    # transaction commits BOTH the delete and the append together, and — since
    # Transaction.__exit__ commits only when no exception propagates — a crash
    # here also leaves the table untouched. A concurrent dlt load that advanced
    # the branch loses the commit race (CommitFailedException, caught by the
    # caller and retried next night), exactly as the stock overwrite would.
    #
    # An *unavailable* catalog is not that, and is retried rather than thrown
    # away with the whole rewrite behind it — see commit_swap.
    with tracer.start_as_current_span("iceberg.compact.commit") as commit_span:
        attempts = commit_swap(table, data_files, snapshot.snapshot_id, cfg)
        commit_span.set_attribute("iceberg.compaction.commit.attempts", attempts)
    span.set_attribute("iceberg.compaction.commit.attempts", attempts)

    # Counted only now: everything above this line is work that a lost commit
    # race turns into orphan parquet, and a "files rewritten" chart that
    # includes it would be measuring effort, not effect.
    span.set_attribute("iceberg.compaction.output.files", len(data_files))
    telemetry.files_rewritten.add(len(sizes))
    telemetry.files_written.add(len(data_files))
    telemetry.bytes_rewritten.add(total)

    return Outcome(
        "compacted",
        f"compacted {len(sizes)} files ({small} small, {total / MIB:.1f} MiB) into "
        f"{len(data_files)} file(s) via a streamed rewrite",
    )


def expire_snapshots(table, cfg: Config) -> Outcome:
    """Expire snapshots past the time-travel window; metadata-only.

    Never expires branch heads or tags (PyIceberg protects those too), and
    always keeps the min_snapshots_to_keep newest snapshots regardless of
    age, so a rarely-written table retains some history.

    Data/manifest files of expired snapshots are NOT deleted here — expiry is
    metadata-only, and freeing the storage is the orphan sweep's job (see
    orphans.py), which runs after this so that it sees the snapshots this pass
    just dropped.
    """
    with tracer.start_as_current_span(
        "iceberg.expire_snapshots", attributes={"iceberg.table": ".".join(table.name())}
    ) as span:
        outcome = _expire_snapshots(span, table, cfg)
        span.set_attribute("iceberg.outcome", outcome.kind)
        return outcome


def _expire_snapshots(span: Span, table, cfg: Config) -> Outcome:
    snaps = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)
    span.set_attribute("iceberg.snapshots.total", len(snaps))
    if not snaps:
        return Outcome("nothing", "nothing to expire (no snapshots)")

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
    span.set_attribute("iceberg.snapshots.expirable", len(expirable))
    if not expirable:
        return Outcome(
            "nothing",
            f"nothing to expire ({len(snaps)} snapshots, all within window/floor)",
        )

    table.maintenance.expire_snapshots().by_ids(expirable).commit()
    telemetry.snapshots_expired.add(len(expirable))
    return Outcome("expired", f"expired {len(expirable)} of {len(snaps)} snapshots")


def run_operation(
    name: str, operation: str, label: str, work: Callable[[], Outcome]
) -> Outcome:
    """Run one per-table operation: log it, time it, count it, never raise.

    The two operations fail the same two ways — a lost commit race (transient,
    retried next run, not the run's fault) and everything else (an error) — so
    the handling lives here once, and the caller is left deciding only what an
    outcome means for the exit code.
    """
    started = time.monotonic()
    try:
        outcome = work()
        log.info("%s: %s: %s", name, label, outcome)
    except CommitFailedException as exc:
        # Concurrent write won the race — fine, next night retries.
        log.warning(
            "%s: %s commit conflict (concurrent write?), retrying next run: %s",
            name,
            label,
            exc,
        )
        outcome = Outcome("conflict", str(exc))
    except Exception as exc:
        log.exception("%s: %s failed", name, label)
        outcome = Outcome("failed", str(exc))
    telemetry.record_operation(operation, outcome.kind, time.monotonic() - started)
    return outcome


def run_sweep(name, table, s3_io, cfg: Config, summary: "RunSummary") -> Outcome:
    """Sweep one table's orphans and fold its numbers into the run summary.

    A function rather than three lines in the loop for two reasons. `sweep_table`
    returns more than an `Outcome` (the found/deleted counts are the whole
    output in dry-run mode) while `run_operation` — which owns the logging, the
    timing, the metric and the never-raise contract — takes something that
    returns exactly one; the closure that bridges the two belongs somewhere it
    cannot outlive the call, because `maintain_warehouse` deletes `table` at the
    end of every iteration to keep the run's RSS high-water at one table.

    The sweep runs on **every** table, including the ones compaction refuses:
    whether PyIceberg can *rewrite* a table's manifests and whether it can *read
    the file paths out of them* are different questions, and only the second one
    matters here — it is the same read the scan does on those tables every day.
    """
    result = orphans.SweepResult(Outcome("disabled", ""))

    def work() -> Outcome:
        nonlocal result
        result = orphans.sweep_table(table, s3_io, cfg)
        return result.outcome

    outcome = run_operation(name, "sweep_orphans", "orphan sweep", work)
    summary.orphan_files += result.files
    summary.orphan_bytes += result.bytes
    summary.orphan_deleted += result.deleted
    return outcome


@dataclass
class RunSummary:
    """What the whole run did, in the three numbers an operator needs.

    `unsupported` is the one this class exists for. It is deliberately NOT
    folded into `errors`: an un-compactable table did not fail, and making the
    run exit non-zero would send the Job into its backoff for a condition that
    is identical on the retry. But it is not a success either — the table is
    permanently not being compacted, and until 2026-08-13 that fact appeared
    nowhere: the run logged one SKIPPED line among a hundred, reported
    "12 tables scanned, 0 errors", exited 0 and stamped the success heartbeat.
    A production box carried an un-compactable dbt staging table for weeks that
    way.

    So it gets a number of its own, and `unsupported_tables` carries the names,
    because "1 un-compactable" is only actionable if you know which one.

    The number counts a property of the tables, not of tonight's run: a table
    is un-compactable whether or not the size gates would have elected it for a
    rewrite. It did not always — until 2026-08-28 the manifest check sat behind
    those gates, so lowering a threshold could take the count to zero with
    nothing repaired.
    """

    tables: int = 0
    errors: int = 0
    unsupported_tables: list[str] = field(default_factory=list)
    # What the orphan sweep FOUND, across every table — reported whether or not
    # it was armed to delete any of it. That is the point: a box running in
    # dry-run publishes the storage it is paying for and nobody is using, which
    # is a number that did not exist anywhere before.
    orphan_files: int = 0
    orphan_bytes: int = 0
    orphan_deleted: int = 0

    @property
    def unsupported(self) -> int:
        return len(self.unsupported_tables)


def maintain_warehouse(catalog, s3_io, cfg: Config) -> RunSummary:
    """Compact + expire every table in the warehouse."""
    summary = RunSummary()
    for ns in all_namespaces(catalog):
        for ident in catalog.list_tables(ns):
            summary.tables += 1
            name = ".".join(ident)
            telemetry.tables_scanned.add(1)
            with tracer.start_as_current_span(
                "iceberg.maintenance.table",
                attributes={
                    "iceberg.table": name,
                    "iceberg.namespace": ".".join(ident[:-1]),
                },
            ) as table_span:
                started = time.monotonic()
                try:
                    with tracer.start_as_current_span("iceberg.catalog.load_table"):
                        table = catalog.load_table(ident)
                except Exception:
                    log.exception("%s: load failed", name)
                    summary.errors += 1
                    table_span.set_status(StatusCode.ERROR, "load failed")
                    # Only the failures are counted here — a table that loads
                    # goes on to be counted by the two operations below, and
                    # the run's denominator is tables.scanned.
                    telemetry.record_operation(
                        "load_table", "failed", time.monotonic() - started
                    )
                    continue

                # partial, not a lambda: `table` is deleted at the end of this
                # loop body, so a late-bound closure over it would be reading a
                # name that is gone by then.
                compaction = run_operation(
                    name,
                    "compact",
                    "compaction",
                    partial(compact_table, table, s3_io, cfg),
                )
                # Expiry runs even when compaction skipped or failed — it is
                # metadata-only and safe for merge-on-read tables too.
                expiry = run_operation(
                    name,
                    "expire_snapshots",
                    "snapshot expiry",
                    partial(expire_snapshots, table, cfg),
                )
                # And the sweep runs last, so it sees the snapshots expiry
                # just dropped.
                sweep = run_sweep(name, table, s3_io, cfg, summary)

                # A conflict is not an error: the next run retries it. Anything
                # that raised is, and it fails the whole job's exit code.
                failed = [o for o in (compaction, expiry, sweep) if o.kind == "failed"]
                summary.errors += len(failed)
                if failed:
                    table_span.set_status(StatusCode.ERROR)

                # Un-compactable is a standing condition, not a run failure —
                # see RunSummary. Only compaction can produce it; expiry runs
                # on these tables regardless, which is why they look fine.
                if compaction.kind == "unsupported":
                    summary.unsupported_tables.append(name)

                # Bound the run's RSS high-water to the single largest table,
                # not the sum across the loop, before moving on.
                del table
                release_memory()

    return summary


def write_textfile(path: str, summary: RunSummary) -> None:
    """Publish the run's numbers as a node_exporter textfile.

    Called only after a clean run (`errors == 0`) — the timestamp means "the
    last time this box finished a maintenance pass", which is what
    `BoxIcebergMaintenanceStale` reads, so a failed run must leave the previous
    value standing rather than refresh it.

    Written to a temp file in the same directory and renamed, because the node
    exporter may scrape mid-write and a half-written file is a parse error for
    the whole scrape, not just this metric.

    This used to live in a shell wrapper in the chart, to avoid rebuilding the
    image for a heartbeat. It moved here when the un-compactable count needed
    publishing too: the wrapper could only see the exit code, and the whole
    point of that count is that it is invisible in the exit code. The orphan
    figures are the same shape of fact — a warehouse can be maintained
    perfectly and still be paying for gigabytes nothing references.
    """
    directory = os.path.dirname(path) or "."
    tmp = f"{path}.tmp"
    body = (
        "# HELP iceberg_maintenance_last_success_timestamp_seconds Unix time of "
        "the last maintenance run that completed with no errors.\n"
        "# TYPE iceberg_maintenance_last_success_timestamp_seconds gauge\n"
        f"iceberg_maintenance_last_success_timestamp_seconds {int(time.time())}\n"
        "# HELP iceberg_maintenance_tables_scanned Tables visited by the last "
        "clean run.\n"
        "# TYPE iceberg_maintenance_tables_scanned gauge\n"
        f"iceberg_maintenance_tables_scanned {summary.tables}\n"
        "# HELP iceberg_maintenance_tables_unsupported Tables the last clean run "
        "could not compact at all (delete files, or manifests PyIceberg cannot "
        "decode). Snapshot expiry still runs on them.\n"
        "# TYPE iceberg_maintenance_tables_unsupported gauge\n"
        f"iceberg_maintenance_tables_unsupported {summary.unsupported}\n"
        "# HELP iceberg_maintenance_orphan_files Files under the warehouse's "
        "table locations that no retained snapshot can reach. Reported whether "
        "or not the sweep is armed to delete them.\n"
        "# TYPE iceberg_maintenance_orphan_files gauge\n"
        f"iceberg_maintenance_orphan_files {summary.orphan_files}\n"
        "# HELP iceberg_maintenance_orphan_bytes Object storage held by those "
        "unreachable files — what the warehouse pays for and nothing reads.\n"
        "# TYPE iceberg_maintenance_orphan_bytes gauge\n"
        f"iceberg_maintenance_orphan_bytes {summary.orphan_bytes}\n"
    )
    os.makedirs(directory, exist_ok=True)
    with open(tmp, "w") as fh:
        fh.write(body)
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)
    log.info("wrote %s", path)


def main() -> int:
    cfg = load_config()
    # Set up before anything else worth tracing, torn down in the finally: a
    # CronJob exits long before batched spans and periodic metrics would flush
    # on their own, so an unflushed run reports nothing at all.
    shutdown_telemetry = telemetry.setup(cfg)
    started = time.monotonic()
    run_outcome = "failed"
    try:
        with tracer.start_as_current_span("iceberg.maintenance.run") as run_span:
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

            summary = maintain_warehouse(catalog, s3_io, cfg)
            run_outcome = "errors" if summary.errors else "ok"
            run_span.set_attributes(
                {
                    "iceberg.tables.scanned": summary.tables,
                    "iceberg.tables.failed": summary.errors,
                    "iceberg.tables.unsupported": summary.unsupported,
                    "iceberg.orphans.files": summary.orphan_files,
                    "iceberg.orphans.bytes": summary.orphan_bytes,
                    "iceberg.orphans.deleted": summary.orphan_deleted,
                    "iceberg.outcome": run_outcome,
                }
            )
            log.info(
                "done: %d tables scanned, %d errors, %d un-compactable%s, "
                "%d orphan file(s) / %.1f MiB unreachable (%d deleted, mode=%s)",
                summary.tables,
                summary.errors,
                summary.unsupported,
                f" ({', '.join(summary.unsupported_tables)})"
                if summary.unsupported_tables
                else "",
                summary.orphan_files,
                summary.orphan_bytes / MIB,
                summary.orphan_deleted,
                cfg.orphan_sweep_mode,
            )
            if summary.errors:
                return 1
            if cfg.textfile_path:
                # Best-effort: a warehouse that was maintained correctly must
                # not be reported as a failed run because a hostPath was not
                # mounted. The staleness alert covers a heartbeat that stops.
                try:
                    write_textfile(cfg.textfile_path, summary)
                except Exception:
                    log.exception("could not write %s", cfg.textfile_path)
            return 0
    finally:
        telemetry.run_duration.record(
            time.monotonic() - started, {"iceberg.outcome": run_outcome}
        )
        shutdown_telemetry()
