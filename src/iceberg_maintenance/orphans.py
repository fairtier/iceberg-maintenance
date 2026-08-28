"""Orphan-file sweep — the storage-reclamation half snapshot expiry cannot do.

Snapshot expiry is metadata-only: it drops snapshots from the table's metadata
and leaves every data file, manifest and manifest list those snapshots
referenced sitting in object storage forever. Compaction makes that worse, not
better — every rewrite replaces a generation of data files with a new one, and
the old generation is unreferenced the moment the swap commits. A rewrite that
is *lost* (an OOM mid-stream, a commit that never landed) orphans its output
without even buying a compaction for it: on 2026-08-27 a box streamed 107 fresh
parquet files — ~1.4 GiB — for `nyc_taxi.yellow_trips` and then lost the commit
to a crashlooping catalog. Nothing has ever removed any of it.

There is no OSS tool for this. Lakekeeper's `remove_orphan_files` task queue is
Enterprise-only, PyIceberg's implementation (PR #1958) died unmerged, and every
other option is JVM. So: list what is under the table's location, subtract
every file any *retained* snapshot can still reach, and delete what is left —
if it is old enough, and only when explicitly armed.

Deleting customer data on a mistake is the failure mode, so the whole module is
written as a set of refusals:

  * **A superset is subtracted.** Every snapshot in the metadata, not just the
    current one (time travel is a product promise); every manifest of every
    snapshot; every entry of every manifest including `DELETED` ones (an entry
    marked deleted in one manifest is still live for an older snapshot); every
    metadata JSON in `metadata_log`; every statistics file. When in doubt a
    file is kept.
  * **Any surprise aborts the table.** A manifest that will not parse, a
    listing that errors, an empty reference set, a location shallow enough to
    be a bucket root — all of them end the sweep for that table with nothing
    deleted. There is no partial-knowledge path that deletes.
  * **Age, not just reachability.** A file younger than
    `orphan_min_age_seconds` is never touched, whatever the manifests say,
    because a concurrent writer's in-flight upload is by definition not
    referenced yet.
  * **A blast-radius cap.** At most `orphan_max_deletes` files per table per
    run, logged when it bites — a bug that mistakes the live set for orphans
    costs a bounded number of files and gets a night to be noticed in.
  * **Off unless armed.** `dry-run` measures and reports; only `delete` acts.

Design notes and the operational sequence live in
docs/plans/iceberg-maintenance.md in the FairTier platform repo.
"""

import logging
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyarrow.fs import FileSelector, FileType
from pyiceberg.manifest import ManifestFile

from . import telemetry
from .config import MIB, Config
from .telemetry import tracer

if TYPE_CHECKING:
    # `maintenance` imports this module, so the Outcome vocabulary can only be
    # borrowed for typing here; at runtime it is imported inside the two
    # functions that build one.
    from .maintenance import Outcome

log = logging.getLogger("maintenance.orphans")

MODE_OFF = "off"
MODE_DRY_RUN = "dry-run"
MODE_DELETE = "delete"
MODES = (MODE_OFF, MODE_DRY_RUN, MODE_DELETE)

# The shallowest location we will sweep, counted in path components including
# the bucket: `bucket/warehouse-prefix/table-uuid`. Lakekeeper gives every
# table its own uuid-suffixed prefix, so a real table location is deeper than
# this; anything shallower is a misconfiguration pointed at shared storage, and
# a recursive listing of it would enumerate other tables' live files.
_MIN_LOCATION_DEPTH = 3

_SLASHES = re.compile(r"/+")


class SweepRefused(Exception):
    """A guard said no. Carries the sentence an operator reads; deletes nothing."""


@dataclass(frozen=True)
class Orphan:
    """One unreachable file: the path its filesystem takes, and its size."""

    path: str
    size: int


@dataclass(frozen=True)
class SweepResult:
    """What the sweep found and what (if anything) it removed.

    `files`/`bytes` are what was *found* — they are the standing reclaimable
    figure a dry run exists to publish, and they stay meaningful once the sweep
    is armed (found-but-capped is a real state). `deleted` is what actually
    went.
    """

    outcome: "Outcome"
    files: int = 0
    bytes: int = 0
    deleted: int = 0


def normalize(io, location: str) -> str:
    """A path two different spellings of the same object both reduce to.

    `parse_location` already folds `s3://bucket/key` and a listing's
    `bucket/key` together (it returns `netloc + path` for remote schemes, an
    absolute path for local ones). This drops the scheme entirely on top of
    that, so `s3://`, `s3a://` and `s3n://` spellings of one file compare
    equal — a scheme mismatch here would make a *live* file look orphaned,
    which is the one error that costs data.
    """
    _, _, path = io.parse_location(location, io.properties)
    return _SLASHES.sub("/", path).strip("/")


def normalize_listed(path: str) -> str:
    """Fold a *listing's* own path the way `normalize` folds a location.

    A listing already hands back the filesystem-native spelling (`bucket/key`),
    so there is no scheme to strip — only the slash-squeezing and trimming, and
    it has to be exactly the same squeezing or a live file compares unequal to
    its own reference and looks like garbage.
    """
    return _SLASHES.sub("/", path).strip("/")


def referenced_files(table, io) -> set[str]:
    """Every file any retained snapshot of this table can still reach.

    Deliberately more than the current snapshot's file set: the time-travel
    window is a product promise, so a file an expirable-but-not-yet-expired
    snapshot needs is live. `discard_deleted=False` for the same reason — an
    entry marked `DELETED` in one manifest is the record of a file an older
    snapshot still reads.

    Manifests are deduped by path before their entries are read: consecutive
    commits re-list the same manifests, so a table with a thousand retained
    snapshots would otherwise re-parse the same avro a thousand times.

    Raises rather than returning a partial set — every caller treats an
    exception here as "delete nothing".
    """
    metadata = table.metadata
    refs: set[str] = set()

    # The current metadata JSON and every previous one the table still lists.
    if table.metadata_location:
        refs.add(normalize(io, table.metadata_location))
    for entry in metadata.metadata_log:
        refs.add(normalize(io, entry.metadata_file))

    # Puffin statistics, table- and partition-level.
    for stat in (*metadata.statistics, *metadata.partition_statistics):
        refs.add(normalize(io, stat.statistics_path))

    # Manifest lists, manifests, and the data/delete files the manifests name.
    manifests: dict[str, ManifestFile] = {}
    for snapshot in metadata.snapshots:
        if snapshot.manifest_list:
            refs.add(normalize(io, snapshot.manifest_list))
        for manifest in snapshot.manifests(io=io):
            manifests[manifest.manifest_path] = manifest
    for path, manifest in manifests.items():
        refs.add(normalize(io, path))
        for entry in manifest.fetch_manifest_entry(io=io, discard_deleted=False):
            refs.add(normalize(io, entry.data_file.file_path))

    return refs


def _guard_location(io, location: str) -> None:
    """Refuse a location shallow enough that a recursive listing is a hazard."""
    normalized = normalize(io, location)
    if not normalized:
        raise SweepRefused("table location is empty")
    depth = len(normalized.split("/"))
    if depth < _MIN_LOCATION_DEPTH:
        raise SweepRefused(
            f"table location {location!r} is only {depth} path component(s) "
            "deep — refusing to sweep something that shallow (a bucket root "
            "would list other tables' live files)"
        )


def find_orphans(table, io, cfg: Config, now: float | None = None):
    """`(filesystem, orphans, listed)` — files under the location nothing reaches.

    `now` is injectable so a test can age files without sleeping.

    Order matters: the reference set is built *first*, so a file written
    between the two steps is listed but not referenced — and is then saved by
    the age floor below, which is one of the two reasons that floor exists.

    The filesystem comes back with the orphans because deletion goes through
    it directly: the listing's paths (`bucket/key`) are what pyarrow's
    filesystem takes, and handing one back to `FileIO.delete` would have it
    re-parsed as a *scheme-less local path*.
    """
    now = time.time() if now is None else now
    location = table.location()
    _guard_location(io, location)

    refs = referenced_files(table, io)
    if not refs:
        # A table with a location but no reachable file at all means we failed
        # to read the metadata, not that everything under it is garbage.
        raise SweepRefused("no referenced files found — refusing to sweep blind")

    scheme, netloc, path = io.parse_location(location, io.properties)
    fs = io.fs_by_scheme(scheme, netloc)

    cutoff = now - cfg.orphan_min_age_seconds
    orphans: list[Orphan] = []
    listed = 0
    for info in fs.get_file_info(FileSelector(path, recursive=True)):
        # Directories are dropped: object stores have none, and on a local
        # filesystem (the tests, and a self-hoster on a mount) deleting one
        # would take live files with it.
        if info.type != FileType.File:
            continue
        listed += 1
        if normalize_listed(info.path) in refs:
            continue
        # No mtime is not evidence of age. Skip it and let a later run, which
        # may know better, decide.
        if info.mtime_ns is None or info.mtime_ns / 1e9 > cutoff:
            continue
        orphans.append(Orphan(info.path, info.size or 0))
    return fs, orphans, listed


def sweep_table(table, io, cfg: Config, now: float | None = None) -> SweepResult:
    """Report — and, when armed, delete — this table's orphan files.

    `Outcome` is imported inside the function, not at module scope: `orphans`
    is imported *by* `maintenance`, and the vocabulary those outcomes belong to
    is documented there.
    """
    from .maintenance import Outcome

    if cfg.orphan_sweep_mode == MODE_OFF:
        return SweepResult(Outcome("disabled", "orphan sweep is off"))

    with tracer.start_as_current_span(
        "iceberg.sweep_orphans",
        attributes={
            "iceberg.table": ".".join(table.name()),
            "iceberg.orphans.mode": cfg.orphan_sweep_mode,
        },
    ) as span:
        try:
            fs, orphans, listed = find_orphans(table, io, cfg, now=now)
        except SweepRefused as exc:
            span.set_attribute("iceberg.outcome", "skipped")
            return SweepResult(Outcome("skipped", f"orphan sweep skipped: {exc}"))

        total_bytes = sum(orphan.size for orphan in orphans)
        telemetry.orphan_files_found.add(len(orphans))
        telemetry.orphan_bytes_found.add(total_bytes)
        span.set_attributes(
            {
                "iceberg.orphans.listed": listed,
                "iceberg.orphans.files": len(orphans),
                "iceberg.orphans.bytes": total_bytes,
            }
        )

        if not orphans:
            span.set_attribute("iceberg.outcome", "clean")
            return SweepResult(Outcome("clean", f"no orphan files ({listed} listed)"))

        age_days = cfg.orphan_min_age_seconds / 86400
        found = (
            f"{len(orphans)} orphan file(s), {total_bytes / MIB:.1f} MiB "
            f"(of {listed} listed, older than {age_days:.0f}d)"
        )

        if cfg.orphan_sweep_mode != MODE_DELETE:
            span.set_attribute("iceberg.outcome", "orphans")
            return SweepResult(
                Outcome("orphans", f"DRY RUN: would delete {found}"),
                files=len(orphans),
                bytes=total_bytes,
            )

        deleted, deleted_bytes = _delete(table, fs, orphans, cfg)
        telemetry.orphan_files_deleted.add(deleted)
        span.set_attributes(
            {"iceberg.outcome": "swept", "iceberg.orphans.deleted": deleted}
        )
        return SweepResult(
            Outcome(
                "swept",
                f"deleted {deleted} orphan file(s), {deleted_bytes / MIB:.1f} MiB "
                f"reclaimed (found {found})",
            ),
            files=len(orphans),
            bytes=total_bytes,
            deleted=deleted,
        )


def _delete(table, fs, orphans: list[Orphan], cfg: Config):
    """Delete up to the cap; a file that is already gone counts as deleted.

    The cap truncates rather than aborts — the next run takes the rest — but it
    says so, because a silent cap reads as "swept clean" when it is not.
    """
    name = ".".join(table.name())
    capped = orphans[: max(cfg.orphan_max_deletes, 0)]
    if len(capped) < len(orphans):
        log.warning(
            "%s: orphan sweep capped at %d of %d files this run "
            "(ORPHAN_MAX_DELETES) — the rest wait for the next run",
            name,
            len(capped),
            len(orphans),
        )
    deleted = 0
    deleted_bytes = 0
    for orphan in capped:
        try:
            fs.delete_file(orphan.path)
        except FileNotFoundError:
            # Someone else got there first, or the listing was stale. The file
            # is gone either way, which is the outcome we wanted.
            pass
        except Exception:
            # One unlucky key must not cost the rest of the sweep, but it must
            # not be silent either.
            log.exception("%s: could not delete orphan %s", name, orphan.path)
            continue
        deleted += 1
        deleted_bytes += orphan.size
    return deleted, deleted_bytes
