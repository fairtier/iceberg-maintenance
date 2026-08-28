"""Prove, before a box is armed, that the orphan set and the live set are disjoint.

`orphans.py` is the only code in the fleet that deletes customer data, and it
is armed **per box** (`icebergOrphanSweepArmed` in the FairTier monorepo's
apps/box/apps/values.yaml). The precondition for arming a box is not that the
sweep behaved somewhere else: it is that on *this* warehouse, against *these*
real objects, the files nothing references and the files everything reads are
two disjoint sets. A warehouse written by an engine we have not met is not
covered by another box's proof — DuckDB's Iceberg writer already decodes
differently enough to make compaction unsafe on the tables it wrote.

This module is that check as shipped, tested code rather than the throwaway
script it was the first time (2026-08-28, fokume). It is **read-only by
construction**: it calls `find_orphans`, never `sweep_table`, so no value of
`ORPHAN_SWEEP_MODE` — including `delete` — can make it remove anything.

Two traps it exists in order not to fall into again, both hit by that script:

  * **The oldest retained snapshot may plan zero files.** `yellow_trips`'s
    oldest was a truncate, so "the oldest snapshot still reads" was vacuously
    true of a snapshot that reads nothing. `--read-back` skips forward to the
    oldest snapshot that actually has data.
  * **`scan().to_arrow()` is not a read-back, it is an OOM.** It buffers whole
    files behind an executor (see `stream_batches`) and got the 1Gi
    verification pod killed at exit 137. Rows are counted through the same
    streaming path the nightly rewrite uses.

Usage — from the box, cloned off the CronJob so the environment matches
exactly; the full procedure is docs/runbooks/arming-the-orphan-sweep.md:

    python -m iceberg_maintenance.verify [--table ns.name ...] [--read-back]

Exit code is the verdict: 0 every check passed, 1 anything failed or refused.
"""

import argparse
import sys
from dataclasses import dataclass, field

from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import ManifestContent

from .config import MIB, Config, load_config
from .maintenance import all_namespaces, direct_s3_io, release_memory, stream_batches
from .orphans import (
    SweepRefused,
    find_orphans,
    normalize,
    normalize_listed,
    referenced_files,
)

# How many rows `--read-back` pulls before it is satisfied. This is a proof
# that the files are there and the table still decodes, not a full-table scan:
# on a 41M-row table reading everything would take the verification pod far
# longer than the answer is worth.
DEFAULT_ROW_LIMIT = 100_000


@dataclass(frozen=True)
class Check:
    """One assertion about this warehouse, and what it saw."""

    name: str
    ok: bool
    detail: str = ""


@dataclass
class TableReport:
    """What the check found for one table. `ok` is what gates arming."""

    name: str
    checks: list[Check] = field(default_factory=list)
    listed: int = 0
    referenced: int = 0
    live: int = 0
    orphans: int = 0
    orphan_bytes: int = 0
    by_prefix: dict[str, list[int]] = field(default_factory=dict)
    refused: str = ""

    @property
    def ok(self) -> bool:
        return not self.refused and all(check.ok for check in self.checks)


def _fs_path(io, location: str) -> str:
    """The filesystem-native spelling of a location (`bucket/key`, or a path)."""
    _, _, path = io.parse_location(location, io.properties)
    return path


def live_data_files(table) -> set[str]:
    """Every data file the table reads *now* — the set that must never be swept."""
    return {task.file.file_path for task in table.scan().plan_files()}


def inspect_table(table, io, cfg: Config) -> tuple[TableReport, object | None]:
    """Run every disjointness check on one table.

    Returns the report and the filesystem the listing used, so a caller doing
    `--read-back` can stat files through the same one rather than building a
    second.

    A `SweepRefused` here is not a pass. It means the sweep declines this table
    — which is safe, and is also the state in which the orphan metrics read
    zero for a table that may be carrying gigabytes, so the report says so
    loudly and the exit code fails.
    """
    report = TableReport(name=".".join(table.name()))
    try:
        fs, found, listed = find_orphans(table, io, cfg)
    except SweepRefused as exc:
        report.refused = str(exc)
        return report, None

    refs = referenced_files(table, io)
    orphan_set = {normalize_listed(orphan.path) for orphan in found}
    live = {normalize(io, path) for path in live_data_files(table)}

    report.listed = listed
    report.referenced = len(refs)
    report.live = len(live)
    report.orphans = len(found)
    report.orphan_bytes = sum(orphan.size for orphan in found)

    location = normalize(io, table.location())
    for orphan in found:
        relative = normalize_listed(orphan.path)
        if relative.startswith(location):
            relative = relative[len(location) :].strip("/")
        prefix = relative.split("/")[0] if "/" in relative else "(root)"
        bucket = report.by_prefix.setdefault(prefix, [0, 0])
        bucket[0] += 1
        bucket[1] += orphan.size

    stranded = live - refs
    report.checks.append(
        Check(
            "live data files are all referenced",
            not stranded,
            f"{len(stranded)} live file(s) outside the reference set",
        )
    )
    swept_live = live & orphan_set
    report.checks.append(
        Check(
            "no live data file is in the orphan set",
            not swept_live,
            f"{len(swept_live)} live file(s) would be deleted: "
            f"{sorted(swept_live)[:3]}",
        )
    )

    if table.metadata_location:
        metadata = normalize(io, table.metadata_location)
        report.checks.append(
            Check(
                "current metadata.json is not an orphan",
                metadata not in orphan_set,
                metadata,
            )
        )

    snapshot = table.current_snapshot()
    if snapshot is None:
        # No snapshot means no manifest list and no manifests to check. The
        # table is empty, which is a legitimate state, not a failure.
        report.checks.append(Check("table has no snapshot (nothing to read)", True))
        return report, fs

    if snapshot.manifest_list:
        manifest_list = normalize(io, snapshot.manifest_list)
        report.checks.append(
            Check(
                "current manifest list is not an orphan",
                manifest_list not in orphan_set,
                manifest_list,
            )
        )
    manifests = {normalize(io, m.manifest_path) for m in snapshot.manifests(io=io)}
    swept_manifests = manifests & orphan_set
    report.checks.append(
        Check(
            "current manifests are not orphans",
            not swept_manifests,
            f"{len(swept_manifests)} of {len(manifests)} would be deleted",
        )
    )
    return report, fs


def oldest_snapshot_with_data(table) -> tuple[int | None, list]:
    """The oldest retained snapshot that actually plans files, and its tasks.

    Not simply the oldest snapshot: `yellow_trips`'s oldest retained snapshot
    is a truncate and plans **zero** files, so reading it back proves nothing
    at all. The far end of the time-travel window is the oldest snapshot a
    customer could get rows out of, and that is the one worth proving.
    """
    for snapshot in sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms):
        tasks = list(table.scan(snapshot_id=snapshot.snapshot_id).plan_files())
        if tasks:
            return snapshot.snapshot_id, tasks
    return None, []


def _missing(fs, io, tasks) -> int:
    """How many of these planned data files are not in object storage."""
    from pyarrow.fs import FileType

    paths = [_fs_path(io, task.file.file_path) for task in tasks]
    if not paths:
        return 0
    return sum(1 for info in fs.get_file_info(paths) if info.type == FileType.NotFound)


def _count_rows(table, snapshot_id: int | None, limit: int) -> tuple[int, int]:
    """Rows and columns actually read back, through the streaming path.

    NEVER `scan().to_arrow()`: `ArrowScan` submits every data file to an
    executor up front and holds each finished file's batches, so a read-back of
    a large table is an OOM rather than a check (exit 137 on the 1Gi pod that
    first ran this by hand). `stream_batches` opens one file at a time.
    """
    scan = table.scan() if snapshot_id is None else table.scan(snapshot_id=snapshot_id)
    tasks = list(scan.plan_files())
    rows = 0
    columns = 0
    for batch in stream_batches(scan, tasks):
        rows += batch.num_rows
        columns = batch.num_columns
        if limit and rows >= limit:
            break
    return rows, columns


def read_back(table, io, fs, limit: int) -> list[Check]:
    """Prove the table still reads — at HEAD and at the far end of time travel.

    This is the check that matters *after* an armed run: the disjointness proof
    says the sweep should not have touched anything live, and this says it
    did not.
    """
    checks: list[Check] = []
    head_tasks = list(table.scan().plan_files())
    missing = _missing(fs, io, head_tasks)
    print(f"  HEAD             : {len(head_tasks)} data files, {missing} missing")
    checks.append(Check("HEAD data files all present", missing == 0, f"{missing} gone"))

    snapshot_id, tasks = oldest_snapshot_with_data(table)
    if snapshot_id is None:
        print("  oldest with data : none (no snapshot plans any file)")
    else:
        missing = _missing(fs, io, tasks)
        print(
            f"  oldest with data : id={snapshot_id} {len(tasks)} data files, "
            f"{missing} missing"
        )
        checks.append(
            Check(
                "oldest-with-data snapshot's files all present",
                missing == 0,
                f"{missing} gone",
            )
        )

    # Delete files make this a pre-delete row count; say so rather than
    # printing a number that quietly means something else.
    snapshot = table.current_snapshot()
    merge_on_read = snapshot is not None and any(
        m.content != ManifestContent.DATA for m in snapshot.manifests(io=io)
    )
    try:
        rows, columns = _count_rows(table, None, limit)
    except Exception as exc:  # noqa: BLE001 - reported, not raised: this is a check
        print(f"  read back        : FAILED — {exc}")
        checks.append(Check("HEAD reads back", False, str(exc)))
        return checks
    capped = " (stopped at --rows)" if limit and rows >= limit else ""
    caveat = " [pre-delete: table has delete files]" if merge_on_read else ""
    print(f"  read back        : {rows} rows, {columns} columns{capped}{caveat}")
    checks.append(Check("HEAD reads back", True))
    return checks


def _human(size: int) -> str:
    return (
        f"{size / MIB:.1f} MiB" if size < 1024 * MIB else f"{size / 1024 / MIB:.2f} GiB"
    )


def print_report(report: TableReport) -> None:
    print(f"=== {report.name}")
    if report.refused:
        print(f"  REFUSED: {report.refused}")
        print("  the sweep will not touch this table — and reports 0 orphans for it")
        return
    print(f"  live data files              : {report.live}")
    print(f"  referenced (all snapshots)   : {report.referenced}")
    print(f"  listed under table location  : {report.listed}")
    print(
        f"  orphans (unreachable, aged)  : {report.orphans}  "
        f"{_human(report.orphan_bytes)}"
    )
    for prefix, (count, size) in sorted(report.by_prefix.items()):
        print(f"    {prefix:<24}   : {count:>8}  {_human(size)}")


def print_checks(checks: list[Check]) -> None:
    for check in checks:
        if check.ok:
            print(f"  [PASS] {check.name}")
        else:
            print(f"  [FAIL] {check.name} — {check.detail}")


def identifiers(catalog, wanted: list[str]):
    """The tables to check: the ones named, or every table in the warehouse."""
    if wanted:
        return [tuple(name.split(".")) for name in wanted]
    return [
        ident for ns in all_namespaces(catalog) for ident in catalog.list_tables(ns)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m iceberg_maintenance.verify",
        description=(
            "Read-only proof that the orphan sweep's delete list is disjoint "
            "from everything this warehouse still reads. Run it before arming "
            "a box, and again after that box's first armed run."
        ),
    )
    parser.add_argument(
        "--table",
        action="append",
        default=[],
        metavar="ns.name",
        help="check only this table (repeatable; default: every table)",
    )
    parser.add_argument(
        "--read-back",
        action="store_true",
        help=(
            "also read each table at HEAD and at the oldest snapshot that has "
            "data, checking every planned file still exists"
        ),
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROW_LIMIT,
        help=f"stop the read-back after this many rows (default {DEFAULT_ROW_LIMIT})",
    )
    args = parser.parse_args(argv)

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
    io = direct_s3_io(cfg)
    return run(catalog, io, cfg, args.table, args.read_back, args.rows)


def run(catalog, io, cfg: Config, wanted, do_read_back: bool, rows: int) -> int:
    """The body of `main`, separated from argument parsing and catalog wiring."""
    print(
        f"verify: warehouse={cfg.warehouse} mode={cfg.orphan_sweep_mode} "
        f"min-age={cfg.orphan_min_age_seconds}s (READ-ONLY — nothing is deleted)"
    )
    tables = 0
    failed: list[str] = []
    for ident in identifiers(catalog, wanted):
        tables += 1
        table = catalog.load_table(ident)
        # Same substitution the nightly run makes: Lakekeeper forces an IO whose
        # signer never fires (see direct_s3_io).
        table.io = io
        report, fs = inspect_table(table, io, cfg)
        print_report(report)
        print_checks(report.checks)
        ok = report.ok
        if do_read_back and fs is not None:
            back = read_back(table, io, fs, rows)
            print_checks(back)
            ok = ok and all(check.ok for check in back)
        if not ok:
            failed.append(report.name)
        del table
        release_memory()

    print()
    if failed:
        print(f"verify: {tables} table(s) checked, FAILED on {', '.join(failed)}")
        print("DO NOT ARM this box. The sweep's delete list is not provably safe here.")
        return 1
    print(f"verify: {tables} table(s) checked, every check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
