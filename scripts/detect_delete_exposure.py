"""Read-only: find tables a pre-fix compaction may have damaged, or still would.

Image <= 0.10.1 decided "this table has no delete files" from the snapshot
summary's `total-delete-files` and from PyIceberg's matching of delete files to
data files. Both can be wrong, and the rewrite then brings deleted rows back and
turns each UPDATE's old version into a duplicate. That rewrite carries the dead
delete manifests along, so its footprint is position deletes aimed at data files
the current snapshot no longer has. Other things leave that too, so the verdict
depends on the snapshot that removed those files:

  SUSPECTED_DAMAGE  it was a whole-table `delete` with its parent and child in
             the snapshot log and itself not, so atomic with the next snapshot,
             and that next snapshot holds exactly as many rows as the old data
             files did. That is the rewrite's footprint, and also that of any
             atomic overwrite of the same size, such as PyIceberg's `overwrite()`.
             Unconfirmed: this script cannot tell them apart.
  NEEDS_HUMAN_CHECK  it was whole-table but atomicity is not shown or the
             snapshots to compare are expired, so even that footprint is not
             established. Also unconfirmed.
  replaced   it was whole-table and the row counts differ. The rewrite never
             changes the count, so this was a replace, truncate or overwrite (dlt's
             `replace` commits this shape). Its deletes are dead. Nothing to do.
             Deletes written since it are judged as below, with its counts shown.
  DANGLING   no whole-table delete on record. A partial drop does this, and it is
             harmless if the row count is right, but compare it with what you
             expect: expiry can have dropped the evidence.
  EXPOSED    live deletes that neither old check sees: an old image would rewrite
             this table (subject to the size gates) and undo them.
  NOT_INSPECTED  deletes this script cannot read (equality deletes, deletion
             vectors), and the old checks don't vouch for them. No verdict earned.
  covered    deletes an old image's checks do see, so it would skip.
  dead       delete entries that cannot apply: all older than every data file
             and aimed at no live data file, or already removed.
  clean      no delete files. Nothing here can be undone by compaction.
  ERROR      could not read it. Not evidence of anything either way.

For those two, `candidate_snapshot_id` is the parent of the whole-table delete,
`after_snapshot_id` its child, and both row counts are given, where known. A
person must compare the data at both by time travel before doing anything. A
rollback to the candidate also discards every later commit, and it discards the
customer's load if this was an overwrite. The next nightly expiry can remove it.

`blocks_compaction` says whether the fixed job skips the table for its deletes.
Its rule (`delete_files_reason`) is repeated here on purpose: this script is for
boxes still running an old image, which does not have that function.
tests/test_detect.py holds the two copies to the same answers.

It only ever loads table metadata and reads manifests and position-delete
parquet. It commits nothing, writes nothing and deletes nothing.

It is not in the image (the Dockerfile copies only src/). It needs the job's
environment variables and an importable `iceberg_maintenance`, for its config
and IO helpers, so either pipe it to the image's Python, from a container that
has that environment:

    python - [--json] [namespace.table ...] < scripts/detect_delete_exposure.py

or run it from a checkout with those variables set:

    uv run python scripts/detect_delete_exposure.py [--json] [namespace.table ...]

Exit status is 0 if every verdict is clean, covered, dead or replaced, else 1.
"""

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass, field

import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import (
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntryStatus,
)

from iceberg_maintenance.config import load_config
from iceberg_maintenance.maintenance import all_namespaces, direct_s3_io
from iceberg_maintenance.orphans import normalize

_FINE = {"clean", "covered", "dead", "replaced"}
_UNCONFIRMED = (
    "unconfirmed: compare the data at both snapshots by time travel; "
    "a rollback also discards every later commit"
)


@dataclass
class _Files:
    """One snapshot's manifests, classified the way the job's guard classifies them."""

    live_paths: set[str] = field(default_factory=set)
    live_rows: int = 0
    data_sequences: list = field(default_factory=list)
    has_deletes: bool = False
    live_deletes: list = field(default_factory=list)
    removed_deletes: int = 0
    delete_sequences: list = field(default_factory=list)
    equality_deletes: int = 0


def _read(table, snapshot) -> _Files:
    files = _Files()
    for manifest in snapshot.manifests(io=table.io):
        is_delete_manifest = manifest.content != ManifestContent.DATA
        if is_delete_manifest:
            files.has_deletes = True
            files.delete_sequences.append(manifest.sequence_number)
        for entry in manifest.fetch_manifest_entry(io=table.io, discard_deleted=False):
            data_file = entry.data_file
            removed = entry.status == ManifestEntryStatus.DELETED
            if is_delete_manifest or data_file.content != DataFileContent.DATA:
                files.has_deletes = True
                files.delete_sequences.append(entry.sequence_number)
                if data_file.content == DataFileContent.EQUALITY_DELETES:
                    files.equality_deletes += 1
                if removed:
                    files.removed_deletes += 1
                else:
                    files.live_deletes.append(data_file)
            elif not isinstance(entry.status, ManifestEntryStatus):
                files.data_sequences.append(None)
            elif not removed:
                files.data_sequences.append(entry.sequence_number)
                files.live_paths.add(data_file.file_path)
                files.live_rows += data_file.record_count
    return files


def _older_than_every_data_file(table, files: _Files) -> bool:
    """The guard's exemption. See delete_files_reason for why each clause is there."""
    sequences = [*files.delete_sequences, *files.data_sequences]
    return (
        table.metadata.format_version == 2
        and not files.equality_deletes
        and all(sequence is not None for sequence in sequences)
        and (
            not files.data_sequences
            or min(files.data_sequences) > max(files.delete_sequences)
        )
    )


def _position_delete_targets(table, data_file) -> set[str] | None:
    """Data file paths a position-delete file refers to, normalized; None if not knowable.

    Normalized because a delete file may spell a path differently from the
    manifests (`s3a://` for `s3://`, doubled slashes) and readers resolve both to
    the same object. Every set compared with these must be normalized too.
    """
    if (
        data_file.content != DataFileContent.POSITION_DELETES
        or data_file.file_format != FileFormat.PARQUET
    ):
        return None  # Equality deletes have no targets; puffin needs a blob read.
    with table.io.new_input(data_file.file_path).open() as stream:
        column = pq.read_table(stream, columns=["file_path"]).column("file_path")
    return {normalize(table.io, str(path)) for path in column.unique().to_pylist()}


def _normalized(table, paths) -> set[str]:
    return {normalize(table.io, path) for path in paths}


def _aimed_at_live_data(table, files: _Files) -> bool:
    """The guard's check: could a live delete name a live data file? Unknown is yes."""
    live = _normalized(table, files.live_paths)
    for delete_file in files.live_deletes:
        targets = _position_delete_targets(table, delete_file)
        if targets is None or targets & live:
            return True
    return False


def _whole_table_deletes(table) -> list:
    """`delete` snapshots that leave no live data file, oldest first.

    The summary's `total-data-files` cannot show that: PyIceberg counts on from
    whatever DuckDB's summaries said. So it is read from the manifests.
    """
    found = []
    for snapshot in table.metadata.snapshots:
        summary = snapshot.summary
        if not (summary and summary.operation.value == "delete"):
            continue
        if int(summary.get("deleted-data-files") or 0) and not (
            _read(table, snapshot).live_paths
        ):
            found.append(snapshot)
    return sorted(found, key=lambda s: s.sequence_number)


def _judge_dangling(table, dangling_targets: set[str]) -> dict:
    """What removed the data files these deletes were aimed at."""
    metadata = table.metadata
    candidates = _whole_table_deletes(table)
    report: dict = {"whole_table_deletes_on_record": len(candidates)}
    parents_gone = 0
    needs_check: dict | None = None
    replaced: dict | None = None
    head = table.current_snapshot()
    logged = {entry.snapshot_id for entry in metadata.snapshot_log}
    for candidate in candidates:
        parent = metadata.snapshot_by_id(candidate.parent_snapshot_id)
        if parent is None:
            parents_gone += 1
            continue
        before = _read(table, parent)
        if not _normalized(table, before.live_paths) & dangling_targets:
            continue

        children = [
            s
            for s in metadata.snapshots
            if s.parent_snapshot_id == candidate.snapshot_id
        ]
        # One transaction: the catalog never made the empty table current, so the
        # snapshot log skips it. A separate truncate, like dlt's replace, is in it.
        # Absence means that only with both neighbours logged: a log can be trimmed.
        atomic = (
            candidate.snapshot_id not in logged
            and parent.snapshot_id in logged
            and bool(children)
            and children[0].snapshot_id in logged
        )
        rows_after = None
        if children:
            rows_after = _read(table, children[0]).live_rows
        elif head.snapshot_id == candidate.snapshot_id:
            rows_after = 0
        evidence = {
            "rows_before_swap": before.live_rows,
            "rows_after_swap": rows_after,
            "committed_atomically": atomic,
            "candidate_snapshot_id": parent.snapshot_id,
            "after_snapshot_id": children[0].snapshot_id if children else None,
            "note": _UNCONFIRMED,
        }
        # A replace does not clear what a later rewrite did, so keep looking.
        if rows_after is not None and rows_after != before.live_rows:
            replaced = replaced or {
                k: evidence[k]
                for k in ("rows_before_swap", "rows_after_swap", "committed_atomically")
            }
        elif atomic and rows_after is not None:
            return {**report, **evidence, "verdict": "SUSPECTED_DAMAGE"}
        else:
            needs_check = needs_check or evidence
    if needs_check:
        return {**report, **needs_check, "verdict": "NEEDS_HUMAN_CHECK"}
    if parents_gone:
        return {**report, "verdict": "NEEDS_HUMAN_CHECK", "note": _UNCONFIRMED}
    if replaced:
        return {**report, **replaced, "verdict": "replaced"}
    return {**report, "verdict": "DANGLING"}


def _dangling_targets(table, files: _Files) -> tuple[set[str], int, int]:
    """(data files aimed at but not live, rows in those delete files, not inspected)."""
    targets: set[str] = set()
    rows = unknown = 0
    live = _normalized(table, files.live_paths)
    for delete_file in files.live_deletes:
        aimed_at = _position_delete_targets(table, delete_file)
        if aimed_at is None:
            unknown += 1
        elif aimed_at - live:
            targets |= aimed_at - live
            rows += delete_file.record_count
    return targets, rows, unknown


def inspect(table) -> dict:
    snapshot = table.current_snapshot()
    report: dict = {
        "format_version": table.metadata.format_version,
        "partition_specs": len(table.metadata.partition_specs),
        "snapshots_retained": len(table.metadata.snapshots),
    }
    if snapshot is None:
        return {**report, "verdict": "clean", "delete_files": 0}

    summary = snapshot.summary or {}
    report["summary_total_delete_files"] = summary.get("total-delete-files")

    files = _read(table, snapshot)
    report["delete_files"] = len(files.live_deletes)
    report["removed_delete_entries"] = files.removed_deletes
    report["equality_delete_files"] = files.equality_deletes
    report["deleted_rows_on_record"] = sum(d.record_count for d in files.live_deletes)
    if not files.has_deletes:
        return {**report, "verdict": "clean", "blocks_compaction": False}
    dead = _older_than_every_data_file(table, files) and not _aimed_at_live_data(
        table, files
    )
    report["blocks_compaction"] = not dead

    targets, dangling_rows, unknown = _dangling_targets(table, files)
    report["dangling_delete_rows_upper_bound"] = dangling_rows
    report["delete_files_not_inspected"] = unknown

    if dangling_rows:
        judged = _judge_dangling(table, targets)
        report |= judged
        # A replace explains the deletes it orphaned, not ones written since.
        if judged["verdict"] != "replaced" or dead:
            return report
    if dead or not files.live_deletes:
        return {**report, "verdict": "dead"}

    summary_sees = int(summary.get("total-delete-files") or 0) > 0
    if files.equality_deletes:
        # PyIceberg's plan raises on them, so there is no matching to report.
        report["planned_tasks_with_deletes"] = None
        matched = 0
    else:
        matched = sum(1 for task in table.scan().plan_files() if task.delete_files)
        report["planned_tasks_with_deletes"] = matched
    if summary_sees or matched:
        return {**report, "verdict": "covered"}
    return {**report, "verdict": "NOT_INSPECTED" if unknown else "EXPOSED"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tables", nargs="*", help="namespace.table; default all")
    parser.add_argument("--json", action="store_true", help="one JSON object per table")
    args = parser.parse_args()

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
    names = args.tables or [
        ".".join(ident)
        for ns in all_namespaces(catalog)
        for ident in catalog.list_tables(ns)
    ]

    verdicts: Counter = Counter()
    for name in names:
        try:
            table = catalog.load_table(name)
            table.io = s3_io
            report = inspect(table)
        except Exception as exc:  # noqa: BLE001 — one table must not hide the rest
            report = {"verdict": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
        verdicts[report["verdict"]] += 1
        if args.json:
            print(json.dumps({"table": name, **report}, default=str))
        else:
            detail = ", ".join(
                f"{k}={v}"
                for k, v in report.items()
                if k != "verdict" and v is not None
            )
            print(f"{report['verdict']:9s} {name}  {detail}")

    print(f"# {dict(verdicts)}", file=sys.stderr)
    return 0 if set(verdicts) <= _FINE else 1


if __name__ == "__main__":
    sys.exit(main())
