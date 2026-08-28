"""The pre-arming proof, tested for the two things it must not do.

Arming a box is a decision taken on this tool's output, so the tool failing
open would be worse than not having it. Two properties carry that weight:

  * it FAILS when the orphan set and the live set intersect — if a doctored
    sweep offering to delete a live file still prints "every check passed",
    the proof proves nothing;
  * it DELETES NOTHING, whatever ORPHAN_SWEEP_MODE says. It is run on armed
    boxes too.

Then the two recorded traps: the oldest retained snapshot can be a truncate
that plans zero files (a vacuous read-back), and rows must be counted through
the streaming path rather than `scan().to_arrow()` (which OOM-killed the pod
that first did this by hand).
"""

import os

import pyarrow
import pytest
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import PyArrowFileIO

from iceberg_maintenance import verify
from iceberg_maintenance.maintenance import compact_table, expire_snapshots
from iceberg_maintenance.orphans import Orphan, SweepRefused, find_orphans


def _io() -> PyArrowFileIO:
    return PyArrowFileIO()


def _files_under(table) -> set[str]:
    root = table.location().replace("file://", "")
    return {
        os.path.join(dirpath, name)
        for dirpath, _, names in os.walk(root)
        for name in names
    }


def _fs_path(io, location: str) -> str:
    _, _, path = io.parse_location(location, io.properties)
    return path


@pytest.fixture
def swept(table, cfg):
    """A table carrying real garbage: a compaction's old generation, expired.

    This is the state every armed box is in — the sweep has something to find,
    and none of it is live.
    """
    io = _io()
    compact_table(table, io, cfg())
    expire_snapshots(table, cfg(max_snapshot_age_ms=0, min_snapshots_to_keep=1))
    return table


def test_a_healthy_warehouse_passes_every_check(swept, warehouse, cfg, capsys):
    code = verify.run(warehouse, _io(), cfg(orphan_min_age_seconds=0), [], True, 10_000)

    assert code == 0
    out = capsys.readouterr().out
    assert "[FAIL]" not in out
    assert "every check passed" in out
    # The report has to carry the numbers an operator decides on, not just a
    # verdict — this is what "the canary read the report" means.
    assert "live data files" in out
    assert "orphans (unreachable, aged)" in out


def test_a_live_file_in_the_orphan_set_fails(swept, warehouse, cfg, monkeypatch):
    """The check the whole tool exists for.

    If the sweep ever offered to delete a file the table still reads, this is
    the run that has to come back non-zero.
    """
    io = _io()
    live = next(iter(swept.scan().plan_files())).file.file_path

    def sweep_the_live_one(table, io, cfg, now=None):
        fs, _, listed = find_orphans(table, io, cfg, now=now)
        return fs, [Orphan(_fs_path(io, live), 1024)], listed

    monkeypatch.setattr(verify, "find_orphans", sweep_the_live_one)

    assert verify.run(warehouse, io, cfg(), [], False, 0) == 1


def test_a_refusal_is_not_a_pass(swept, warehouse, cfg, monkeypatch, capsys):
    """A table the sweep refuses reports zero orphans — that is not "clean"."""

    def refuse(*args, **kwargs):
        raise SweepRefused("manifest would not parse")

    monkeypatch.setattr(verify, "find_orphans", refuse)

    assert verify.run(warehouse, _io(), cfg(), [], False, 0) == 1
    assert "REFUSED" in capsys.readouterr().out


def test_verify_deletes_nothing_even_when_armed(swept, warehouse, cfg):
    """It is run on armed boxes, so `delete` must not leak into it."""
    before = _files_under(swept)
    assert before  # the fixture left orphans to be tempted by

    code = verify.run(
        warehouse,
        _io(),
        cfg(orphan_sweep_mode="delete", orphan_min_age_seconds=0),
        [],
        True,
        10_000,
    )

    assert code == 0
    assert _files_under(swept) == before


def test_the_oldest_snapshot_with_data_is_not_the_truncate(warehouse, cfg):
    """The vacuous-pass trap, reproduced exactly.

    `nyc_taxi.yellow_trips`'s oldest retained snapshot is a truncate: it plans
    zero files, so reading it back proves nothing about whether the sweep took
    a live file. The read-back has to skip forward to the oldest snapshot that
    actually has data.
    """
    schema = pyarrow.schema([pyarrow.field("id", pyarrow.int64(), nullable=False)])
    tbl = warehouse.create_table("ns.truncated", schema=schema)
    tbl.append(pyarrow.table({"id": [1, 2, 3]}, schema=schema))
    tbl.delete(delete_filter=AlwaysTrue())
    # Drop the pre-truncate snapshot, so the OLDEST retained one is the empty
    # truncate — the state that made the first hand-run check vacuous.
    expire_snapshots(tbl, cfg(max_snapshot_age_ms=0, min_snapshots_to_keep=1))
    tbl.append(pyarrow.table({"id": [4, 5]}, schema=schema))

    oldest = min(tbl.metadata.snapshots, key=lambda s: s.timestamp_ms)
    assert not list(tbl.scan(snapshot_id=oldest.snapshot_id).plan_files())

    snapshot_id, tasks = verify.oldest_snapshot_with_data(tbl)

    assert snapshot_id != oldest.snapshot_id
    assert len(tasks) == 1


def test_rows_are_counted_without_materializing_the_table(swept, monkeypatch):
    """Never `scan().to_arrow()` — that is the OOM, not the read-back."""

    def forbidden(*args, **kwargs):
        raise AssertionError("to_arrow() must never be called by the read-back")

    monkeypatch.setattr(type(swept.scan()), "to_arrow", forbidden)

    rows, columns = verify._count_rows(swept, None, 0)

    assert (rows, columns) == (12 * 500, 2)


def test_the_read_back_limit_stops_early(table):
    """The limit bounds the read-back; it stops at a batch boundary, not mid-batch.

    Uncompacted here on purpose — one data file per batch, so "stopped early"
    is observable. On a 41M-row table this is the difference between a check
    and a full scan.
    """
    rows, _ = verify._count_rows(table, None, 10)
    assert 0 < rows < 12 * 500


def test_a_named_table_is_the_only_one_checked(swept, warehouse, cfg, capsys):
    schema = pyarrow.schema([pyarrow.field("id", pyarrow.int64(), nullable=False)])
    warehouse.create_table("ns.other", schema=schema)

    assert verify.run(warehouse, _io(), cfg(), ["ns.small_files"], False, 0) == 0

    out = capsys.readouterr().out
    assert "=== ns.small_files" in out
    assert "ns.other" not in out
    assert "1 table(s) checked" in out
