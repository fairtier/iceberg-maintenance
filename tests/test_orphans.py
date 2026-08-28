"""Orphan-sweep tests against a real (local, on-disk) Iceberg warehouse.

This is the one part of the job that deletes files, so the tests are written
around what must *not* happen. In order of how much they would cost:

  * a file the current snapshot reads is never an orphan;
  * a file only an older, still-retained snapshot reads is never an orphan
    either — that is the time-travel window, and it is a product promise;
  * a file young enough to be someone's in-flight upload is never an orphan,
    however unreachable it looks;
  * nothing at all is deleted unless the sweep is explicitly armed.

Only then: the files that genuinely are garbage — the previous generation a
compaction replaced, and parquet a lost commit left behind — do get found, and
do get removed once expiry has let go of them.

The warehouse fixture and the config factory live in conftest.py.
"""

import os
import time

import pytest
from conftest import FILES
from pyiceberg.io.pyarrow import PyArrowFileIO

from iceberg_maintenance import orphans
from iceberg_maintenance.maintenance import compact_table, expire_snapshots
from iceberg_maintenance.orphans import (
    SweepRefused,
    find_orphans,
    referenced_files,
    sweep_table,
)

# Far enough past every file the fixtures write that the age floor never
# silently explains a result. Tests that are about the floor set it themselves.
LATER = time.time() + 3650 * 86400


def _io() -> PyArrowFileIO:
    """The same FileIO the job builds — connection properties are never dialled."""
    return PyArrowFileIO()


def _stray(table, name: str = "leftover.parquet") -> str:
    """A file under the table's location that no snapshot knows about.

    This is exactly what a lost commit leaves: real parquet in the data
    directory, referenced by nothing, invisible to snapshot expiry.
    """
    path = os.path.join(table.location(), "data", name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"PAR1" + b"\0" * 1020)
    return path


def _rows(tbl) -> dict:
    return tbl.scan().to_arrow().sort_by("id").to_pydict()


def test_a_live_table_has_no_orphans(table, cfg):
    """Every file under an untouched table's location is reachable."""
    _, found, listed = find_orphans(table, _io(), cfg(), now=LATER)

    assert found == []
    assert listed > FILES  # data files + manifests + manifest lists + metadata


def test_referenced_files_covers_every_data_file_the_scan_plans(table):
    refs = referenced_files(table, _io())
    io = _io()

    planned = {
        orphans.normalize(io, task.file.file_path) for task in table.scan().plan_files()
    }
    assert planned <= refs


def test_an_unreferenced_file_is_found_but_not_deleted_in_dry_run(table, cfg):
    stray = _stray(table)

    result = sweep_table(table, _io(), cfg(orphan_sweep_mode="dry-run"), now=LATER)

    assert result.outcome.kind == "orphans", result.outcome
    assert result.files == 1
    assert result.deleted == 0
    assert "DRY RUN" in result.outcome.message
    assert os.path.exists(stray)


def test_a_young_unreferenced_file_is_left_alone(table, cfg):
    """The age floor, which is what protects a concurrent writer mid-upload."""
    stray = _stray(table)

    # `now` is the moment the file was written, so it is zero seconds old.
    result = sweep_table(table, _io(), cfg(orphan_sweep_mode="delete"), now=time.time())

    assert result.outcome.kind == "clean", result.outcome
    assert os.path.exists(stray)


def test_arming_the_sweep_removes_it_and_leaves_the_table_readable(table, cfg):
    stray = _stray(table)
    before = _rows(table)

    result = sweep_table(table, _io(), cfg(orphan_sweep_mode="delete"), now=LATER)

    assert result.outcome.kind == "swept", result.outcome
    assert (result.files, result.deleted) == (1, 1)
    assert not os.path.exists(stray)
    table.refresh()
    assert _rows(table) == before


def test_off_mode_does_not_even_look(table, cfg):
    stray = _stray(table)

    result = sweep_table(table, _io(), cfg(orphan_sweep_mode="off"), now=LATER)

    assert result.outcome.kind == "disabled", result.outcome
    assert (result.files, result.deleted) == (0, 0)
    assert os.path.exists(stray)


def test_a_retained_snapshots_files_survive_a_compaction(table, cfg):
    """The time-travel window, pinned.

    After a rewrite the previous generation of data files is unreferenced by
    the *current* snapshot and referenced by an older one that is still inside
    the window. Deleting it would break time travel — and reads of any client
    holding that snapshot id.
    """
    before = {task.file.file_path for task in table.scan().plan_files()}
    assert compact_table(table, table.io, cfg()).kind == "compacted"
    table.refresh()

    _, found, _ = find_orphans(table, _io(), cfg(), now=LATER)

    io = _io()
    orphaned = {orphan.path for orphan in found}
    for path in before:
        assert orphans.normalize(io, path) not in {
            orphans.normalize(io, candidate) for candidate in orphaned
        }


def test_expiry_is_what_releases_the_old_generation(table, cfg):
    """...and once it has, the sweep is what actually frees the storage.

    This is the whole point of the module: compaction on its own *adds* to the
    bill, because the generation it replaced stays in object storage forever.
    """
    before = {task.file.file_path for task in table.scan().plan_files()}
    assert compact_table(table, table.io, cfg()).kind == "compacted"
    table.refresh()

    # Let go of every snapshot but the current one.
    expiring = cfg(max_snapshot_age_ms=0, min_snapshots_to_keep=1)
    assert expire_snapshots(table, expiring).kind == "expired"
    table.refresh()

    result = sweep_table(table, _io(), cfg(orphan_sweep_mode="delete"), now=LATER)

    assert result.outcome.kind == "swept", result.outcome
    assert result.deleted >= len(before)
    for path in before:
        assert not os.path.exists(path)
    # The table itself is untouched by all of it.
    assert len(_rows(table)["id"]) == FILES * 500


def test_the_cap_truncates_loudly_rather_than_deleting_everything(table, cfg, caplog):
    for n in range(4):
        _stray(table, f"leftover-{n}.parquet")

    result = sweep_table(
        table, _io(), cfg(orphan_sweep_mode="delete", orphan_max_deletes=2), now=LATER
    )

    assert (result.files, result.deleted) == (4, 2)
    assert "capped at 2 of 4" in caplog.text


def test_a_shallow_location_is_refused(cfg):
    io = _io()
    for location in ("s3://bucket", "s3://bucket/warehouse"):
        with pytest.raises(SweepRefused, match="path component"):
            orphans._guard_location(io, location)
    # One level deeper is a real Lakekeeper table location.
    orphans._guard_location(io, "s3://bucket/warehouse/tbl-uuid")


def test_an_unreadable_manifest_deletes_nothing(table, cfg, monkeypatch):
    """Partial knowledge is not knowledge. It must not become a delete list."""
    stray = _stray(table)

    def boom(*args, **kwargs):
        raise OSError("manifest unreadable")

    monkeypatch.setattr(
        "pyiceberg.manifest.ManifestFile.fetch_manifest_entry", boom, raising=True
    )

    with pytest.raises(OSError):
        find_orphans(table, _io(), cfg(orphan_sweep_mode="delete"), now=LATER)
    assert os.path.exists(stray)


def test_the_loop_reports_orphans_in_the_run_summary(table, cfg, warehouse, spans):
    from iceberg_maintenance.maintenance import maintain_warehouse

    _stray(table)
    # Nothing here may compact — this test is about the sweep's numbers
    # reaching the summary, not about a rewrite happening first.
    summary = maintain_warehouse(
        warehouse,
        table.io,
        cfg(min_input_files=FILES + 1, orphan_sweep_mode="dry-run"),
    )

    assert summary.errors == 0
    assert summary.orphan_files == 0  # the stray is seconds old — age floor
    assert "iceberg.sweep_orphans" in spans()
