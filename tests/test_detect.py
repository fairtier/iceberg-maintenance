"""The read-only detection script's verdicts, against a real local warehouse.

A wrong DAMAGED hands an operator a recovery snapshot that discards a customer's
load, so the two false ones that shipped are pinned next to a true one.
"""

import importlib.util
import json
import sys
from itertools import count
from pathlib import Path
from types import SimpleNamespace

import pyarrow
import pytest
from conftest import EVOLVED_ROWS_PER_FILE, FILES, commit_delete_file, replace_rows
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.manifest import (
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestFile,
)
from pyiceberg.typedef import Record

from iceberg_maintenance.maintenance import (
    commit_swap,
    delete_files_reason,
    stream_batches,
)

_SCRIPT = Path(__file__).parents[1] / "scripts" / "detect_delete_exposure.py"
_spec = importlib.util.spec_from_file_location("detect_delete_exposure", _SCRIPT)
assert _spec and _spec.loader
detect = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(detect)

ALL_ROWS = FILES * EVOLVED_ROWS_PER_FILE


def _logged_as_lakekeeper_does(tbl):
    """Leave the one transaction's empty snapshot out of the snapshot log.

    Lakekeeper does this. InMemoryCatalog does not, so do it here.
    """
    empty = sorted(tbl.metadata.snapshots, key=lambda s: s.sequence_number)[-2]
    tbl.metadata = tbl.metadata.model_copy(
        update={
            "snapshot_log": [
                entry
                for entry in tbl.metadata.snapshot_log
                if entry.snapshot_id != empty.snapshot_id
            ]
        }
    )


def _old_image_rewrite(tbl, cfg):
    """What image <= 0.10.1 did once its two checks had passed."""
    import uuid

    scan = tbl.scan()
    tasks = list(scan.plan_files())
    chunk = pyarrow.Table.from_batches(list(stream_batches(scan, tasks)))
    data_files = list(
        _dataframe_to_data_files(
            table_metadata=tbl.metadata,
            df=chunk,
            io=tbl.io,
            write_uuid=uuid.uuid4(),
            counter=count(0),
        )
    )
    commit_swap(tbl, data_files, tbl.current_snapshot().snapshot_id, cfg())
    tbl.refresh()
    _logged_as_lakekeeper_does(tbl)


def test_a_table_without_deletes_is_clean(table):
    assert detect.inspect(table)["verdict"] == "clean"


def test_deletes_no_old_check_sees_are_exposed(evolved):
    commit_delete_file(evolved)

    report = detect.inspect(evolved)

    assert report["verdict"] == "EXPOSED"
    assert report["planned_tasks_with_deletes"] == 0
    assert report["blocks_compaction"] is True


def test_deletes_the_summary_reports_are_covered(evolved):
    commit_delete_file(evolved, summary={"total-delete-files": "1"})

    assert detect.inspect(evolved)["verdict"] == "covered"


def _assert_unconfirmed(report, verdict, candidate):
    assert report["verdict"] == verdict, report
    assert report["candidate_snapshot_id"] == candidate
    assert report["after_snapshot_id"]
    assert report["note"].startswith("unconfirmed")
    assert "recovery_snapshot_id" not in report


def test_a_rewrite_that_undid_deletes_is_suspected_damage(evolved, cfg):
    commit_delete_file(evolved, 3)
    before = evolved.current_snapshot().snapshot_id

    _old_image_rewrite(evolved, cfg)

    report = detect.inspect(evolved)
    _assert_unconfirmed(report, "SUSPECTED_DAMAGE", before)
    assert report["rows_before_swap"] == report["rows_after_swap"] == ALL_ROWS


def test_an_atomic_same_size_overwrite_is_never_called_confirmed(evolved):
    """A customer's reload with every row new looks exactly like the rewrite.

    It used to report DAMAGED with a recovery id, and following it would have
    discarded the reload. No row count separates the two, so the script offers a
    candidate to compare and nothing to roll back to.
    """
    commit_delete_file(evolved)
    before = evolved.current_snapshot().snapshot_id
    reload = pyarrow.table(
        {
            "id": list(range(9000, 9000 + ALL_ROWS)),
            "g": [1] * ALL_ROWS,
            "h": [0] * ALL_ROWS,
        },
        schema=evolved.schema().as_arrow(),
    )

    evolved.overwrite(reload)
    evolved.refresh()
    _logged_as_lakekeeper_does(evolved)

    report = detect.inspect(evolved)
    _assert_unconfirmed(report, "SUSPECTED_DAMAGE", before)
    assert report["rows_before_swap"] == report["rows_after_swap"] == ALL_ROWS


def _with_scheme(path):
    """The same local object, spelled the way a delete file may spell it."""
    return path if path.startswith("file:") else f"file://{path}"


def test_a_differently_spelled_live_target_is_exposed_not_dangling(evolved):
    """Readers resolve both spellings to one object, so the delete is live."""
    commit_delete_file(evolved, spell=_with_scheme)

    report = detect.inspect(evolved)

    assert report["verdict"] == "EXPOSED", report
    assert report["dangling_delete_rows_upper_bound"] == 0


def test_a_rewrite_is_suspected_whatever_the_spelling_of_its_targets(evolved, cfg):
    commit_delete_file(evolved, 3, spell=_with_scheme)
    before = evolved.current_snapshot().snapshot_id

    _old_image_rewrite(evolved, cfg)

    _assert_unconfirmed(detect.inspect(evolved), "SUSPECTED_DAMAGE", before)


@pytest.mark.parametrize("trimmed", ["parent", "child"])
def test_a_trimmed_log_does_not_show_atomicity(evolved, cfg, trimmed):
    """Absent from the log proves nothing if its neighbours are absent as well."""
    commit_delete_file(evolved)
    before = evolved.current_snapshot().snapshot_id
    _old_image_rewrite(evolved, cfg)
    gone = before if trimmed == "parent" else evolved.current_snapshot().snapshot_id
    evolved.metadata = evolved.metadata.model_copy(
        update={
            "snapshot_log": [
                entry
                for entry in evolved.metadata.snapshot_log
                if entry.snapshot_id != gone
            ]
        }
    )

    report = detect.inspect(evolved)

    _assert_unconfirmed(report, "NEEDS_HUMAN_CHECK", before)
    assert report["committed_atomically"] is False


def test_a_later_rewrite_of_reloaded_rows_is_not_blamed(evolved, cfg):
    """It held none of the files the deletes aim at, so it undid nothing."""
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3, 4, 5, 6])

    _old_image_rewrite(evolved, cfg)

    report = detect.inspect(evolved)
    assert report["verdict"] == "replaced", report
    assert report["rows_after_swap"] == 6


def test_a_replace_is_not_damage(evolved):
    """DuckDB DELETE, truncate, then a load of 6 rows, as dlt's replace does it.

    It used to report DAMAGED with a recovery snapshot of the old rows, and
    following that would have discarded the customer's load.
    """
    commit_delete_file(evolved)

    replace_rows(evolved, [1, 2, 3, 4, 5, 6])

    report = detect.inspect(evolved)
    assert report["verdict"] == "replaced", report
    assert report["rows_before_swap"] == ALL_ROWS
    assert report["rows_after_swap"] == 6
    assert "candidate_snapshot_id" not in report


def test_a_replace_does_not_clear_deletes_written_after_it(evolved):
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3, 4, 5, 6])
    commit_delete_file(evolved, partition=Record(1))

    report = detect.inspect(evolved)

    assert report["verdict"] == "EXPOSED", report
    assert report["rows_after_swap"] == 6


def test_a_same_size_replace_needs_a_human(evolved):
    """Row counts cannot clear a reload of the same size."""
    commit_delete_file(evolved)
    before = evolved.current_snapshot().snapshot_id

    replace_rows(evolved, list(range(ALL_ROWS)))

    report = detect.inspect(evolved)
    _assert_unconfirmed(report, "NEEDS_HUMAN_CHECK", before)
    assert report["rows_before_swap"] == report["rows_after_swap"] == ALL_ROWS


def test_a_rewrite_after_an_earlier_replace_is_still_suspected(evolved, cfg):
    """The replace explains the older deletes, not the ones the rewrite undid."""
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3, 4, 5, 6])
    commit_delete_file(evolved)
    before = evolved.current_snapshot().snapshot_id

    _old_image_rewrite(evolved, cfg)

    report = detect.inspect(evolved)
    _assert_unconfirmed(report, "SUSPECTED_DAMAGE", before)
    assert report["rows_before_swap"] == report["rows_after_swap"] == 6


def test_a_rewrite_whose_parent_expired_needs_a_human(evolved, cfg):
    """No rows left to compare, so even the suspicion cannot be earned."""
    commit_delete_file(evolved)
    before = evolved.current_snapshot().snapshot_id
    _old_image_rewrite(evolved, cfg)
    evolved.maintenance.expire_snapshots().by_ids([before]).commit()
    evolved.refresh()

    report = detect.inspect(evolved)

    assert report["verdict"] == "NEEDS_HUMAN_CHECK", report
    assert report["note"].startswith("unconfirmed")
    assert "candidate_snapshot_id" not in report


def test_a_partial_drop_after_expiry_is_not_damage(evolved):
    """A delete, then one whole file dropped, then one snapshot expired: row count fine.

    Lakekeeper keeps parent_snapshot_id after expiry, so "the oldest retained
    snapshot has a parent" holds on healthy tables and proves nothing.
    """
    commit_delete_file(evolved)
    evolved.delete(delete_filter="id == 0")
    evolved.refresh()
    oldest = min(evolved.metadata.snapshots, key=lambda s: s.sequence_number)
    evolved.maintenance.expire_snapshots().by_ids([oldest.snapshot_id]).commit()
    evolved.refresh()

    report = detect.inspect(evolved)

    assert report["verdict"] == "DANGLING", report
    assert "candidate_snapshot_id" not in report


def test_equality_deletes_are_reported_not_inspected(evolved):
    commit_delete_file(evolved, content=DataFileContent.EQUALITY_DELETES)

    report = detect.inspect(evolved)

    assert report["verdict"] == "NOT_INSPECTED", report
    assert report["equality_delete_files"] == 1
    assert report["delete_files_not_inspected"] == 1


def test_a_deletion_vector_earns_no_exposed_verdict(evolved):
    commit_delete_file(evolved, file_format=FileFormat.PUFFIN)

    report = detect.inspect(evolved)

    assert report["verdict"] == "NOT_INSPECTED", report
    assert report["delete_files_not_inspected"] == 1


def test_removed_delete_entries_are_counted_and_shown(evolved):
    commit_delete_file(evolved, removed=True)

    report = detect.inspect(evolved)

    assert report["verdict"] == "dead", report
    assert report["delete_files"] == 0
    assert report["removed_delete_entries"] == 1
    assert report["blocks_compaction"] is True


def test_a_re_added_file_named_by_an_old_delete_is_not_dead(evolved):
    """DuckDB applies position deletes by path, whatever the sequence numbers."""
    paths = [task.file.file_path for task in evolved.scan().plan_files()]
    commit_delete_file(evolved)
    evolved.delete(delete_filter=AlwaysTrue())
    evolved.refresh()
    evolved.add_files(paths)
    evolved.refresh()

    report = detect.inspect(evolved)

    assert report["blocks_compaction"] is True
    assert report["verdict"] == "EXPOSED", report


def _sequences(monkeypatch, sequence_of):
    original = ManifestFile.fetch_manifest_entry

    def patched(self, io, discard_deleted=True):
        entries = original(self, io, discard_deleted)
        for entry in entries:
            entry.sequence_number = sequence_of(entry)
        return entries

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", patched)


def test_a_data_file_at_the_same_sequence_is_not_dead(evolved, monkeypatch):
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3])
    _sequences(monkeypatch, lambda entry: 99)

    assert detect.inspect(evolved)["blocks_compaction"] is True
    assert delete_files_reason(evolved, evolved.current_snapshot())


def test_an_unresolved_sequence_number_is_not_dead(evolved, monkeypatch):
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3])
    _sequences(
        monkeypatch,
        lambda entry: (
            None
            if entry.data_file.content == DataFileContent.DATA
            else entry.sequence_number
        ),
    )

    assert detect.inspect(evolved)["blocks_compaction"] is True
    assert delete_files_reason(evolved, evolved.current_snapshot())


def test_dead_deletes_on_a_v3_table_are_not_dead(evolved):
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3])
    v3 = SimpleNamespace(metadata=SimpleNamespace(format_version=3))

    files = detect._read(evolved, evolved.current_snapshot())

    assert detect._older_than_every_data_file(evolved, files) is True
    assert detect._older_than_every_data_file(v3, files) is False


@pytest.mark.parametrize(
    ("build", "verdict", "status"),
    [
        pytest.param(lambda t: None, "clean", 0, id="clean"),
        pytest.param(
            lambda t: (commit_delete_file(t), replace_rows(t, [1, 2])),
            "replaced",
            0,
            id="replaced",
        ),
        pytest.param(
            lambda t: (
                commit_delete_file(t),
                t.delete(delete_filter="id == 0"),
                t.refresh(),
            ),
            "DANGLING",
            1,
            id="dangling",
        ),
        pytest.param(commit_delete_file, "EXPOSED", 1, id="exposed"),
    ],
)
def test_main_reports_and_exits_on_the_verdict(
    evolved, warehouse, cfg, monkeypatch, capsys, build, verdict, status
):
    build(evolved)
    monkeypatch.setattr(detect, "load_config", cfg)
    monkeypatch.setattr(detect, "load_catalog", lambda *a, **k: warehouse)
    monkeypatch.setattr(detect, "direct_s3_io", lambda c: evolved.io)
    monkeypatch.setattr(sys, "argv", ["detect", "--json"])

    assert detect.main() == status

    report = json.loads(capsys.readouterr().out)
    assert (report["table"], report["verdict"]) == ("ns.evolved", verdict)


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda t: None, id="no-deletes"),
        pytest.param(commit_delete_file, id="unmatched"),
        pytest.param(
            lambda t: commit_delete_file(t, removed=True), id="removed-entries"
        ),
        pytest.param(
            lambda t: commit_delete_file(t, manifest_content=ManifestContent.DATA),
            id="delete-entry-in-data-manifest",
        ),
        pytest.param(
            lambda t: commit_delete_file(t, content=DataFileContent.EQUALITY_DELETES),
            id="equality",
        ),
        pytest.param(
            lambda t: (commit_delete_file(t), replace_rows(t, [1, 2])), id="dead"
        ),
        pytest.param(
            lambda t: (
                commit_delete_file(t, content=DataFileContent.EQUALITY_DELETES),
                replace_rows(t, [1, 2]),
            ),
            id="dead-equality",
        ),
        pytest.param(
            lambda t: (
                commit_delete_file(t),
                t.delete(delete_filter="id == 0"),
                t.refresh(),
            ),
            id="older-data-file",
        ),
    ],
)
def test_the_script_and_the_guard_agree(evolved, build):
    build(evolved)

    guard = delete_files_reason(evolved, evolved.current_snapshot())

    assert detect.inspect(evolved).get("blocks_compaction", False) == (
        guard is not None
    )
