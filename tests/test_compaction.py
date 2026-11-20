"""Compaction tests against a real (local, on-disk) Iceberg warehouse.

These exercise the properties that have actually broken in production: the
rewrite must produce the same rows in fewer files, it must read the scan
lazily (one data file at a time) instead of buffering the table, and a table
whose manifests can't be rewritten must be skipped before anything is written
rather than after a full rewrite.

The warehouse fixture and the config factory live in conftest.py.
"""

from functools import partial
from types import SimpleNamespace

import pytest
import requests
from conftest import (
    FILES,
    ROWS_PER_FILE,
    append_rows,
    commit_delete_file,
    replace_rows,
)
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    ValidationException,
)
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.manifest import (
    DataFileContent,
    ManifestContent,
    ManifestFile,
)
from pyiceberg.table import TableProperties

from iceberg_maintenance.maintenance import (
    compact_table,
    delete_files_reason,
    maintain_warehouse,
    run_operation,
    stream_batches,
    unrewritable_manifest_reason,
)


def _data_files(tbl) -> list[str]:
    return [task.file.file_path for task in tbl.scan().plan_files()]


def _rows(tbl) -> dict:
    """Row values, order- and Arrow-string-flavour-independent.

    Compared as pydicts rather than pa.Tables on purpose: the rewrite reads
    through the scan's projected schema, so a `string` column comes back as
    `large_string` (both are Iceberg `string`, and this predates the streaming
    rewrite — PyIceberg's own reader casts the same way). The Iceberg schema
    and the values are what must not move.
    """
    return tbl.scan().to_arrow().sort_by("id").to_pydict()


def test_compaction_preserves_rows_and_shrinks_file_count(table, cfg):
    before = _rows(table)
    schema_before = table.schema()
    assert len(_data_files(table)) == FILES

    outcome = compact_table(table, table.io, cfg())

    assert outcome.kind == "compacted", outcome
    assert outcome.message.startswith("compacted"), outcome
    table.refresh()
    assert len(_data_files(table)) < FILES
    assert _rows(table) == before
    assert table.schema() == schema_before


def test_compaction_is_a_no_op_below_the_small_file_threshold(table, cfg):
    outcome = compact_table(table, table.io, cfg(min_input_files=FILES + 1))

    assert outcome.kind == "healthy", outcome
    assert len(_data_files(table)) == FILES


def test_compaction_writes_several_chunks_when_they_are_small(table, cfg):
    """A chunk cap below the table size must flush repeatedly, not once."""
    before = _rows(table)

    outcome = compact_table(table, table.io, cfg(rewrite_chunk_bytes=4096))

    assert outcome.kind == "compacted", outcome
    table.refresh()
    assert len(_data_files(table)) > 1
    assert _rows(table) == before


def test_stream_batches_reads_one_file_at_a_time(table, monkeypatch):
    """The regression guard for the 2026-07-31 OOM.

    PyIceberg's own `scan.to_arrow_batch_reader()` submits every data file to
    an executor up front and holds each finished file's batches in memory
    until the consumer catches up — O(table size), which OOM-killed the box.
    Pulling a single batch must therefore open exactly ONE data file.
    """
    import pyiceberg.io.pyarrow as pyarrow_io

    opened = []
    original = pyarrow_io._task_to_record_batches

    def counting(io, task, *args, **kwargs):
        opened.append(task.file.file_path)
        return original(io, task, *args, **kwargs)

    monkeypatch.setattr(pyarrow_io, "_task_to_record_batches", counting)

    scan = table.scan()
    tasks = list(scan.plan_files())
    assert len(tasks) == FILES

    batches = stream_batches(scan, tasks)
    first = next(batches)
    assert len(opened) == 1

    # ...and the rest still arrive: laziness must not lose data.
    rows = first.num_rows + sum(batch.num_rows for batch in batches)
    assert rows == FILES * ROWS_PER_FILE
    assert opened == [task.file.file_path for task in tasks]


def _shift_manifest_entry_headers(monkeypatch):
    """Make PyIceberg mis-decode this warehouse's manifests, as DuckDB's do.

    Tables written by DuckDB's Iceberg writer come back with the entry header
    shifted (`status` holding the snapshot id, `sequence_number` None), so the
    swap's delete manifest is rejected at commit — after the whole table has
    already been streamed into fresh parquet. Simulated by mangling the decoded
    entries the same way.
    """
    from pyiceberg.manifest import ManifestFile

    original = ManifestFile.fetch_manifest_entry

    def shifted(self, io, discard_deleted=True):
        entries = original(self, io, discard_deleted)
        for entry in entries:
            # setattr, not attribute syntax: these assignments are deliberately
            # ill-typed — that is the whole point of the fixture (PyIceberg
            # hands back a `status` that is not a status).
            setattr(entry, "status", 7366955187931005335)  # noqa: B010 — a snapshot id
            setattr(entry, "snapshot_id", None)  # noqa: B010
            setattr(entry, "sequence_number", None)  # noqa: B010
        return entries

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", shifted)


def test_unrewritable_manifest_is_skipped_before_anything_is_written(
    table, cfg, monkeypatch
):
    """A manifest PyIceberg can't decode must cost nothing, not a full rewrite."""
    _shift_manifest_entry_headers(monkeypatch)

    before = _data_files(table)
    outcome = compact_table(table, table.io, cfg())

    assert outcome.kind == "unsupported", outcome
    assert outcome.message.startswith("SKIPPED: manifest entry status is"), outcome
    table.refresh()
    assert _data_files(table) == before


@pytest.mark.parametrize(
    "not_a_candidate",
    [
        # Every file is above the small-file threshold, so none of them counts
        # toward min_input_files: one workspace's two affected tables after
        # SMALL_FILE_MAX_BYTES dropped to 8 MiB on 2026-08-27.
        pytest.param({"small_file_max_bytes": 1}, id="below-min-input-files"),
        # Small files are a sliver of a big table — the write-amplification gate.
        pytest.param({"rewrite_min_small_fraction": 1.1}, id="below-small-fraction"),
    ],
)
def test_unrewritable_manifest_is_reported_even_when_not_a_candidate(
    table, cfg, monkeypatch, not_a_candidate
):
    """The regression guard for the 2026-08-27 silence.

    `unsupported` counts tables compaction can never run on — a property of who
    wrote the table, not of whether tonight's size gates happen to elect it. So
    a table nobody would compact anyway must still be *reported*, or tuning a
    threshold silently resolves BoxIcebergTablesUnsupported on a box where the
    interop bug is exactly where it was.
    """
    _shift_manifest_entry_headers(monkeypatch)

    outcome = compact_table(table, table.io, cfg(**not_a_candidate))

    assert outcome.kind == "unsupported", outcome


def test_healthy_manifests_are_not_reported_unrewritable(table):
    assert unrewritable_manifest_reason(table, table.current_snapshot()) is None


def _flaky_commit(
    monkeypatch, fail_times, *, apply_anyway=False, exc=None, before=None
):
    """Make the catalog fail the first `fail_times` commits of the swap.

    `apply_anyway` applies the commit before raising — the ambiguous case a
    5xx on the commit endpoint leaves behind (the write landed, the response
    did not). `before` runs just before the raise, to let another writer in.
    """
    from pyiceberg.catalog.memory import InMemoryCatalog

    original = InMemoryCatalog.commit_table
    state = {"calls": 0, "armed": False}

    def flaky(self, table, requirements, updates):
        if not state["armed"] or state["calls"] >= fail_times:
            return original(self, table, requirements, updates)
        state["calls"] += 1
        if apply_anyway:
            original(self, table, requirements, updates)
        if before is not None:
            before()
        raise (exc or requests.exceptions.ConnectionError("catalog gone"))

    monkeypatch.setattr(InMemoryCatalog, "commit_table", flaky)
    return state


def test_commit_is_retried_when_the_catalog_is_unavailable(table, cfg, monkeypatch):
    """The regression guard for the 2026-08-27 loss.

    A completed rewrite is minutes of streaming and gigabytes through object
    storage; the commit is one HTTP call. Throwing the first away because the
    second was refused turns a blip into a night of orphans.
    """
    before = _rows(table)
    state = _flaky_commit(monkeypatch, fail_times=2)
    state["armed"] = True

    outcome = compact_table(table, table.io, cfg(commit_max_attempts=5))

    assert outcome.kind == "compacted", outcome
    assert state["calls"] == 2
    table.refresh()
    assert len(_data_files(table)) < FILES
    assert _rows(table) == before


def test_commit_gives_up_after_the_attempt_budget(table, cfg, monkeypatch):
    """Bounded, not infinite: concurrencyPolicy is Forbid, so a pinned run
    would eat the following nights too."""
    state = _flaky_commit(monkeypatch, fail_times=99)
    state["armed"] = True

    with pytest.raises(requests.exceptions.ConnectionError):
        compact_table(table, table.io, cfg(commit_max_attempts=3))

    assert state["calls"] == 3
    table.refresh()
    assert len(_data_files(table)) == FILES  # untouched


def test_a_lost_commit_race_is_not_retried(table, cfg, monkeypatch):
    """A concurrent writer advanced the branch: the rewrite is stale by
    definition, so yield rather than insist.

    Counted in *our* attempts, not in catalog calls. Since pyiceberg 0.12.0 a
    failed commit is re-offered inside `Transaction.commit_transaction`
    (`commit.retry.num-retries`, default 4) before it ever reaches us, so one
    attempt of ours is `1 + num_retries` calls. What this pins is that
    `commit_swap` adds no attempts of its own on top: were it retrying, five
    attempts would be five times this number.
    """
    per_attempt = 1 + TableProperties.COMMIT_NUM_RETRIES_DEFAULT

    state = _flaky_commit(
        monkeypatch, fail_times=99, exc=CommitFailedException("someone else won")
    )
    state["armed"] = True

    with pytest.raises(CommitFailedException):
        compact_table(table, table.io, cfg(commit_max_attempts=5))

    assert state["calls"] == per_attempt


def test_the_inner_retry_does_not_clobber_a_concurrent_writer(
    table, cfg, warehouse, monkeypatch
):
    """The same hazard as the test below, on pyiceberg's own retry path.

    `test_a_concurrent_write_during_the_retry_is_not_clobbered` guards the
    retry *we* wrote. Since 0.12.0 there is a second one underneath it: on
    `CommitFailedException` the transaction refreshes and rebuilds the swap
    against whatever the branch head is now — which is precisely the rebase
    `commit_swap` refuses to do, and with `delete(ALWAYS_TRUE)` it would drop
    the concurrent writer's rows.

    It does not, because 0.12.0 validates before it rebuilds: our delete
    predicate is ALWAYS_TRUE at serializable isolation, so any data file added
    in the meantime is a conflict and the attempt ends as a
    `ValidationException`. This test is what says so out loud — if a future
    bump loosens that validation, the rows disappear here first.
    """
    import pyarrow

    def concurrent_append():
        other = warehouse.load_table("ns.small_files")
        other.append(
            pyarrow.table(
                {"id": [999_999], "payload": ["written-by-someone-else"]},
                schema=other.schema().as_arrow(),
            )
        )

    state = _flaky_commit(
        monkeypatch,
        fail_times=1,
        exc=CommitFailedException("someone else won"),
        before=concurrent_append,
    )
    state["armed"] = True

    with pytest.raises(ValidationException):
        compact_table(table, table.io, cfg(commit_max_attempts=1))

    # The rewrite yielded before the rebase: one call, and their row is live.
    assert state["calls"] == 1
    table.refresh()
    assert 999_999 in _rows(table)["id"]
    assert len(_rows(table)["id"]) == FILES * ROWS_PER_FILE + 1


def test_a_landed_commit_is_not_committed_twice(table, cfg, monkeypatch):
    """500/502/504 on the commit endpoint says "state unknown", not "failed".

    The write may have been applied and only the response lost, so the retry
    re-reads the table instead of taking the exception at its word — otherwise
    it mints a second, identical snapshot for nothing.
    """
    before = _rows(table)
    state = _flaky_commit(
        monkeypatch,
        fail_times=1,
        apply_anyway=True,
        exc=CommitStateUnknownException("504 from the catalog"),
    )
    state["armed"] = True

    outcome = compact_table(table, table.io, cfg(commit_max_attempts=5))

    assert outcome.kind == "compacted", outcome
    assert state["calls"] == 1
    table.refresh()
    snapshots_after_the_swap = [
        s
        for s in table.metadata.snapshots
        if s.snapshot_id == table.metadata.current_snapshot_id
    ]
    assert len(snapshots_after_the_swap) == 1
    assert len(_data_files(table)) < FILES
    assert _rows(table) == before


def test_a_concurrent_write_during_the_retry_is_not_clobbered(table, cfg, monkeypatch):
    """The hazard the retry introduces, and must not fall into.

    The swap is `delete(ALWAYS_TRUE) + append(our files)` — safe only against
    the snapshot it was planned on. Refreshing onto a snapshot somebody *else*
    produced would make that delete succeed and silently drop their rows.
    """
    import pyarrow

    def concurrent_append():
        other = table.catalog.load_table(table.name())
        other.append(
            pyarrow.table(
                {"id": [999_999], "payload": ["written-by-someone-else"]},
                schema=other.schema().as_arrow(),
            )
        )

    state = _flaky_commit(monkeypatch, fail_times=1, before=concurrent_append)
    state["armed"] = True

    with pytest.raises(CommitFailedException, match="stale"):
        compact_table(table, table.io, cfg(commit_max_attempts=5))

    table.refresh()
    assert 999_999 in _rows(table)["id"]


def _assert_left_untouched(tbl, cfg):
    head = tbl.current_snapshot().snapshot_id
    files = _data_files(tbl)

    outcome = compact_table(tbl, tbl.io, cfg())

    assert outcome.kind == "unsupported", outcome
    assert outcome.message.startswith("SKIPPED: has delete files"), outcome
    tbl.refresh()
    assert tbl.current_snapshot().snapshot_id == head
    assert _data_files(tbl) == files


@pytest.mark.parametrize(
    "summary",
    [
        # DuckDB 1.5.3/1.5.4: the key is simply not there.
        pytest.param(None, id="summary-silent"),
        # A later writer counting on from a summary that had no total.
        pytest.param({"total-delete-files": "0"}, id="summary-says-zero"),
    ],
)
def test_unmatched_delete_files_are_skipped(evolved, cfg, summary):
    """The regression guard for compaction bringing deleted rows back.

    Both signals the job used to rely on say "no deletes" here: the summary
    carries no count and PyIceberg's scan matches the delete file to nothing.
    The rewrite then read the data files without the deletes and committed
    them, while still reporting "compacted".
    """
    commit_delete_file(evolved, summary=summary)
    assert not any(task.delete_files for task in evolved.scan().plan_files())

    _assert_left_untouched(evolved, cfg)


def test_delete_files_are_reported_even_when_not_a_candidate(evolved, cfg):
    """`unsupported` is a property of the table, not of tonight's size gates."""
    commit_delete_file(evolved)

    outcome = compact_table(evolved, evolved.io, cfg(min_input_files=FILES + 1))

    assert outcome.kind == "unsupported", outcome


def test_a_delete_manifest_counts_even_if_every_entry_was_removed(evolved, cfg):
    commit_delete_file(evolved, removed=True)

    _assert_left_untouched(evolved, cfg)


def test_a_data_typed_entry_in_a_delete_manifest_counts(evolved, cfg):
    """The manifest's label is evidence by itself, whatever its entries claim."""
    commit_delete_file(evolved, entry_content=DataFileContent.DATA)

    _assert_left_untouched(evolved, cfg)


def test_a_delete_entry_in_a_data_manifest_counts(evolved, cfg):
    """A DATA label on the manifest does not vouch for its entries."""
    commit_delete_file(evolved, manifest_content=ManifestContent.DATA)

    _assert_left_untouched(evolved, cfg)


def test_a_removed_delete_entry_in_a_data_manifest_counts(evolved, cfg):
    """Only reading removed entries too can see this one."""
    commit_delete_file(evolved, manifest_content=ManifestContent.DATA, removed=True)

    _assert_left_untouched(evolved, cfg)


def test_dead_deletes_do_not_block_compaction_and_rows_survive_it(evolved, cfg):
    """After a truncate or replace, deletes carried forward can reach nothing.

    Counts the rows through a real rewrite: nothing is lost, and the deletes
    (aimed at files long gone) take nothing from the rows the replace wrote.
    """
    commit_delete_file(evolved, 3, summary={"total-delete-files": "1"})
    replace_rows(evolved, list(range(1000, 1007)))
    for k in range(5):
        append_rows(evolved, [2000 + k])
    expected = sorted([*range(1000, 1007), *range(2000, 2005)])
    # The truncate left the summary's count standing. It is stale, not evidence.
    assert int(evolved.current_snapshot().summary["total-delete-files"]) > 0

    outcome = compact_table(evolved, evolved.io, cfg())

    assert outcome.kind == "compacted", outcome
    evolved.refresh()
    assert sorted(evolved.scan().to_arrow().column("id").to_pylist()) == expected
    assert sum(task.file.record_count for task in evolved.scan().plan_files()) == 12


def test_one_data_file_older_than_the_deletes_still_blocks(evolved, cfg):
    """A partial drop: the older files the deletes apply to are still live."""
    commit_delete_file(evolved)
    evolved.delete(delete_filter="id == 0")
    evolved.refresh()

    _assert_left_untouched(evolved, cfg)


def _with_sequence(monkeypatch, sequence_of):
    """Rewrite the data sequence number PyIceberg resolved for each entry."""
    original = ManifestFile.fetch_manifest_entry

    def patched(self, io, discard_deleted=True):
        entries = original(self, io, discard_deleted)
        for entry in entries:
            entry.sequence_number = sequence_of(entry)
        return entries

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", patched)


def test_a_data_file_at_the_same_sequence_as_a_delete_blocks(evolved, cfg, monkeypatch):
    """Position deletes apply at <=. DuckDB's UPDATE writes both in one commit."""
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3])
    newest_data = max(
        entry.sequence_number
        for manifest in evolved.current_snapshot().manifests(evolved.io)
        if manifest.content == ManifestContent.DATA
        for entry in manifest.fetch_manifest_entry(evolved.io)
    )
    _with_sequence(monkeypatch, lambda entry: newest_data)

    _assert_left_untouched(evolved, cfg)


@pytest.mark.parametrize("unresolved", ["data", "delete"])
def test_an_unresolved_sequence_number_blocks(evolved, cfg, monkeypatch, unresolved):
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3])
    data = unresolved == "data"
    _with_sequence(
        monkeypatch,
        lambda entry: (
            None
            if (entry.data_file.content == DataFileContent.DATA) == data
            else entry.sequence_number
        ),
    )

    assert delete_files_reason(evolved, evolved.current_snapshot())


def test_a_live_path_named_by_an_old_delete_still_blocks(evolved, cfg):
    """DELETE, truncate, then the same files re-added under a new sequence number.

    Every data file is newer than the delete, so the spec says it cannot apply.
    DuckDB applies it by path anyway, so a rewrite would move DuckDB's row count.
    """
    paths = _data_files(evolved)
    commit_delete_file(evolved, 3)
    evolved.delete(delete_filter=AlwaysTrue())
    evolved.refresh()
    evolved.add_files(paths)
    evolved.refresh()

    _assert_left_untouched(evolved, cfg)


def test_a_live_path_spelled_differently_by_an_old_delete_still_blocks(evolved, cfg):
    """The path check compares objects, not strings: a scheme is not a new file."""
    paths = _data_files(evolved)
    commit_delete_file(
        evolved,
        3,
        spell=lambda path: path if path.startswith("file:") else f"file://{path}",
    )
    evolved.delete(delete_filter=AlwaysTrue())
    evolved.refresh()
    evolved.add_files(paths)
    evolved.refresh()

    _assert_left_untouched(evolved, cfg)


def test_an_unreadable_delete_file_fails_rather_than_exempts(evolved, cfg):
    commit_delete_file(evolved)
    deletes = [
        entry.data_file.file_path
        for manifest in evolved.current_snapshot().manifests(evolved.io)
        if manifest.content == ManifestContent.DELETES
        for entry in manifest.fetch_manifest_entry(evolved.io)
    ]
    replace_rows(evolved, [1, 2, 3])
    evolved.io.delete(deletes[0])
    head = evolved.current_snapshot().snapshot_id

    outcome = run_operation(
        "t", "compact", "compaction", partial(compact_table, evolved, evolved.io, cfg())
    )

    assert outcome.kind == "failed", outcome
    evolved.refresh()
    assert evolved.current_snapshot().snapshot_id == head


def test_dead_equality_deletes_still_block(evolved, cfg):
    """PyIceberg's plan raises on them, so exempting one is a nightly `failed`."""
    commit_delete_file(evolved, content=DataFileContent.EQUALITY_DELETES)
    replace_rows(evolved, [1, 2, 3])

    assert delete_files_reason(evolved, evolved.current_snapshot())


def test_dead_deletes_on_a_v3_table_still_block(evolved):
    """The swap cannot commit v3, so exempting one is a rewrite that always fails."""
    commit_delete_file(evolved)
    replace_rows(evolved, [1, 2, 3])
    v3 = SimpleNamespace(
        metadata=SimpleNamespace(format_version=3), io=evolved.io, name=evolved.name
    )

    assert delete_files_reason(v3, evolved.current_snapshot())


def test_an_emptied_table_with_dead_deletes_has_no_delete_reason(evolved):
    commit_delete_file(evolved)
    evolved.delete(delete_filter=AlwaysTrue())
    evolved.refresh()

    assert delete_files_reason(evolved, evolved.current_snapshot()) is None


def test_unreadable_manifests_fail_and_write_nothing(table, cfg, monkeypatch):
    """A read error is a fault, not a property of the table: never a skip."""

    def unreadable(self, io, discard_deleted=True):
        raise OSError("connection reset")

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", unreadable)

    head = table.current_snapshot().snapshot_id
    outcome = run_operation(
        "t", "compact", "compaction", partial(compact_table, table, table.io, cfg())
    )

    assert outcome.kind == "failed", outcome
    table.refresh()
    assert table.current_snapshot().snapshot_id == head


def test_delete_files_are_counted_as_unsupported_and_named(evolved, cfg, warehouse):
    commit_delete_file(evolved)

    summary = maintain_warehouse(warehouse, evolved.io, cfg())

    assert (summary.errors, summary.unsupported_tables) == (0, ["ns.evolved"])


def test_a_table_without_delete_files_has_no_delete_reason(table):
    assert delete_files_reason(table, table.current_snapshot()) is None


def test_an_evolved_table_without_delete_files_still_compacts(evolved, cfg):
    """The guard is about deletes, not about partition evolution."""
    before = _rows(evolved)

    outcome = compact_table(evolved, evolved.io, cfg())

    assert outcome.kind == "compacted", outcome
    evolved.refresh()
    assert _rows(evolved) == before
