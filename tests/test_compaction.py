"""Compaction tests against a real (local, on-disk) Iceberg warehouse.

These exercise the properties that have actually broken in production: the
rewrite must produce the same rows in fewer files, it must read the scan
lazily (one data file at a time) instead of buffering the table, and a table
whose manifests can't be rewritten must be skipped before anything is written
rather than after a full rewrite.

The warehouse fixture and the config factory live in conftest.py.
"""

from conftest import FILES, ROWS_PER_FILE

from iceberg_maintenance.maintenance import (
    compact_table,
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


def test_unrewritable_manifest_is_skipped_before_anything_is_written(
    table, cfg, monkeypatch
):
    """A manifest PyIceberg can't decode must cost nothing, not a full rewrite.

    Tables written by DuckDB's Iceberg writer come back with the entry header
    shifted (`status` holding the snapshot id, `sequence_number` None), so the
    swap's delete manifest is rejected at commit — after the whole table has
    already been streamed into fresh parquet. Simulated here by mangling the
    decoded entries the same way.
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

    before = _data_files(table)
    outcome = compact_table(table, table.io, cfg())

    assert outcome.kind == "unsupported", outcome
    assert outcome.message.startswith("SKIPPED: manifest entry status is"), outcome
    table.refresh()
    assert _data_files(table) == before


def test_healthy_manifests_are_not_reported_unrewritable(table):
    assert unrewritable_manifest_reason(table, table.current_snapshot()) is None
