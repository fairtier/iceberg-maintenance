"""Compaction tests against a real (local, on-disk) Iceberg warehouse.

These exercise the properties that have actually broken in production: the
rewrite must produce the same rows in fewer files, it must read the scan
lazily (one data file at a time) instead of buffering the table, and a table
whose manifests can't be rewritten must be skipped before anything is written
rather than after a full rewrite.
"""

import dataclasses

import pyarrow
import pytest
from pyiceberg.catalog.memory import InMemoryCatalog

from iceberg_maintenance.config import MIB, Config
from iceberg_maintenance.maintenance import (
    compact_table,
    stream_batches,
    unrewritable_manifest_reason,
)

_FILES = 12
_ROWS_PER_FILE = 500


# Connection fields are never dialled here (the tests run against a local
# warehouse), so they are placeholders. The knobs are what matter: everything
# counts as "small" and the whole table is small files, so the size/fraction
# gates never stand between a test and the rewrite it wants to exercise.
_BASE = Config(
    catalog_uri="http://lakekeeper:8181/catalog",
    warehouse="default",
    oidc_client_id="client",
    oidc_client_secret="secret",
    oidc_token_url="https://auth.example/token",
    aws_endpoint_url="https://s3.example",
    aws_access_key_id="ak",
    aws_secret_access_key="sk",
    aws_region="auto",
    small_file_max_bytes=32 * MIB,
    min_input_files=4,
    rewrite_min_small_fraction=0.0,
    rewrite_chunk_bytes=32 * MIB,
    max_snapshot_age_ms=7 * 24 * 3600 * 1000,
    min_snapshots_to_keep=5,
)


def _cfg(**overrides) -> Config:
    return dataclasses.replace(_BASE, **overrides)


@pytest.fixture
def table(tmp_path):
    """A table of _FILES small data files, one per append."""
    catalog = InMemoryCatalog("test", warehouse=str(tmp_path))
    catalog.create_namespace("ns")
    schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64(), nullable=False),
            pyarrow.field("payload", pyarrow.string(), nullable=False),
        ]
    )
    tbl = catalog.create_table("ns.small_files", schema=schema)
    for f in range(_FILES):
        tbl.append(
            pyarrow.table(
                {
                    "id": list(range(f * _ROWS_PER_FILE, (f + 1) * _ROWS_PER_FILE)),
                    "payload": [f"row-{f}"] * _ROWS_PER_FILE,
                },
                schema=schema,
            )
        )
    return tbl


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


def test_compaction_preserves_rows_and_shrinks_file_count(table):
    before = _rows(table)
    schema_before = table.schema()
    assert len(_data_files(table)) == _FILES

    outcome = compact_table(table, table.io, _cfg())

    assert outcome.startswith("compacted"), outcome
    table.refresh()
    assert len(_data_files(table)) < _FILES
    assert _rows(table) == before
    assert table.schema() == schema_before


def test_compaction_is_a_no_op_below_the_small_file_threshold(table):
    outcome = compact_table(table, table.io, _cfg(min_input_files=_FILES + 1))

    assert outcome.startswith("healthy"), outcome
    assert len(_data_files(table)) == _FILES


def test_compaction_writes_several_chunks_when_they_are_small(table):
    """A chunk cap below the table size must flush repeatedly, not once."""
    before = _rows(table)

    outcome = compact_table(table, table.io, _cfg(rewrite_chunk_bytes=4096))

    assert outcome.startswith("compacted"), outcome
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
    assert len(tasks) == _FILES

    batches = stream_batches(scan, tasks)
    first = next(batches)
    assert len(opened) == 1

    # ...and the rest still arrive: laziness must not lose data.
    rows = first.num_rows + sum(batch.num_rows for batch in batches)
    assert rows == _FILES * _ROWS_PER_FILE
    assert opened == [task.file.file_path for task in tasks]


def test_unrewritable_manifest_is_skipped_before_anything_is_written(
    table, monkeypatch
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
    outcome = compact_table(table, table.io, _cfg())

    assert outcome.startswith("SKIPPED: manifest entry status is"), outcome
    table.refresh()
    assert _data_files(table) == before


def test_healthy_manifests_are_not_reported_unrewritable(table):
    assert unrewritable_manifest_reason(table, table.current_snapshot()) is None
