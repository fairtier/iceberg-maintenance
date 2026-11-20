"""Shared fixtures: a real (local, on-disk) Iceberg warehouse, config, and an
in-memory OpenTelemetry SDK to assert against."""

import dataclasses

import pyarrow
import pyarrow.parquet as pq
import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.update import AddSnapshotUpdate, SetSnapshotRefUpdate
from pyiceberg.typedef import Record

from iceberg_maintenance.config import MIB, Config

FILES = 12
ROWS_PER_FILE = 500


# Connection fields are never dialled (the tests run against a local
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
    commit_max_attempts=1,
    # 0: the retry tests bend attempts up and must not actually wait.
    commit_retry_backoff_seconds=0.0,
    max_snapshot_age_ms=7 * 24 * 3600 * 1000,
    min_snapshots_to_keep=5,
    # The sweep is the one operation that deletes, so the shared base has it
    # OFF: a test that wants it says so (`cfg(orphan_sweep_mode="delete")`),
    # and no test acquires the power to delete by accident.
    orphan_sweep_mode="off",
    orphan_min_age_seconds=7 * 24 * 3600,
    orphan_max_deletes=1000,
)


@pytest.fixture
def cfg():
    """Factory: `cfg()` for the defaults above, `cfg(min_input_files=99)` to bend one."""

    def make(**overrides) -> Config:
        return dataclasses.replace(_BASE, **overrides)

    return make


@pytest.fixture
def warehouse(tmp_path):
    """An empty local warehouse with one namespace."""
    catalog = InMemoryCatalog("test", warehouse=str(tmp_path))
    catalog.create_namespace("ns")
    return catalog


@pytest.fixture
def table(warehouse):
    """A table of FILES small data files, one per append."""
    catalog = warehouse
    schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64(), nullable=False),
            pyarrow.field("payload", pyarrow.string(), nullable=False),
        ]
    )
    tbl = catalog.create_table("ns.small_files", schema=schema)
    for f in range(FILES):
        tbl.append(
            pyarrow.table(
                {
                    "id": list(range(f * ROWS_PER_FILE, (f + 1) * ROWS_PER_FILE)),
                    "payload": [f"row-{f}"] * ROWS_PER_FILE,
                },
                schema=schema,
            )
        )
    return tbl


EVOLVED_ROWS_PER_FILE = 10


@pytest.fixture
def evolved(warehouse):
    """A table repartitioned after its data was written (ALTER ... SET PARTITIONED BY)."""
    schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64(), nullable=False),
            pyarrow.field("g", pyarrow.int32(), nullable=False),
            pyarrow.field("h", pyarrow.int32(), nullable=False),
        ]
    )
    tbl = warehouse.create_table("ns.evolved", schema=schema)
    with tbl.update_spec() as update:
        update.add_identity("g")
    for f in range(FILES):
        ids = range(f * EVOLVED_ROWS_PER_FILE, (f + 1) * EVOLVED_ROWS_PER_FILE)
        tbl.append(
            pyarrow.table(
                {
                    "id": list(ids),
                    "g": [0] * EVOLVED_ROWS_PER_FILE,
                    "h": [0] * EVOLVED_ROWS_PER_FILE,
                },
                schema=schema,
            )
        )
    with tbl.update_spec() as update:
        update.remove_field("g")
        update.add_identity("h")
    return tbl


def commit_delete_file(
    tbl,
    rows_per_data_file=1,
    *,
    partition=None,
    content=DataFileContent.POSITION_DELETES,
    file_format=FileFormat.PARQUET,
    entry_content=None,
    manifest_content=ManifestContent.DELETES,
    removed=False,
    summary=None,
    spell=lambda path: path,
):
    """Commit a snapshot adding one real delete file, as DuckDB does.

    PyIceberg cannot write delete files, so the parquet, manifest and manifest
    list are written by hand. It deletes the first rows of every live data file.

    `spell` rewrites how the delete file names each target, e.g. with a scheme
    the manifests don't use: readers resolve both to the same object.

    By default it has the shape DuckDB 1.5.4 gives a table repartitioned after
    its data was written: filed under the current spec with the same partition
    value as its data files (Record[0] under spec 2 beside Record[0] under spec
    1), so PyIceberg's scan matches it to no data file, and with no
    `total-delete-files` in the summary. (With data under the new spec as well,
    DuckDB writes Record[0, None] instead; that matches nothing either.)
    """

    class _Writer(ManifestWriterV2):
        def content(self):
            return manifest_content

    spec = tbl.spec()
    snapshot_id = tbl.metadata.new_snapshot_id()
    sequence_number = tbl.metadata.next_sequence_number()
    parent = tbl.current_snapshot()
    location = f"{tbl.metadata.location}/metadata/{snapshot_id}"

    path = f"{tbl.metadata.location}/data/{snapshot_id}-deletes.parquet"
    targets = [
        (spell(task.file.file_path), position)
        for task in tbl.scan().plan_files()
        for position in range(rows_per_data_file)
    ]
    written = pyarrow.table(
        {
            "file_path": pyarrow.array([t[0] for t in targets], pyarrow.string()),
            "pos": pyarrow.array([t[1] for t in targets], pyarrow.int64()),
        }
    )
    with tbl.io.new_output(path).create() as stream:
        pq.write_table(written, stream)

    delete_file = DataFile.from_args(
        content=content if entry_content is None else entry_content,
        file_path=path,
        file_format=file_format,
        partition=Record(0) if partition is None else partition,
        record_count=written.num_rows,
        file_size_in_bytes=tbl.io.new_input(path).__len__(),
    )
    delete_file.spec_id = spec.spec_id
    with _Writer(
        spec,
        tbl.schema(),
        tbl.io.new_output(f"{location}-m0.avro"),
        snapshot_id,
        "null",
    ) as writer:
        writer.add_entry(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.DELETED
                if removed
                else ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=sequence_number,
                file_sequence_number=sequence_number,
                data_file=delete_file,
            )
        )
    manifest = writer.to_manifest_file()
    with write_manifest_list(
        2,
        tbl.io.new_output(f"{location}.avro"),
        snapshot_id,
        parent.snapshot_id,
        sequence_number,
        "null",
    ) as manifest_list:
        manifest_list.add_manifests([*parent.manifests(tbl.io), manifest])
    tbl.catalog.commit_table(
        tbl,
        (),
        (
            AddSnapshotUpdate(
                snapshot=Snapshot(
                    snapshot_id=snapshot_id,
                    parent_snapshot_id=parent.snapshot_id,
                    sequence_number=sequence_number,
                    timestamp_ms=parent.timestamp_ms + 1,
                    manifest_list=f"{location}.avro",
                    summary=Summary(Operation.DELETE, **(summary or {})),
                    schema_id=tbl.schema().schema_id,
                )
            ),
            SetSnapshotRefUpdate(
                ref_name="main",
                type="branch",
                snapshot_id=snapshot_id,
            ),
        ),
    )
    tbl.refresh()


def append_rows(tbl, ids):
    tbl.append(
        pyarrow.table(
            {"id": ids, "g": [1] * len(ids), "h": [0] * len(ids)},
            schema=tbl.schema().as_arrow(),
        )
    )
    tbl.refresh()


def replace_rows(tbl, ids):
    """What dlt's `replace` does: a truncate, committed alone, then appends."""
    tbl.delete(delete_filter=AlwaysTrue())
    tbl.refresh()
    append_rows(tbl, ids)


@pytest.fixture(scope="session")
def _sdk():
    """An in-memory OpenTelemetry SDK, installed once.

    Session-scoped because the global providers can only be set once per
    process — a second `set_tracer_provider` is ignored with a warning — so
    per-test isolation comes from draining the exporter, not from reinstalling.
    Note this also proves the lazily-bound instruments in `telemetry` work:
    they are created at import, long before this provider exists.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    reader = InMemoryMetricReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    return exporter, reader


@pytest.fixture
def spans(_sdk):
    """The spans finished during this test, by name — drained beforehand."""
    exporter, _ = _sdk
    exporter.clear()

    def finished() -> dict:
        return {span.name: span for span in exporter.get_finished_spans()}

    return finished


@pytest.fixture
def counter(_sdk):
    """Reads one counter's running total — the SDK's sums are cumulative."""
    _, reader = _sdk

    def total(name: str) -> float:
        collected = reader.get_metrics_data()
        return sum(
            point.value
            for resource_metric in collected.resource_metrics
            for scope_metric in resource_metric.scope_metrics
            for metric in scope_metric.metrics
            if metric.name == name
            for point in metric.data.data_points
        )

    return total
