"""Shared fixtures: a real (local, on-disk) Iceberg warehouse, config, and an
in-memory OpenTelemetry SDK to assert against."""

import dataclasses

import pyarrow
import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pyiceberg.catalog.memory import InMemoryCatalog

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
    max_snapshot_age_ms=7 * 24 * 3600 * 1000,
    min_snapshots_to_keep=5,
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
