"""OpenTelemetry traces + metrics for the nightly run.

Wired up only when the box actually has a collector: `load_config` flips
`Config.otel_enabled` on when an OTLP endpoint variable is set (and off for
`OTEL_SDK_DISABLED=true`). With it off every call here goes through the
OpenTelemetry *API*'s no-op implementations — the instrumentation in
`maintenance.py` stays exactly where it is, costs nothing, and the job never
depends on a collector being reachable.

Two things matter for a CronJob that lives for minutes and then exits:

  * **Flush on exit.** Batched span export and periodic metric export both
    assume a long-lived process; a batch job dies before either fires and the
    whole run's telemetry goes with it. `setup` returns a shutdown callable
    that `main` runs in a `finally` — that force-flushes both pipelines.
  * **Delta temporality.** Every run is a fresh process, so cumulative counters
    would restart at zero each night and read downstream as a reset. The OTLP
    exporters honour the standard preference variable, so we only *default* it
    to delta; an explicitly configured preference still wins.

Telemetry must never take the run down: a failed setup is logged and degrades
to the no-op API, and shutdown failures are swallowed.

Attribute namespace (Iceberg has no semantic convention of its own):

  iceberg.warehouse / iceberg.namespace / iceberg.table   what is worked on
  iceberg.operation (compact | expire_snapshots)          which half of the job
  iceberg.outcome                                         how it went, as a
                                                          fixed vocabulary — see
                                                          maintenance.Outcome
  iceberg.compaction.* / iceberg.snapshots.*              the numbers behind it

Table names ride on *spans* only. They stay off metric attributes on purpose:
one time series per table per outcome is cardinality a box-local collector
should not have to carry, and the counters are meant to answer "did tonight go
well", with the trace answering "which table".
"""

import logging
import os
from collections.abc import Callable

from opentelemetry import metrics, trace

from . import __version__
from .config import Config

log = logging.getLogger("maintenance.telemetry")

SCOPE = "iceberg-maintenance"

tracer = trace.get_tracer(SCOPE, __version__)
_meter = metrics.get_meter(SCOPE, __version__)

# Instruments are created at import time, i.e. *before* `setup` installs the
# SDK. That is supported: until a provider is set the API hands back proxy
# instruments, and they start forwarding to the real ones the moment it is.
tables_scanned = _meter.create_counter(
    "iceberg.maintenance.tables.scanned",
    unit="{table}",
    description="Tables visited by the maintenance run.",
)
operations = _meter.create_counter(
    "iceberg.maintenance.operations",
    unit="{operation}",
    description=(
        "Per-table operations by outcome — the run's headline signal "
        "(alert on outcome=failed, watch outcome=conflict for write contention)."
    ),
)
operation_duration = _meter.create_histogram(
    "iceberg.maintenance.operation.duration",
    unit="s",
    description="Wall time of one per-table operation.",
)
files_rewritten = _meter.create_counter(
    "iceberg.maintenance.compaction.files.rewritten",
    unit="{file}",
    description="Data files read and replaced by a committed compaction.",
)
files_written = _meter.create_counter(
    "iceberg.maintenance.compaction.files.written",
    unit="{file}",
    description=(
        "Data files a committed compaction produced — against files.rewritten, "
        "the compaction ratio the job is actually buying."
    ),
)
bytes_rewritten = _meter.create_counter(
    "iceberg.maintenance.compaction.bytes.rewritten",
    unit="By",
    description=(
        "Bytes rewritten by committed compactions — the run's object-storage "
        "write amplification."
    ),
)
orphan_files_found = _meter.create_counter(
    "iceberg.maintenance.orphans.files.found",
    unit="{file}",
    description=(
        "Files under a table's location that no retained snapshot can reach — "
        "counted whether or not the sweep is armed to delete them."
    ),
)
orphan_bytes_found = _meter.create_counter(
    "iceberg.maintenance.orphans.bytes.found",
    unit="By",
    description=(
        "Object storage held by unreachable files — what the warehouse is "
        "paying for and nothing is reading."
    ),
)
orphan_files_deleted = _meter.create_counter(
    "iceberg.maintenance.orphans.files.deleted",
    unit="{file}",
    description=(
        "Unreachable files the sweep actually removed. Zero in dry-run mode, "
        "which is the point of having both numbers."
    ),
)
snapshots_expired = _meter.create_counter(
    "iceberg.maintenance.snapshots.expired",
    unit="{snapshot}",
    description="Snapshots expired past the time-travel window.",
)
run_duration = _meter.create_histogram(
    "iceberg.maintenance.run.duration",
    unit="s",
    description="Wall time of the whole nightly run, by outcome.",
)


def record_operation(operation: str, outcome: str, seconds: float) -> None:
    """Count one per-table operation and time it, labelled by how it ended."""
    attributes = {"iceberg.operation": operation, "iceberg.outcome": outcome}
    operations.add(1, attributes)
    operation_duration.record(seconds, attributes)


def setup(cfg: Config) -> Callable[[], None]:
    """Install the OTLP pipelines if configured; return their shutdown hook.

    The returned callable is always safe to call (a no-op when telemetry is
    off) and must be called before the process exits — see the module
    docstring on flushing.
    """
    if not cfg.otel_enabled:
        log.debug("OpenTelemetry disabled (no OTLP endpoint configured)")
        return lambda: None
    try:
        return _install(cfg)
    except Exception:
        # A missing/broken collector must cost telemetry, never the run.
        log.warning(
            "OpenTelemetry setup failed — continuing without telemetry", exc_info=True
        )
        return lambda: None


def _install(cfg: Config) -> Callable[[], None]:
    # Fresh process every night, so cumulative sums would look like a nightly
    # reset to whatever scrapes them. setdefault, not set: an operator who
    # configured a preference means it.
    os.environ.setdefault("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "delta")

    # Imported here, not at module import: the SDK and the exporter are only
    # touched on a box that has a collector, and the API alone is what the rest
    # of the package depends on.
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create(_resource_attributes(cfg))

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
    )
    metrics.set_meter_provider(meter_provider)

    _instrument_requests()
    log.info(
        "OpenTelemetry enabled (OTLP/HTTP -> %s)",
        os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "<per-signal endpoint>"),
    )

    def shutdown() -> None:
        try:
            tracer_provider.shutdown()
            meter_provider.shutdown()
        except Exception:
            log.warning("OpenTelemetry shutdown failed", exc_info=True)

    return shutdown


def _resource_attributes(cfg: Config) -> dict[str, str]:
    """Resource identity: the service, plus the warehouse it maintains.

    The warehouse belongs here rather than on every span — one box, one
    warehouse, for the whole process.

    `Resource.create` merges what we pass *over* the environment detector, so
    a service name we set unconditionally would silently beat OTEL_SERVICE_NAME.
    Fill the gap only.
    """
    attributes = {"service.version": __version__, "iceberg.warehouse": cfg.warehouse}
    configured = os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")
    if not os.environ.get("OTEL_SERVICE_NAME") and "service.name=" not in configured:
        attributes["service.name"] = "iceberg-maintenance"
    return attributes


def _instrument_requests() -> None:
    """Spans for the catalog's HTTP calls — PyIceberg's REST client uses requests.

    Best-effort, and worth the dependency: without it a trace is our own spans
    separated by unexplained gaps, exactly where Lakekeeper or the OAuth token
    endpoint was slow. Data-file IO goes through PyArrow's C++ S3 client and
    stays invisible either way.
    """
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor

        RequestsInstrumentor().instrument()
    except Exception:
        log.debug("requests instrumentation unavailable (ignored)", exc_info=True)
