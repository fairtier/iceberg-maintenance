"""Telemetry tests: the run must be observable, and never depend on it.

Two halves. First, that a real compaction against the local warehouse produces
the spans, attributes and metrics an operator is expected to alert on — the
per-table outcome label above all, since it is what turns "the job exited 0"
into "and here is what it actually did". Second, that the instrumentation is
inert without a collector: no endpoint configured means the API's no-op
implementations, and a per-table operation that raises must still come back as
a counted outcome rather than as an exception through the loop.

The in-memory SDK, the warehouse and the config factory live in conftest.py.
"""

import pytest
from pyiceberg.exceptions import CommitFailedException

from iceberg_maintenance import telemetry
from iceberg_maintenance.maintenance import (
    Outcome,
    compact_table,
    expire_snapshots,
    run_operation,
)


def test_a_compaction_is_traced_end_to_end(table, cfg, spans):
    outcome = compact_table(table, table.io, cfg(rewrite_chunk_bytes=4096))
    assert outcome.kind == "compacted", outcome

    recorded = spans()
    assert set(recorded) == {
        "iceberg.compact",
        "iceberg.compact.plan",
        "iceberg.compact.check_manifests",
        "iceberg.compact.rewrite",
        "iceberg.compact.commit",
    }

    compact = recorded["iceberg.compact"]
    assert compact.attributes["iceberg.table"] == "ns.small_files"
    assert compact.attributes["iceberg.outcome"] == "compacted"
    assert compact.attributes["iceberg.compaction.input.files"] == 12
    assert compact.attributes["iceberg.compaction.output.files"] >= 1

    # Chunk progress is an event, not a span — and with a 4 KiB cap there is
    # more than one, which is what makes a killed rewrite locatable.
    rewrite = recorded["iceberg.compact.rewrite"]
    chunks = [event for event in rewrite.events if event.name == "chunk written"]
    assert len(chunks) > 1
    assert all(
        event.attributes["iceberg.compaction.chunk.rows"] > 0 for event in chunks
    )


def test_a_skipped_table_is_traced_without_a_rewrite(table, cfg, spans):
    outcome = compact_table(table, table.io, cfg(min_input_files=99))

    assert outcome.kind == "healthy", outcome
    recorded = spans()
    assert recorded["iceberg.compact"].attributes["iceberg.outcome"] == "healthy"
    assert "iceberg.compact.rewrite" not in recorded


def test_committed_compaction_is_counted(table, cfg, counter):
    before = (
        counter("iceberg.maintenance.compaction.files.rewritten"),
        counter("iceberg.maintenance.compaction.files.written"),
        counter("iceberg.maintenance.compaction.bytes.rewritten"),
    )

    assert compact_table(table, table.io, cfg()).kind == "compacted"

    rewritten, written, moved_bytes = (
        counter("iceberg.maintenance.compaction.files.rewritten") - before[0],
        counter("iceberg.maintenance.compaction.files.written") - before[1],
        counter("iceberg.maintenance.compaction.bytes.rewritten") - before[2],
    )
    assert rewritten == 12
    assert 0 < written < rewritten
    assert moved_bytes > 0


def test_a_skipped_compaction_moves_no_byte_counters(table, cfg, counter):
    """The byte counters measure committed work, not effort spent deciding."""
    before = counter("iceberg.maintenance.compaction.bytes.rewritten")

    assert compact_table(table, table.io, cfg(min_input_files=99)).kind == "healthy"

    assert counter("iceberg.maintenance.compaction.bytes.rewritten") == before


def test_expiry_is_traced(table, cfg, spans):
    outcome = expire_snapshots(table, cfg())

    assert outcome.kind == "nothing", outcome
    expiry = spans()["iceberg.expire_snapshots"]
    assert expiry.attributes["iceberg.outcome"] == "nothing"
    assert expiry.attributes["iceberg.snapshots.total"] == 12
    assert expiry.attributes["iceberg.snapshots.expirable"] == 0


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (CommitFailedException("lost the race"), "conflict"),
        (RuntimeError("boom"), "failed"),
    ],
)
def test_a_failing_operation_becomes_a_counted_outcome(error, kind, counter):
    """The loop must survive any per-table failure, and still count it."""
    before = counter("iceberg.maintenance.operations")

    def work() -> Outcome:
        raise error

    outcome = run_operation("ns.t", "compact", "compaction", work)

    assert outcome.kind == kind
    assert counter("iceberg.maintenance.operations") == before + 1


def test_setup_is_a_no_op_without_a_collector(cfg):
    """No OTLP endpoint, no SDK, no exporter retrying into a closed port."""
    shutdown = telemetry.setup(cfg(otel_enabled=False))

    shutdown()  # always safe to call, whether or not anything was installed
