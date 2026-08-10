"""The per-table loop: what it does to a whole warehouse, and how it fails.

One table's trouble must never end the run — that is the property the nightly
job is built around — and the exit code has to keep distinguishing a real
error from a commit lost to a concurrent dlt load.
"""

from opentelemetry.trace import StatusCode
from pyiceberg.exceptions import CommitFailedException

from iceberg_maintenance import maintenance
from iceberg_maintenance.maintenance import maintain_warehouse


def test_the_loop_maintains_every_table(table, cfg, warehouse, spans):
    tables, errors = maintain_warehouse(warehouse, table.io, cfg())

    assert (tables, errors) == (1, 0)
    assert len(list(warehouse.load_table("ns.small_files").scan().plan_files())) == 1

    recorded = spans()
    assert "iceberg.catalog.load_table" in recorded
    assert recorded["iceberg.maintenance.table"].attributes["iceberg.table"] == (
        "ns.small_files"
    )
    assert recorded["iceberg.maintenance.table"].status.status_code != StatusCode.ERROR


def test_a_raising_table_is_an_error_but_not_the_end_of_the_run(
    table, cfg, warehouse, spans, monkeypatch
):
    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(maintenance, "compact_table", boom)

    tables, errors = maintain_warehouse(warehouse, table.io, cfg())

    assert (tables, errors) == (1, 1)
    recorded = spans()
    assert recorded["iceberg.maintenance.table"].status.status_code is StatusCode.ERROR
    # Expiry runs anyway — it is metadata-only, and a table whose compaction
    # is broken is exactly the one accumulating snapshots.
    assert "iceberg.expire_snapshots" in recorded


def test_a_lost_commit_race_is_not_an_error(table, cfg, warehouse, monkeypatch):
    """Optimistic concurrency doing its job must not fail the nightly run."""

    def contended(*args, **kwargs):
        raise CommitFailedException("a concurrent dlt load got there first")

    monkeypatch.setattr(maintenance, "compact_table", contended)

    assert maintain_warehouse(warehouse, table.io, cfg()) == (1, 0)
