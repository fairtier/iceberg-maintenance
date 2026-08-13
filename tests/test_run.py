"""The per-table loop: what it does to a whole warehouse, and how it fails.

One table's trouble must never end the run — that is the property the nightly
job is built around — and the exit code has to keep distinguishing a real
error from a commit lost to a concurrent dlt load.

It also has to distinguish a table it *cannot* compact from one it compacted.
Until 2026-08-13 it did not: a dbt staging table on a production box had been
silently un-compactable for weeks while every run reported "12 tables scanned,
0 errors" and stamped a success heartbeat.
"""

from opentelemetry.trace import StatusCode
from pyiceberg.exceptions import CommitFailedException

from iceberg_maintenance import maintenance
from iceberg_maintenance.maintenance import Outcome, RunSummary, maintain_warehouse


def test_the_loop_maintains_every_table(table, cfg, warehouse, spans):
    summary = maintain_warehouse(warehouse, table.io, cfg())

    assert (summary.tables, summary.errors, summary.unsupported) == (1, 0, 0)
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

    summary = maintain_warehouse(warehouse, table.io, cfg())

    assert (summary.tables, summary.errors) == (1, 1)
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

    summary = maintain_warehouse(warehouse, table.io, cfg())
    assert (summary.tables, summary.errors, summary.unsupported) == (1, 0, 0)


def test_an_uncompactable_table_is_counted_and_named(
    table, cfg, warehouse, monkeypatch
):
    """The regression this whole tally exists for.

    A table PyIceberg cannot rewrite is not an error — the run should still
    exit 0 and expiry should still run on it — but it must not disappear into
    "0 errors" either.
    """

    def undecodable(*args, **kwargs):
        return Outcome(
            "unsupported",
            "SKIPPED: PyIceberg cannot decode ...-m0.avro — written by another engine",
        )

    monkeypatch.setattr(maintenance, "compact_table", undecodable)

    summary = maintain_warehouse(warehouse, table.io, cfg())

    assert summary.errors == 0
    assert summary.unsupported == 1
    assert summary.unsupported_tables == ["ns.small_files"]


def test_a_declined_rewrite_is_not_counted_as_uncompactable(
    table, cfg, warehouse, monkeypatch
):
    """`skipped` and `unsupported` are different, and only one is a problem.

    The write-amplification gate declines most healthy tables most nights. If
    that counted, the metric would be permanently non-zero on every box and
    the alert built on it would be noise.
    """

    def declined(*args, **kwargs):
        return Outcome("skipped", "skipped: not worth a whole-table rewrite")

    monkeypatch.setattr(maintenance, "compact_table", declined)

    summary = maintain_warehouse(warehouse, table.io, cfg())

    assert (summary.errors, summary.unsupported) == (0, 0)


def test_the_textfile_is_written_atomically_and_readably(tmp_path):
    path = tmp_path / "sub" / "iceberg_maintenance.prom"

    maintenance.write_textfile(
        str(path), RunSummary(tables=12, unsupported_tables=["staging.stg_trips"])
    )

    body = path.read_text()
    assert "iceberg_maintenance_tables_scanned 12" in body
    assert "iceberg_maintenance_tables_unsupported 1" in body
    assert "iceberg_maintenance_last_success_timestamp_seconds " in body
    # A half-written file is a parse error for the node exporter's whole
    # scrape, not just this metric — hence the rename, and hence no leftovers.
    assert [p.name for p in path.parent.iterdir()] == [path.name]
    assert path.stat().st_mode & 0o777 == 0o644


def test_a_failed_run_leaves_the_previous_heartbeat_standing(
    table, cfg, warehouse, monkeypatch, tmp_path
):
    """`last_success` must mean last success, or the staleness alert is a lie."""

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(maintenance, "compact_table", boom)
    monkeypatch.setattr(maintenance, "load_catalog", lambda *a, **k: warehouse)
    monkeypatch.setattr(maintenance, "direct_s3_io", lambda c: table.io)
    monkeypatch.setattr(
        maintenance, "load_config", lambda: cfg(textfile_path=str(tmp_path / "m.prom"))
    )

    assert maintenance.main() == 1
    assert not (tmp_path / "m.prom").exists()
