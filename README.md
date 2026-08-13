# iceberg-maintenance

[![CI](https://github.com/fairtier/iceberg-maintenance/actions/workflows/ci.yml/badge.svg)](https://github.com/fairtier/iceberg-maintenance/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/fairtier/iceberg-maintenance)](LICENSE)

Nightly [Apache Iceberg](https://iceberg.apache.org/) table maintenance for a
[FairTier](https://fairtier.com) box warehouse: **small-file compaction** plus
**snapshot expiry**, run as a Kubernetes CronJob against the on-box
[Lakekeeper](https://lakekeeper.io/) catalog.

This is the baked-image form of what used to be a `pip install pyiceberg` at
job start inside the box chart. The per-run install was the single biggest
CPU/IO spike of the nightly job on a small (2‑vCPU) box; shipping a prebuilt
`ghcr.io/fairtier/iceberg-maintenance` image removes it. The Helm chart that
schedules it lives in the FairTier monorepo at
`apps/box/iceberg-maintenance/`, and the full design is in
`docs/plans/iceberg-maintenance.md` there.

## What it does

For every table in the warehouse:

1. **Compaction** — when a table has accumulated at least `MIN_INPUT_FILES`
   small (`< SMALL_FILE_MAX_BYTES`) data files *and* those small files are a
   meaningful share of the table (`≥ REWRITE_MIN_SMALL_FRACTION` by bytes), the
   table is rewritten. The rewrite **streams** the scan (`stream_batches`, one
   data file open at a time) through a chunked read → write
   (`REWRITE_CHUNK_BYTES` per flush), so peak memory is **O(1) in table size** —
   there is no size cap, and the job cannot OOM on a large table. The swap (drop
   every old data file, add the new ones) is a single atomic Iceberg
   transaction.

   > PyIceberg's own `scan.to_arrow_batch_reader()` is *not* lazy despite the
   > name: it `executor.map`s every data file up front and holds each finished
   > file's batches until the consumer catches up, i.e. O(table size). Using it
   > OOM-killed the nightly job on a 1Gi box (0.1.0); `stream_batches` drives
   > the sequential path instead, and a test pins the invariant.
2. **Snapshot expiry** — snapshots older than `MAX_SNAPSHOT_AGE_MS` (the
   customer-visible time-travel window) are expired, keeping the newest
   `MIN_SNAPSHOTS_TO_KEEP` regardless of age and never touching branch heads or
   tags. This is metadata-only.

Compaction and expiry are each per-table and independent; a failure or skip on
one table never blocks the others, and a commit lost to a concurrent dlt load
is logged and retried next run (Iceberg optimistic concurrency — never
corrupted data).

### Deliberately not handled

- **Orphan-file removal** — no OSS option exists (Lakekeeper's queue is
  Enterprise-only; PyIceberg's implementation never merged). Snapshot expiry
  above trims *metadata* only; the unreferenced data files stay in object
  storage until an orphan sweep exists. This is a tracked open gap.
- **Tables with delete files** (DuckFlight/DuckDB merge-on-read writes) — a
  copy-on-write rewrite isn't safe for them, so compaction skips them loudly.
  Snapshot expiry still runs (it never touches data files).
- **Tables whose manifests PyIceberg can't decode** — on tables written by
  DuckDB's Iceberg writer, PyIceberg reads the manifest *entry header* wrong:
  `status` comes back holding the snapshot id and `sequence_number` is `None`
  (the `data_file` half decodes fine, so reads and scans are unaffected).
  Rewriting entries we can't read faithfully would be reckless, and the commit
  rejects them anyway (`Only entries with status ADDED can have null sequence
  number`), so compaction skips those tables — checked **before** any parquet
  is written, since discovering it at commit time means a whole table rewritten
  into orphan files for nothing. Snapshot expiry still runs.

  This is not a rare edge: **every table a dbt model materializes through
  DuckDB lands this way**, so a warehouse's un-compactable share grows with its
  transformation layer, not with anything the operator did. It stayed invisible
  until 2026-08-13 because expiry still ran and the run reported "0 errors" —
  which is why both cases above are now **counted** and published as
  `iceberg_maintenance_tables_unsupported`, distinct from both a success and a
  failure. Neither is a run error, and neither is fine.

## Usage

```bash
docker run --rm \
  -e CATALOG_URI=http://lakekeeper:8181/catalog \
  -e WAREHOUSE=default \
  -e OIDC_CLIENT_ID=... \
  -e OIDC_CLIENT_SECRET=... \
  -e OIDC_TOKEN_URL=https://auth.example/api/login/oauth/access_token \
  -e AWS_ENDPOINT_URL=https://s3.example \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e AWS_REGION=auto \
  ghcr.io/fairtier/iceberg-maintenance:latest
```

Exit code is `0` when every table was processed cleanly, `1` if any table hit a
non-transient error (compaction/expiry commit conflicts are transient and do
not fail the run).

## Configuration

All configuration is via environment variables (injected by the box Helm
chart). Defaults mirror that chart's `values.yaml`.

### Required

| Variable              | Description                                                            |
|-----------------------|------------------------------------------------------------------------|
| `CATALOG_URI`         | Lakekeeper Iceberg REST catalog URL (e.g. `http://lakekeeper:8181/catalog`) |
| `WAREHOUSE`           | Lakekeeper warehouse to maintain                                       |
| `OIDC_CLIENT_ID`      | OAuth2 client id — a Lakekeeper warehouse **writer** principal         |
| `OIDC_CLIENT_SECRET`  | OAuth2 client secret                                                   |
| `OIDC_TOKEN_URL`      | OAuth2 token endpoint (Casdoor)                                        |
| `AWS_ENDPOINT_URL`    | S3-compatible endpoint for **direct** data-file IO (bypasses the catalog's forced signer — see below) |
| `AWS_ACCESS_KEY_ID`   | Storage access key                                                     |
| `AWS_SECRET_ACCESS_KEY` | Storage secret key                                                   |

### Optional

| Variable                     | Default        | Description                                                                 |
|------------------------------|----------------|-----------------------------------------------------------------------------|
| `AWS_REGION`                 | `auto`         | S3 region (`auto` for Cloudflare R2)                                        |
| `SMALL_FILE_MAX_BYTES`       | `33554432`     | A data file below this counts as "small"                                   |
| `MIN_INPUT_FILES`            | `8`            | Compact only when a table has at least this many small files               |
| `REWRITE_MIN_SMALL_FRACTION` | `0.3`          | Write-amplification gate: rewrite only when small files are ≥ this fraction of the table by bytes |
| `REWRITE_CHUNK_BYTES`        | `134217728`    | Streaming chunk size — the **only** thing that sets peak rewrite memory (constant in table size), and the approximate output data-file size |
| `MAX_SNAPSHOT_AGE_MS`        | `604800000`    | Time-travel window; snapshots older than this are expired (7 days)         |
| `MIN_SNAPSHOTS_TO_KEEP`      | `5`            | Retention floor: always keep the newest N snapshots regardless of age      |
| `TEXTFILE_PATH`              | *(unset)*      | Write the run's numbers here as a node_exporter textfile — see below. Unset writes nothing |

### Observability without a collector (node_exporter textfile)

OTLP below is the richer signal, but it needs a collector on the far end. Set
`TEXTFILE_PATH=/textfile/iceberg_maintenance.prom` and a **clean** run also
writes three gauges to a file the box's existing node exporter already scrapes:

| Metric | Meaning |
|---|---|
| `iceberg_maintenance_last_success_timestamp_seconds` | When the last error-free run finished |
| `iceberg_maintenance_tables_scanned` | Tables it visited |
| `iceberg_maintenance_tables_unsupported` | Tables it **could not compact at all** |

Three things about it are deliberate:

- **A run with any error writes nothing.** `last_success` has to mean last
  success or a staleness alert built on it is a lie, so a failed run leaves
  the previous value standing and lets it go stale.
- **It is written to a temp file and renamed.** The exporter can scrape
  mid-write, and a half-written file fails the parse for the *whole* scrape,
  not just this metric.
- **A failure to write it does not fail the run.** A warehouse that was
  maintained correctly must not be reported as broken because a hostPath was
  not mounted; the staleness alert covers a heartbeat that stops arriving.

`tables_unsupported` is the one worth wiring an alert to. It counts tables
compaction can never run on as things stand — delete files, or manifests
PyIceberg cannot decode — as distinct from the `skipped` outcome, which is the
write-amplification gate declining a healthy table and is normal every night.
Snapshot expiry still runs on unsupported tables, which is exactly why they
look fine from the outside: one production box carried an un-compactable dbt
staging table for weeks while every run reported "12 tables scanned, 0 errors"
and stamped a success heartbeat.

### Observability (OpenTelemetry)

Off unless a collector is configured: set `OTEL_EXPORTER_OTLP_ENDPOINT` (OTLP
over **HTTP**, so `http://collector:4318`) and the run exports traces and
metrics; leave it unset and every instrumentation call goes through the
OpenTelemetry API's no-op path, costing nothing. `OTEL_SDK_DISABLED=true` and
the rest of the standard `OTEL_*` variables (`OTEL_SERVICE_NAME`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_RESOURCE_ATTRIBUTES`, …) work as usual;
`service.name` defaults to `iceberg-maintenance` and the warehouse rides along
as the resource attribute `iceberg.warehouse`.

A run is one trace:

```
iceberg.maintenance.run
└── iceberg.maintenance.table                  iceberg.table, iceberg.namespace
    ├── iceberg.catalog.load_table
    ├── iceberg.compact                        iceberg.outcome + the input/small
    │   ├── iceberg.compact.plan                 file counts every gate decided on
    │   ├── iceberg.compact.check_manifests
    │   ├── iceberg.compact.rewrite            one "chunk written" event per flush
    │   └── iceberg.compact.commit               (rows, bytes, files so far)
    └── iceberg.expire_snapshots               iceberg.outcome, snapshot counts
```

Catalog HTTP calls (Lakekeeper, the OAuth token endpoint) are traced too, via
`opentelemetry-instrumentation-requests` — without them the trace is our spans
separated by unexplained gaps. Data-file IO goes through PyArrow's C++ S3
client and stays invisible.

| Metric                                          | Type      | Attributes                      |
|-------------------------------------------------|-----------|---------------------------------|
| `iceberg.maintenance.run.duration`              | histogram | `iceberg.outcome` (`ok`/`errors`/`failed`) |
| `iceberg.maintenance.tables.scanned`            | counter   | —                               |
| `iceberg.maintenance.operations`                | counter   | `iceberg.operation`, `iceberg.outcome` |
| `iceberg.maintenance.operation.duration`        | histogram | `iceberg.operation`, `iceberg.outcome` |
| `iceberg.maintenance.compaction.files.rewritten`| counter   | —                               |
| `iceberg.maintenance.compaction.files.written`  | counter   | —                               |
| `iceberg.maintenance.compaction.bytes.rewritten`| counter   | —                               |
| `iceberg.maintenance.snapshots.expired`         | counter   | —                               |

`iceberg.outcome` is the headline label — `compacted`, `healthy`, `skipped`,
`unsupported`, `empty`, `expired`, `nothing`, `conflict`, `failed` (see
`Outcome` in `maintenance.py`). Alert on `failed`, and on a standing
`unsupported`; a rising `conflict` means compaction keeps losing the commit
race to concurrent dlt loads. Table names are deliberately **spans-only**,
never metric attributes — the counters answer "did tonight go well", the trace
answers "which table". (The run's closing log line is the exception, and names
the un-compactable tables outright: a count of 1 is only actionable if you
know which one.)

Two things this job does because it is a CronJob and not a server: it
force-flushes both pipelines at exit (a batch process dies long before batched
spans or periodic metrics would export on their own), and it defaults
`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` to `delta`, since a fresh
process every night would otherwise make cumulative counters read as a nightly
reset. Setting that variable explicitly still wins.

### Why direct S3 credentials?

Lakekeeper force-overrides clients to `FsspecFileIO` + `S3V4RestSigner`, and
PyIceberg's `S3V4RestSigner` event handler never fires under async s3fs (a
known PyIceberg bug). So after `load_table()` the job replaces `table.io` with
a direct-credential `PyArrowFileIO` built from the `AWS_*` variables — the same
workaround dlt-worker uses via AWS env vars.

## Pinned PyIceberg

`pyiceberg` is pinned **exactly** to `0.11.1` (not a floor). The compaction
rewrite drives two PyIceberg-internal helpers — `_dataframe_to_data_files` to
write a chunk (the released 0.11.1 public API still rejects a reader:
`ValueError("Expected PyArrow table")`) and `ArrowScan`'s sequential
`_record_batches_from_scan_tasks_and_deletes` to *read* one file at a time.
Depending on internals is safe *only* while the version is frozen. When a
PyIceberg release ships `Table.overwrite`/`append` accepting a
`RecordBatchReader`, the hand-rolled write block collapses to
`table.overwrite(stream_batches(scan, tasks))` — note: still **not**
`scan.to_arrow_batch_reader()`, which buffers whole files behind an executor.
See the `AWAITING-UPSTREAM` note in
[`maintenance.py`](src/iceberg_maintenance/maintenance.py).

## Development

```bash
uv sync                        # install deps + dev tools
uv run ruff check .            # lint
uv run ruff format --check .   # format check
uv run ty check                # type check
uv run pytest -v               # tests
uv run python -m iceberg_maintenance   # run (needs the env vars above)
```

## Releasing

Images are published to `ghcr.io/fairtier/iceberg-maintenance` by the
[release workflow](.github/workflows/release.yml) on any `v*` tag
(multi-arch: linux/amd64 + linux/arm64):

```bash
git tag v0.3.0 && git push origin v0.3.0
```

Then bump the image tag in the box chart
(`apps/box/iceberg-maintenance/values.yaml`) via GitOps.
