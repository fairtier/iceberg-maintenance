# iceberg-maintenance

Nightly Iceberg table maintenance (small-file compaction + snapshot expiry) for
a FairTier box warehouse, run as a Kubernetes CronJob.

Published as `github.com/fairtier/iceberg-maintenance`. Docker images at
`ghcr.io/fairtier/iceberg-maintenance`.

This repo is the **baked-image** form of the maintenance job. It replaces a
per-run `pip install pyiceberg[pyarrow]` that used to happen inside the box
Helm chart at job start — that install was the biggest CPU/IO spike of the
nightly run on a 2‑vCPU box. The chart that schedules the image lives in the
FairTier monorepo at `apps/box/iceberg-maintenance/`, and the design +
history is in `docs/plans/iceberg-maintenance.md` there. Box images MUST be
**public** on `ghcr.io/fairtier` and built in a separate GitHub repo like this
one — boxes have no GitLab pull secret.

## Commands

```bash
uv run pytest -v                       # tests
uv run ruff check .                    # lint
uv run ruff format --check .           # format check
uv run ty check                        # type check
uv run python -m iceberg_maintenance   # run locally (needs env vars — see README.md)
```

All code must pass `ruff check`, `ruff format --check`, and `ty check` with
zero errors before committing.

## Package management

[uv](https://github.com/astral-sh/uv); the lockfile (`uv.lock`) is committed.
`pyiceberg` is pinned **exactly** to `0.11.1` — do not loosen it. The
compaction rewrite depends on a PyIceberg-internal helper
(`_dataframe_to_data_files`), which is only safe because the version is frozen.
See the `AWAITING-UPSTREAM` note in `maintenance.py` for the release that lets
the hand-rolled streaming block collapse to a one-liner.

## Project structure

```
src/iceberg_maintenance/
  __init__.py       # version
  __main__.py       # entry point (python -m iceberg_maintenance)
  config.py         # environment-variable configuration (Config dataclass)
  maintenance.py    # compaction + snapshot expiry logic and the main loop
tests/              # pytest suite
```

## CI/CD

- **CI**: GitHub Actions runs `ruff check`, `ruff format`, `ty check`, and
  `pytest` on push to master and PRs.
- **Release**: a `v*` tag triggers a multi-arch (amd64 + arm64) Docker build +
  push to GHCR. After release, bump the image tag in the box chart's
  `values.yaml` via GitOps.

## Correctness invariants (do not regress)

- **Streaming, not buffering.** Compaction rewrites a table by streaming the
  scan through `stream_batches` in `REWRITE_CHUNK_BYTES` chunks — peak memory
  is O(1) in table size. Never reintroduce a whole-table `scan().to_arrow()` or
  a "skip big tables" size cap; both were removed for causing box-wide stalls.
  **And never go back to `scan.to_arrow_batch_reader()`** — it looks like the
  streaming API but `ArrowScan.to_record_batches` `executor.map`s every data
  file up front and keeps each finished file's batches until the consumer gets
  there, so a slow consumer (us: parquet + upload) buffers the whole table.
  That OOM-killed the 1Gi nightly job on 2026-07-31; `stream_batches` and
  `tests/test_compaction.py::test_stream_batches_reads_one_file_at_a_time`
  exist to keep it out. "OOM is a bug, not a small box."
- **Atomic swap.** The old-files delete and new-files append commit in a single
  transaction; a crash before commit leaves the table completely untouched
  (only orphan parquet in storage, which the orphan-sweep follow-up handles).
- **Safe skips.** Tables with delete files (merge-on-read) are skipped for
  compaction but still get (metadata-only) snapshot expiry.
- **Direct S3 IO.** `table.io` is replaced with a direct-credential
  `PyArrowFileIO` after `load_table()` to sidestep Lakekeeper's forced
  `S3V4RestSigner` (a known PyIceberg async-s3fs bug).
