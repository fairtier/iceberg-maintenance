"""Environment-variable configuration for the maintenance job.

Every knob is injected by the box Helm chart (apps/box/iceberg-maintenance in
the FairTier monorepo). Defaults here mirror that chart's values.yaml so the
job also runs standalone (`python -m iceberg_maintenance`) with only the
required connection variables set.
"""

import os
from dataclasses import dataclass

MIB = 1024 * 1024


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def _one_of(name: str, default: str, allowed: tuple[str, ...]) -> str:
    """Read an enum-valued variable, failing fast on a typo.

    A misspelled mode must not silently pick a default — for ORPHAN_SWEEP_MODE
    that would be the difference between deleting and not deleting.
    """
    value = os.environ.get(name, default).strip()
    if value not in allowed:
        raise SystemExit(f"{name}={value!r} is not one of {', '.join(allowed)}")
    return value


def _otel_enabled() -> bool:
    """Telemetry is on when — and only when — an OTLP endpoint is configured.

    No bespoke toggle: the presence of a collector endpoint *is* the intent,
    and the standard kill switch (`OTEL_SDK_DISABLED=true`) still wins. Absent
    an endpoint the SDK would default to localhost:4318 and spend the end of
    every run retrying an export into a closed port.
    """
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return False
    return any(
        os.environ.get(name)
        for name in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        )
    )


@dataclass(frozen=True)
class Config:
    # --- Catalog (Lakekeeper REST) + OAuth2 (Casdoor) ---
    catalog_uri: str
    warehouse: str
    oidc_client_id: str
    oidc_client_secret: str
    oidc_token_url: str

    # --- Direct data-file IO (see maintenance.direct_s3_io) ---
    aws_endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str

    # --- Compaction knobs ---
    small_file_max_bytes: int
    min_input_files: int
    rewrite_min_small_fraction: float
    rewrite_chunk_bytes: int
    commit_max_attempts: int
    commit_retry_backoff_seconds: float

    # --- Snapshot-expiry knobs (the customer-visible time-travel window) ---
    max_snapshot_age_ms: int
    min_snapshots_to_keep: int

    # --- Orphan-file sweep (see orphans.py) ---
    # The one part of this job that DELETES files, so it is off-by-default in
    # spirit: "dry-run" measures and reports, only "delete" acts.
    orphan_sweep_mode: str
    orphan_min_age_seconds: int
    orphan_max_deletes: int

    # --- Observability (see telemetry.py) ---
    # Everything else OpenTelemetry needs (endpoint, headers, service name,
    # timeouts) is read straight from the standard OTEL_* variables by the SDK;
    # this is only the on/off decision. Defaults off so the job runs unchanged
    # on a box with no collector.
    otel_enabled: bool = False

    # --- Observability that survives the run (see maintenance.write_textfile) ---
    # A node_exporter textfile. The OTLP metrics above are the richer signal but
    # they need a collector on the far end; this is one file on a shared
    # hostPath, scraped by whatever node exporter the box already runs, and it
    # is what the fleet alerts read. Empty (the default) writes nothing.
    textfile_path: str = ""

    @property
    def credential(self) -> str:
        """PyIceberg REST `credential` string (`client_id:client_secret`)."""
        return f"{self.oidc_client_id}:{self.oidc_client_secret}"


def load_config() -> Config:
    return Config(
        catalog_uri=_require("CATALOG_URI"),
        warehouse=_require("WAREHOUSE"),
        oidc_client_id=_require("OIDC_CLIENT_ID"),
        oidc_client_secret=_require("OIDC_CLIENT_SECRET"),
        oidc_token_url=_require("OIDC_TOKEN_URL"),
        aws_endpoint_url=_require("AWS_ENDPOINT_URL"),
        aws_access_key_id=_require("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require("AWS_SECRET_ACCESS_KEY"),
        aws_region=os.environ.get("AWS_REGION", "auto"),
        # 8 MiB, not 32: this threshold judges *on-disk parquet* bytes while
        # REWRITE_CHUNK_BYTES below bounds *in-memory Arrow* bytes, and at the
        # ~10:1 compression of a real table a 128 MiB Arrow chunk lands as a
        # ~13 MiB parquet file. At 32 MiB every file the rewrite produced was
        # born "small", so the gate re-fired the next night, forever: a
        # production box rewrote 107 files / 1.44 GiB into 107 files /
        # 1.44 GiB nightly until 2026-08-27, orphaning the whole 1.44 GiB in
        # object storage each time, and the fourth such night livelocked the
        # box. Keep it below the achievable output size until the rewrite
        # closes files on an on-disk size rather than an in-memory one.
        small_file_max_bytes=int(os.environ.get("SMALL_FILE_MAX_BYTES", str(8 * MIB))),
        min_input_files=int(os.environ.get("MIN_INPUT_FILES", "8")),
        rewrite_min_small_fraction=float(
            os.environ.get("REWRITE_MIN_SMALL_FRACTION", "0.3")
        ),
        rewrite_chunk_bytes=int(os.environ.get("REWRITE_CHUNK_BYTES", str(128 * MIB))),
        # How many times to offer the finished rewrite to a catalog that is
        # merely unavailable, and the first backoff (doubling: 15s, 30s, 60s,
        # 120s — ~3.75 min of patience across the default 5 attempts).
        #
        # Bounded, and deliberately modest: this buys back a Lakekeeper *pod
        # restart*, which is what the failure usually is. It cannot buy back a
        # 45-minute crashloop, and it should not try — concurrencyPolicy is
        # Forbid, so a run that waits forever eats the following nights too.
        # The asymmetry is what justifies any wait at all: the rewrite is 20
        # minutes of streaming and the commit is one HTTP call, so throwing the
        # first away because the second was refused for 15 seconds is the worst
        # possible trade.
        commit_max_attempts=int(os.environ.get("COMMIT_MAX_ATTEMPTS", "5")),
        commit_retry_backoff_seconds=float(
            os.environ.get("COMMIT_RETRY_BACKOFF_SECONDS", "15")
        ),
        max_snapshot_age_ms=int(
            os.environ.get("MAX_SNAPSHOT_AGE_MS", str(7 * 24 * 3600 * 1000))
        ),
        min_snapshots_to_keep=int(os.environ.get("MIN_SNAPSHOTS_TO_KEEP", "5")),
        # Ships as "dry-run", not "delete": this is the only code here that
        # removes customer data, and the number it reports is worth having on
        # its own (until it existed, nothing anywhere said how much orphaned
        # parquet a warehouse was paying for). Arm it per box once a run's
        # report has been read — see docs/plans/iceberg-maintenance.md.
        orphan_sweep_mode=_one_of(
            "ORPHAN_SWEEP_MODE", "dry-run", ("off", "dry-run", "delete")
        ),
        # A file younger than this is never deleted, however unreachable it
        # looks. Two reasons, and both are load-bearing: a concurrent writer's
        # in-flight upload is not referenced yet by construction, and the
        # reference set is read before the listing, so anything written between
        # the two would otherwise look like garbage. 7 days also matches the
        # time-travel window, which makes "how far back can a mistake reach"
        # one number instead of two.
        orphan_min_age_seconds=int(
            os.environ.get("ORPHAN_MIN_AGE_SECONDS", str(7 * 24 * 3600))
        ),
        # Blast radius, per table per run. A bug that mistook the live file set
        # for orphans costs at most this many files and gets a night to be
        # noticed in; the cap truncates (loudly) rather than aborting, so a
        # genuinely large backlog still drains over successive runs.
        orphan_max_deletes=int(os.environ.get("ORPHAN_MAX_DELETES", "1000")),
        otel_enabled=_otel_enabled(),
        textfile_path=os.environ.get("TEXTFILE_PATH", ""),
    )
