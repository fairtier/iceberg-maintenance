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

    # --- Snapshot-expiry knobs (the customer-visible time-travel window) ---
    max_snapshot_age_ms: int
    min_snapshots_to_keep: int

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
        small_file_max_bytes=int(os.environ.get("SMALL_FILE_MAX_BYTES", str(32 * MIB))),
        min_input_files=int(os.environ.get("MIN_INPUT_FILES", "8")),
        rewrite_min_small_fraction=float(
            os.environ.get("REWRITE_MIN_SMALL_FRACTION", "0.3")
        ),
        rewrite_chunk_bytes=int(os.environ.get("REWRITE_CHUNK_BYTES", str(128 * MIB))),
        max_snapshot_age_ms=int(
            os.environ.get("MAX_SNAPSHOT_AGE_MS", str(7 * 24 * 3600 * 1000))
        ),
        min_snapshots_to_keep=int(os.environ.get("MIN_SNAPSHOTS_TO_KEEP", "5")),
    )
