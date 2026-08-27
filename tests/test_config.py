import pytest

from iceberg_maintenance.config import MIB, load_config

_REQUIRED = {
    "CATALOG_URI": "http://lakekeeper:8181/catalog",
    "WAREHOUSE": "default",
    "OIDC_CLIENT_ID": "client",
    "OIDC_CLIENT_SECRET": "secret",
    "OIDC_TOKEN_URL": "https://auth.example/token",
    "AWS_ENDPOINT_URL": "https://s3.example",
    "AWS_ACCESS_KEY_ID": "ak",
    "AWS_SECRET_ACCESS_KEY": "sk",
}


def _set(env, **overrides):
    for k, v in {**_REQUIRED, **overrides}.items():
        env.setenv(k, v)


def test_defaults_match_chart(monkeypatch):
    """apps/box/iceberg-maintenance/values.yaml in the FairTier monorepo.

    Kept in lockstep so the image is not shipped with a default the fleet
    overrides — a self-hoster running it bare gets what a box gets.
    """
    _set(monkeypatch)
    cfg = load_config()
    # 8 MiB, not 32 — see the note at the default in config.py.
    assert cfg.small_file_max_bytes == 8 * MIB
    assert cfg.min_input_files == 8
    assert cfg.rewrite_min_small_fraction == 0.3
    assert cfg.rewrite_chunk_bytes == 128 * MIB
    assert cfg.commit_max_attempts == 5
    assert cfg.commit_retry_backoff_seconds == 15
    assert cfg.max_snapshot_age_ms == 7 * 24 * 3600 * 1000
    assert cfg.min_snapshots_to_keep == 5
    # AWS_REGION is the one optional connection var, defaulting to "auto".
    assert cfg.aws_region == "auto"


def test_credential_property(monkeypatch):
    _set(monkeypatch)
    assert load_config().credential == "client:secret"


def test_overrides_are_read(monkeypatch):
    _set(
        monkeypatch,
        MIN_INPUT_FILES="16",
        REWRITE_MIN_SMALL_FRACTION="0.5",
        AWS_REGION="fsn1",
    )
    cfg = load_config()
    assert cfg.min_input_files == 16
    assert cfg.rewrite_min_small_fraction == 0.5
    assert cfg.aws_region == "fsn1"


def test_missing_required_exits(monkeypatch):
    # Only set part of the required set — load_config must fail fast and name it.
    monkeypatch.setenv("CATALOG_URI", "http://lakekeeper:8181/catalog")
    with pytest.raises(SystemExit, match="WAREHOUSE"):
        load_config()
