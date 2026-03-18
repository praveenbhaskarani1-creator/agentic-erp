"""
tests/test_config.py
────────────────────
Tests for app/config.py — Agentic Time Entry Validation

Run:
    pytest tests/test_config.py -v

What this tests:
  1. Settings loads without errors
  2. All required fields are present
  3. db_url is correctly constructed
  4. summary() never exposes password
  5. is_production / is_development flags work correctly
  6. Bedrock model IDs are correctly set
"""

import os
import pytest

# Inject test env vars BEFORE importing settings
os.environ.update({
    "APP_ENV":      "development",
    "DB_HOST":      "localhost",
    "DB_PORT":      "5433",
    "DB_NAME":      "agentdb",
    "DB_USER":      "pgadmin",
    "DB_PASSWORD":  "test_password_123",
    "AWS_REGION":   "us-east-1",
    "DEBUG":        "true",
})

# Import AFTER env vars are set
from app.config import Settings


# ─── Fixtures ────────────────────────────────────────────────

@pytest.fixture
def dev_settings():
    """Fresh Settings instance with dev values."""
    return Settings(
        APP_ENV="development",
        DB_HOST="localhost",
        DB_PORT=5433,
        DB_NAME="agentdb",
        DB_USER="pgadmin",
        DB_PASSWORD="test_password_123",
    )

@pytest.fixture
def prod_settings():
    """Fresh Settings instance with production values."""
    return Settings(
        APP_ENV="production",
        DB_HOST="agentic-erp-pgvector.cwb0sigyk8na.us-east-1.rds.amazonaws.com",
        DB_PORT=5432,
        DB_NAME="agentdb",
        DB_USER="pgadmin",
        DB_PASSWORD="prod_password",
    )


# ─── Tests: Basic Loading ─────────────────────────────────────

def test_settings_loads_without_error(dev_settings):
    """Settings must instantiate cleanly."""
    assert dev_settings is not None

def test_app_name_is_set(dev_settings):
    assert dev_settings.app_name == "Agentic Time Entry Validation"

def test_app_version_is_set(dev_settings):
    assert dev_settings.app_version == "1.0.0"


# ─── Tests: Database ─────────────────────────────────────────

def test_db_url_constructed_correctly(dev_settings):
    """db_url must be a valid SQLAlchemy PostgreSQL connection string."""
    url = dev_settings.db_url
    assert url.startswith("postgresql+psycopg2://")
    assert "pgadmin" in url
    assert "localhost" in url
    assert "5433" in url
    assert "agentdb" in url

def test_db_url_contains_password(dev_settings):
    """db_url must embed the password (used by SQLAlchemy connection pool)."""
    assert "test_password_123" in dev_settings.db_url

def test_db_port_is_integer(dev_settings):
    assert isinstance(dev_settings.db_port, int)
    assert dev_settings.db_port == 5433

def test_db_pool_size_default(dev_settings):
    assert dev_settings.db_pool_size == 5

def test_db_max_overflow_default(dev_settings):
    assert dev_settings.db_max_overflow == 10


# ─── Tests: AWS / Bedrock ─────────────────────────────────────

def test_aws_region_is_set(dev_settings):
    assert dev_settings.aws_region == "us-east-1"

def test_bedrock_sonnet_model_id(dev_settings):
    """Must match the exact Bedrock model ID you enabled."""
    assert dev_settings.bedrock_model_sonnet == "anthropic.claude-3-5-sonnet-20241022-v2:0"

def test_bedrock_haiku_model_id(dev_settings):
    assert dev_settings.bedrock_model_haiku == "anthropic.claude-3-5-haiku-20241022-v1:0"

def test_bedrock_embed_model_id(dev_settings):
    assert dev_settings.bedrock_embed_model == "amazon.titan-embed-text-v2:0"

def test_bedrock_temperature_is_zero(dev_settings):
    """Temperature must be 0.0 for deterministic SQL generation."""
    assert dev_settings.bedrock_temperature == 0.0

def test_bedrock_max_tokens(dev_settings):
    assert dev_settings.bedrock_max_tokens == 2048


# ─── Tests: S3 Buckets ───────────────────────────────────────

def test_s3_docs_bucket(dev_settings):
    assert dev_settings.s3_docs_bucket == "agentic-erp-docs-241030170015"

def test_s3_artifacts_bucket(dev_settings):
    assert dev_settings.s3_artifacts_bucket == "agentic-erp-artifacts-241030170015"


# ─── Tests: Environment Flags ────────────────────────────────

def test_is_development_flag(dev_settings):
    assert dev_settings.is_development is True
    assert dev_settings.is_production is False

def test_is_production_flag(prod_settings):
    assert prod_settings.is_production is True
    assert prod_settings.is_development is False


# ─── Tests: Security — summary() never leaks password ────────

def test_summary_hides_password(dev_settings):
    """CRITICAL: summary() must never expose DB password in logs."""
    summary = dev_settings.summary()
    assert summary["db_password"] == "***hidden***"
    assert "test_password_123" not in str(summary)

def test_summary_shows_safe_fields(dev_settings):
    summary = dev_settings.summary()
    assert summary["db_host"] == "localhost"
    assert summary["db_port"] == 5433
    assert summary["db_name"] == "agentdb"
    assert summary["aws_region"] == "us-east-1"
    assert summary["is_production"] is False

def test_summary_shows_bedrock_models(dev_settings):
    summary = dev_settings.summary()
    assert "claude" in summary["bedrock_model_sonnet"].lower()
    assert "titan" in summary["bedrock_embed_model"].lower()


# ─── Tests: Secrets Manager Secret Names ─────────────────────

def test_rds_secret_name(dev_settings):
    assert dev_settings.rds_secret_name == "agentic-erp/rds-pgvector"

def test_api_keys_secret_name(dev_settings):
    assert dev_settings.api_keys_secret_name == "agentic-erp/api-keys"

def test_oracle_secret_name(dev_settings):
    assert dev_settings.oracle_secret_name == "agentic-erp/oracle-fusion-dev"


# ─── Tests: load_rds_secret() skips in dev mode ──────────────

def test_load_rds_secret_noop_in_dev(dev_settings):
    """
    In development, load_rds_secret() must not change any values.
    (Secrets Manager is not called in dev mode.)
    """
    original_host = dev_settings.db_host
    original_port = dev_settings.db_port
    dev_settings.load_rds_secret()           # should be no-op
    assert dev_settings.db_host == original_host
    assert dev_settings.db_port == original_port
