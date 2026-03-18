"""
app/config.py
─────────────
Central configuration for the Agentic Time Entry Validation system.

Priority order for values:
  1. AWS Secrets Manager  (production on ECS)
  2. Environment variables / .env file  (local dev)
  3. Defaults defined below

Usage:
    from app.config import settings
    conn_str = settings.db_url
    model    = settings.bedrock_model_sonnet
"""

import json
import logging
import os
from functools import lru_cache
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load .env for local development (no-op in production)
load_dotenv()

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Secrets Manager helper
# ─────────────────────────────────────────────────────────────

def _fetch_secret(secret_name: str, region: str = "us-east-1") -> dict:
    """
    Pull a JSON secret from AWS Secrets Manager.
    Returns empty dict if unavailable (local dev fallback).

    Example secret stored in SM:
        agentic-erp/rds-pgvector →
            {"host": "...", "port": 5432, "dbname": "agentdb",
             "username": "pgadmin", "password": "..."}
    """
    try:
        client = boto3.client("secretsmanager", region_name=region)
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        code = e.response["Error"]["Code"]
        logger.warning(f"[config] Secrets Manager '{secret_name}' → {code}. Using env vars.")
        return {}
    except Exception as e:
        logger.warning(f"[config] Secrets Manager unavailable → using .env: {e}")
        return {}


# ─────────────────────────────────────────────────────────────
# Settings class
# ─────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    """
    All application settings.
    In production (ECS), env vars are injected via task definition.
    In development, values come from .env file.
    """

    # ── App ───────────────────────────────────────────────────
    app_name: str       = "Agentic Time Entry Validation"
    app_version: str    = "1.0.0"
    app_env: str        = Field(default="development", alias="APP_ENV")
    debug: bool         = Field(default=False, alias="DEBUG")
    log_level: str      = Field(default="INFO", alias="LOG_LEVEL")

    # ── Database ──────────────────────────────────────────────
    # Local dev:   localhost:5433  (via SSH tunnel)
    # Production:  RDS endpoint    (private subnet, no tunnel)
    db_host: str        = Field(default="localhost",  alias="DB_HOST")
    db_port: int        = Field(default=5433,         alias="DB_PORT")
    db_name: str        = Field(default="agentdb",    alias="DB_NAME")
    db_user: str        = Field(default="pgadmin",    alias="DB_USER")
    db_password: str    = Field(default="",           alias="DB_PASSWORD")
    db_pool_size: int   = Field(default=5,            alias="DB_POOL_SIZE")
    db_max_overflow: int= Field(default=10,           alias="DB_MAX_OVERFLOW")

    # ── AWS ───────────────────────────────────────────────────
    aws_region: str         = Field(default="us-east-1",     alias="AWS_REGION")
    aws_account_id: str     = Field(default="241030170015",  alias="AWS_ACCOUNT_ID")

    # ── Bedrock Model IDs ─────────────────────────────────────
    bedrock_model_sonnet: str = Field(
        default="anthropic.claude-3-5-sonnet-20241022-v2:0",
        alias="BEDROCK_MODEL_SONNET"
    )
    bedrock_model_haiku: str = Field(
        default="anthropic.claude-3-5-haiku-20241022-v1:0",
        alias="BEDROCK_MODEL_HAIKU"
    )
    bedrock_embed_model: str = Field(
        default="amazon.titan-embed-text-v2:0",
        alias="BEDROCK_EMBED_MODEL"
    )
    bedrock_max_tokens: int = Field(default=2048,  alias="BEDROCK_MAX_TOKENS")
    bedrock_temperature: float = Field(default=0.0, alias="BEDROCK_TEMPERATURE")

    # ── Secrets Manager Names ─────────────────────────────────
    rds_secret_name: str    = Field(
        default="agentic-erp/rds-pgvector",  alias="RDS_SECRET_NAME"
    )
    api_keys_secret_name: str = Field(
        default="agentic-erp/api-keys",      alias="API_KEYS_SECRET_NAME"
    )
    oracle_secret_name: str = Field(
        default="agentic-erp/oracle-fusion-dev", alias="ORACLE_SECRET_NAME"
    )

    # ── API Security ──────────────────────────────────────────
    api_key: str = Field(default="dev-local-key-change-in-prod", alias="API_KEY")

    # ── S3 ────────────────────────────────────────────────────
    s3_docs_bucket: str      = Field(
        default="agentic-erp-docs-241030170015",      alias="S3_DOCS_BUCKET"
    )
    s3_artifacts_bucket: str = Field(
        default="agentic-erp-artifacts-241030170015", alias="S3_ARTIFACTS_BUCKET"
    )

    # ─────────────────────────────────────────────────────────
    # Computed properties (derived, not from env)
    # ─────────────────────────────────────────────────────────

    @computed_field
    @property
    def db_url(self) -> str:
        """
        SQLAlchemy connection string.
        Local dev  → uses SSH tunnel (localhost:5433)
        Production → uses RDS private endpoint directly
        """
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @computed_field
    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @computed_field
    @property
    def is_development(self) -> bool:
        return self.app_env == "development"

    # ─────────────────────────────────────────────────────────
    # Secrets Manager loader (call once at startup in production)
    # ─────────────────────────────────────────────────────────

    def load_rds_secret(self) -> "Settings":
        """
        In production (ECS), override DB credentials from Secrets Manager.
        In development, this is a no-op (uses .env values).

        Call this in main.py startup:
            settings.load_rds_secret()
        """
        if not self.is_production:
            logger.info("[config] Dev mode — skipping Secrets Manager, using .env values")
            return self

        secret = _fetch_secret(self.rds_secret_name, self.aws_region)
        if secret:
            self.db_host     = secret.get("host",     self.db_host)
            self.db_port     = int(secret.get("port", self.db_port))
            self.db_name     = secret.get("dbname",   self.db_name)
            self.db_user     = secret.get("username", self.db_user)
            self.db_password = secret.get("password", self.db_password)
            logger.info(f"[config] RDS credentials loaded from Secrets Manager → {self.db_host}:{self.db_port}/{self.db_name}")
        return self

    def summary(self) -> dict:
        """
        Safe summary for logging — never includes passwords.
        Call at startup to confirm config loaded correctly.
        """
        return {
            "app_name":    self.app_name,
            "app_version": self.app_version,
            "app_env":     self.app_env,
            "db_host":     self.db_host,
            "db_port":     self.db_port,
            "db_name":     self.db_name,
            "db_user":     self.db_user,
            "db_password": "***hidden***",
            "aws_region":  self.aws_region,
            "bedrock_model_sonnet": self.bedrock_model_sonnet,
            "bedrock_model_haiku":  self.bedrock_model_haiku,
            "bedrock_embed_model":  self.bedrock_embed_model,
            "s3_docs_bucket":       self.s3_docs_bucket,
            "is_production":        self.is_production,
        }

    class Config:
        # Allow both alias and field name to be used
        populate_by_name = True
        env_file = ".env"
        env_file_encoding = "utf-8"


# ─────────────────────────────────────────────────────────────
# Singleton — import this everywhere
# ─────────────────────────────────────────────────────────────

@lru_cache()
def get_settings() -> Settings:
    """
    Returns a cached Settings instance.
    Use lru_cache so Secrets Manager is called only once per process.
    """
    return Settings()


# Module-level singleton for convenience
settings = get_settings()
