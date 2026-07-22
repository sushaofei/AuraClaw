from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="AURACLAW_", extra="ignore")

    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "INFO"
    storage_backend: Literal["auto", "memory", "postgres"] = "auto"
    database_url: str = "postgresql+asyncpg://auraclaw:auraclaw@localhost:5432/auraclaw"
    db_host: str | None = Field(default=None, validation_alias="DB_HOST")
    db_port: int = Field(default=5432, validation_alias="DB_PORT")
    db_user: str | None = Field(default=None, validation_alias="DB_USER")
    db_password: str | None = Field(default=None, validation_alias="DB_PWD")
    db_name: str | None = Field(default=None, validation_alias="DB_NAME")
    artifact_backend: Literal["auto", "local", "seaweedfs"] = "auto"
    artifact_root: Path = Path(".data/artifacts")
    seaweedfs_host: str | None = Field(default=None, validation_alias="SEAWEEDFS_HOST")
    seaweedfs_master_port: int = Field(
        default=9333, ge=1, le=65535, validation_alias="SEAWEEDFS_MASTER_PORT"
    )
    seaweedfs_filer_port: int = Field(
        default=8888, ge=1, le=65535, validation_alias="SEAWEEDFS_FILER_PORT"
    )
    seaweedfs_s3_port: int = Field(
        default=8333, ge=1, le=65535, validation_alias="SEAWEEDFS_S3_PORT"
    )
    seaweedfs_access_key: SecretStr | None = Field(
        default=None, validation_alias="SEAWEEDFS_ACCESS_KEY"
    )
    seaweedfs_secret_key: SecretStr | None = Field(
        default=None, validation_alias="SEAWEEDFS_SECRET_KEY"
    )
    seaweedfs_bucket: str = Field(default="auraclaw-artifacts", validation_alias="SEAWEEDFS_BUCKET")
    seaweedfs_region: str = Field(default="us-east-1", validation_alias="SEAWEEDFS_REGION")
    seaweedfs_use_ssl: bool = Field(default=False, validation_alias="SEAWEEDFS_USE_SSL")
    seaweedfs_path_style: bool = Field(default=True, validation_alias="SEAWEEDFS_PATH_STYLE")
    runtime_event_backend: Literal["auto", "memory", "kafka"] = "auto"
    kafka_host: str | None = Field(default=None, validation_alias="KAFKA_HOST")
    kafka_port: int = Field(default=9092, validation_alias="KAFKA_PORT")
    kafka_runtime_topic: str = "managed-agent.runtime-events"
    kafka_streaming_group: str = "streaming-ingestor"
    runtime_event_retention_events: int = 1_000
    stream_connection_queue_size: int = 128
    cors_allow_origins: str = ""
    runtime_enabled: bool = True
    runtime_poll_interval: float = 0.05
    model_api_key: str | None = None
    model_base_url: str | None = None
    model_name: str | None = None
    model_provider: str = "openai_compatible"
    model_timeout_seconds: float = 120.0
    # None omits the field; True/False maps to OpenAI-compatible thinking.type enabled/disabled.
    model_thinking_enabled: bool | None = None

    @model_validator(mode="after")
    def validate_artifact_backend(self) -> Settings:
        if self.artifact_backend != "seaweedfs":
            return self
        missing = []
        if not self.seaweedfs_host:
            missing.append("SEAWEEDFS_HOST")
        if not self.seaweedfs_bucket.strip():
            missing.append("SEAWEEDFS_BUCKET")
        if self.seaweedfs_access_key is None or not self.seaweedfs_access_key.get_secret_value():
            missing.append("SEAWEEDFS_ACCESS_KEY")
        if self.seaweedfs_secret_key is None or not self.seaweedfs_secret_key.get_secret_value():
            missing.append("SEAWEEDFS_SECRET_KEY")
        if missing:
            raise ValueError(f"SeaweedFS backend requires: {', '.join(missing)}")
        return self

    @property
    def resolved_database_url(self) -> str:
        if self.db_host and self.db_user and self.db_password is not None and self.db_name:
            user = quote(self.db_user, safe="")
            password = quote(self.db_password, safe="")
            database = quote(self.db_name, safe="")
            return (
                f"postgresql+asyncpg://{user}:{password}@{self.db_host}:{self.db_port}/{database}"
            )
        return self.database_url

    @property
    def postgres_enabled(self) -> bool:
        if self.storage_backend == "memory":
            return False
        if self.storage_backend == "postgres":
            return True
        return bool(self.db_host and self.db_user and self.db_name)

    @property
    def kafka_enabled(self) -> bool:
        if self.runtime_event_backend == "memory":
            return False
        if self.runtime_event_backend == "kafka":
            return True
        return self.kafka_host is not None

    @property
    def kafka_bootstrap_servers(self) -> str:
        return f"{self.kafka_host or '127.0.0.1'}:{self.kafka_port}"

    @property
    def seaweedfs_enabled(self) -> bool:
        if self.artifact_backend == "local":
            return False
        if self.artifact_backend == "seaweedfs":
            return True
        return self.seaweedfs_host is not None

    @property
    def seaweedfs_master(self) -> str:
        return f"{self.seaweedfs_host or '127.0.0.1'}:{self.seaweedfs_master_port}"

    @property
    def seaweedfs_filer_url(self) -> str:
        scheme = "https" if self.seaweedfs_use_ssl else "http"
        return f"{scheme}://{self.seaweedfs_host or '127.0.0.1'}:{self.seaweedfs_filer_port}"

    @property
    def seaweedfs_s3_endpoint(self) -> str:
        scheme = "https" if self.seaweedfs_use_ssl else "http"
        return f"{scheme}://{self.seaweedfs_host or '127.0.0.1'}:{self.seaweedfs_s3_port}"

    @property
    def allowed_cors_origins(self) -> list[str]:
        configured = [
            origin.strip() for origin in self.cors_allow_origins.split(",") if origin.strip()
        ]
        return configured

    @property
    def model_gateway_configured(self) -> bool:
        return bool(self.model_api_key and self.model_base_url and self.model_name)


@lru_cache
def get_settings() -> Settings:
    return Settings()
