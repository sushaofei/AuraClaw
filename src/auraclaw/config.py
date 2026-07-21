from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field
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
    artifact_root: Path = Path(".data/artifacts")
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
