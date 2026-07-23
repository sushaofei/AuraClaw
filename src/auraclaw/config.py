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
    deployment_profile: Literal["development", "production"] = "development"
    task_api_port: int = 8000
    session_port: int = 8001
    projection_port: int = 8002
    orchestrator_port: int = 8003
    runtime_port: int = 8004
    model_gateway_port: int = 8005
    hands_port: int = 8006
    policy_port: int = 8007
    credential_proxy_port: int = 8008
    artifact_port: int = 8009
    streaming_port: int = 8010
    delivery_port: int = 8011
    lease_signing_key: SecretStr | None = None
    task_api_workload_token: SecretStr | None = None
    orchestrator_workload_token: SecretStr | None = None
    projection_workload_token: SecretStr | None = None
    runtime_workload_token: SecretStr | None = None
    model_gateway_workload_token: SecretStr | None = None
    action_hands_workload_token: SecretStr | None = None
    credential_proxy_workload_token: SecretStr | None = None
    artifact_service_workload_token: SecretStr | None = None
    policy_workload_token: SecretStr | None = None
    delivery_workload_token: SecretStr | None = None
    session_base_url: str = "http://127.0.0.1:8001"
    projection_base_url: str = "http://127.0.0.1:8002"
    control_base_url: str = "http://127.0.0.1:8003"
    model_gateway_base_url: str = "http://127.0.0.1:8005"
    hands_mcp_url: str = "http://127.0.0.1:8006/mcp"
    policy_base_url: str = "http://127.0.0.1:8007"
    credential_proxy_base_url: str = "http://127.0.0.1:8008"
    credential_egress_allowlist: str = ""
    credential_vault_addr: str | None = None
    credential_vault_token: SecretStr | None = None
    credential_vault_mount: str = "secret"
    artifact_base_url: str = "http://127.0.0.1:8009"
    delivery_base_url: str = "http://127.0.0.1:8011"
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
    runtime_id: str = "runtime-local-1"
    runtime_role: str = "root"
    runtime_node_id: str = "local"
    runtime_capacity: int = Field(default=1, ge=1)
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
    def allowed_credential_egress_hosts(self) -> tuple[str, ...]:
        return tuple(
            host.strip().lower()
            for host in self.credential_egress_allowlist.split(",")
            if host.strip()
        )

    @property
    def model_gateway_configured(self) -> bool:
        return bool(self.model_api_key and self.model_base_url and self.model_name)

    def workload_token_value(self, service_name: str) -> str | None:
        tokens = {
            "task-api": self.task_api_workload_token,
            "orchestrator": self.orchestrator_workload_token,
            "projection-worker": self.projection_workload_token,
            "agent-runtime": self.runtime_workload_token,
            "model-gateway": self.model_gateway_workload_token,
            "action-hands": self.action_hands_workload_token,
            "credential-proxy": self.credential_proxy_workload_token,
            "artifact-service": self.artifact_service_workload_token,
            "policy": self.policy_workload_token,
            "delivery-worker": self.delivery_workload_token,
        }
        token = tokens.get(service_name)
        return token.get_secret_value() if token is not None else None

    def service_port(self, service_name: str) -> int:
        ports = {
            "task-api": self.task_api_port,
            "session": self.session_port,
            "projection-worker": self.projection_port,
            "orchestrator": self.orchestrator_port,
            "agent-runtime": self.runtime_port,
            "model-gateway": self.model_gateway_port,
            "action-hands": self.hands_port,
            "policy": self.policy_port,
            "credential-proxy": self.credential_proxy_port,
            "artifact-service": self.artifact_port,
            "streaming-gateway": self.streaming_port,
            "delivery-worker": self.delivery_port,
        }
        try:
            return ports[service_name]
        except KeyError as exc:
            raise ValueError(f"unknown service: {service_name}") from exc


@lru_cache
def get_settings() -> Settings:
    return Settings()
