from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field, SecretStr, TypeAdapter, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from auraclaw.contracts.capabilities import JavaApiServerDefinition, McpServerDefinition

_SECRET_FILE_VARIABLES = {
    "AURACLAW_DATABASE_URL",
    "AURACLAW_MIGRATION_DATABASE_URL",
    "AURACLAW_TASK_API_WORKLOAD_TOKEN",
    "AURACLAW_ORCHESTRATOR_WORKLOAD_TOKEN",
    "AURACLAW_PROJECTION_WORKLOAD_TOKEN",
    "AURACLAW_RUNTIME_WORKLOAD_TOKEN",
    "AURACLAW_MODEL_GATEWAY_WORKLOAD_TOKEN",
    "AURACLAW_ACTION_HANDS_WORKLOAD_TOKEN",
    "AURACLAW_CREDENTIAL_PROXY_WORKLOAD_TOKEN",
    "AURACLAW_ARTIFACT_SERVICE_WORKLOAD_TOKEN",
    "AURACLAW_POLICY_WORKLOAD_TOKEN",
    "AURACLAW_DELIVERY_WORKLOAD_TOKEN",
    "AURACLAW_STREAMING_GATEWAY_WORKLOAD_TOKEN",
    "AURACLAW_CHAINTOWER_WORKLOAD_TOKEN",
    "AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON",
    "AURACLAW_LEASE_SIGNING_KEY",
    "AURACLAW_MODEL_API_KEY",
    "AURACLAW_MODEL_SKILL_SIGNING_KEY",
    "AURACLAW_PRICE_INSIGHT_MYSQL_PASSWORD",
    "AURACLAW_CREDENTIAL_VAULT_TOKEN",
    "MYSQL_DB_PWD",
    "SEAWEEDFS_ACCESS_KEY",
    "SEAWEEDFS_SECRET_KEY",
}


def load_secret_files(environ: dict[str, str] | None = None) -> None:
    selected = os.environ if environ is None else environ
    for variable in sorted(_SECRET_FILE_VARIABLES):
        if selected.get(variable):
            continue
        path_value = selected.get(f"{variable}_FILE")
        if not path_value:
            continue
        path = Path(path_value)
        if not path.is_file():
            raise ValueError(f"secret file is unavailable for {variable}")
        if path.stat().st_size > 64 * 1024:
            raise ValueError(f"secret file is too large for {variable}")
        value = path.read_text().rstrip("\r\n")
        if not value:
            raise ValueError(f"secret file is empty for {variable}")
        selected[variable] = value


def _resolve_settings_env_file() -> str | None:
    if os.environ.get("AURACLAW_DISABLE_ENV_FILE") == "1":
        return None
    configured = os.environ.get("AURACLAW_ENV_FILE")
    if configured:
        return configured
    for candidate in (".env.debug", ".env"):
        if Path(candidate).is_file():
            return candidate
    return None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="AURACLAW_", extra="ignore"
    )

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
    ingress_port: int = 8080
    ingress_enabled: bool = True
    lease_signing_key: SecretStr | None = None
    allow_insecure_identity_headers: bool | None = None
    chaintower_workload_token: SecretStr | None = None
    agent_context_issuer: str = "chaintower"
    agent_context_audience: str = "auraclaw-task-api"
    agent_context_signing_keys_json: str = "{}"
    agent_context_max_ttl_seconds: int = Field(default=300, ge=30, le=600)
    agent_context_clock_skew_seconds: int = Field(default=30, ge=0, le=120)
    agent_context_required_scope: str = "agent.task.invoke"
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
    streaming_gateway_workload_token: SecretStr | None = None
    session_base_url: str = "http://127.0.0.1:8001"
    projection_base_url: str = "http://127.0.0.1:8002"
    control_base_url: str = "http://127.0.0.1:8003"
    runtime_base_url: str = "http://127.0.0.1:8004"
    model_gateway_base_url: str = "http://127.0.0.1:8005"
    hands_url: str = "http://127.0.0.1:8006"
    policy_base_url: str = "http://127.0.0.1:8007"
    credential_proxy_base_url: str = "http://127.0.0.1:8008"
    credential_egress_allowlist: str = ""
    mcp_egress_servers_json: str = "[]"
    mcp_egress_servers_file: str | None = None
    java_api_servers_json: str = "[]"
    debug_vault_secrets_json: str = "{}"
    mcp_reconcile_interval_seconds: float = Field(default=60.0, ge=5.0, le=3600.0)
    mcp_trust_remote_tool_annotations: bool = False
    model_skill_source_enabled: bool = True
    model_skill_source_tenant_id: int = Field(default=1, ge=0)
    model_skill_target_tenant_id: str = Field(default="development", min_length=1)
    model_skill_include_drafts: bool = True
    model_skill_reconcile_interval_seconds: float = Field(
        default=60.0,
        ge=5.0,
        le=3600.0,
    )
    model_skill_signing_key: SecretStr | None = None
    model_skill_mysql_host: str | None = Field(
        default=None, validation_alias="MYSQL_DB_HOST"
    )
    model_skill_mysql_port: int = Field(
        default=3306, ge=1, le=65535, validation_alias="MYSQL_DB_PORT"
    )
    model_skill_mysql_user: str | None = Field(
        default=None, validation_alias="MYSQL_DB_USER"
    )
    model_skill_mysql_password: SecretStr | None = Field(
        default=None, validation_alias="MYSQL_DB_PWD"
    )
    model_skill_mysql_database: str | None = Field(
        default=None, validation_alias="MYSQL_DB_NAME"
    )
    price_insight_source: Literal["auto", "disabled", "fixture", "mysql"] = "auto"
    price_insight_target_tenant_id: str = Field(default="development", min_length=1)
    price_insight_mysql_host: str | None = None
    price_insight_mysql_port: int = Field(default=3306, ge=1, le=65535)
    price_insight_mysql_user: str | None = None
    price_insight_mysql_password: SecretStr | None = None
    price_insight_mysql_database: str | None = None
    credential_vault_addr: str | None = None
    credential_vault_token: SecretStr | None = None
    credential_vault_mount: str = "secret"
    artifact_base_url: str = "http://127.0.0.1:8009"
    delivery_base_url: str = "http://127.0.0.1:8011"
    log_level: str = "INFO"
    storage_backend: Literal["auto", "memory", "postgres", "mysql"] = "auto"
    db_dialect: Literal["mysql", "postgres"] = "mysql"
    database_url: str = "mysql+aiomysql://auraclaw:auraclaw@localhost:3306/auraclaw"
    migration_database_url: SecretStr | None = None
    db_host: str | None = Field(default=None, validation_alias="DB_HOST")
    db_port: int = Field(default=3306, validation_alias="DB_PORT")
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
    seaweedfs_bucket: str = Field(
        default="auraclaw-artifacts", validation_alias="SEAWEEDFS_BUCKET"
    )
    seaweedfs_region: str = Field(
        default="us-east-1", validation_alias="SEAWEEDFS_REGION"
    )
    seaweedfs_use_ssl: bool = Field(default=False, validation_alias="SEAWEEDFS_USE_SSL")
    seaweedfs_path_style: bool = Field(
        default=True, validation_alias="SEAWEEDFS_PATH_STYLE"
    )
    artifact_multipart_threshold: int = Field(
        default=16 * 1024 * 1024, ge=5 * 1024 * 1024
    )
    artifact_multipart_part_size: int = Field(
        default=8 * 1024 * 1024, ge=5 * 1024 * 1024
    )
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
    # Shared production-topology worker ticks (Outbox → Feed / Projection).
    # Keep identical semantics across compose.services and compose.production.
    # With worker_wake_enabled, idle uses worker_idle_interval; busy ticks drain
    # immediately after Session outbox HTTP wake.
    worker_wake_enabled: bool = True
    worker_idle_interval: float = Field(default=0.25, ge=0.05, le=60.0)
    projection_worker_interval: float = Field(default=0.1, ge=0.01, le=30.0)
    orchestrator_worker_interval: float = Field(default=0.1, ge=0.01, le=30.0)
    orchestrator_lease_ttl_seconds: int = Field(default=300, ge=30, le=3600)
    runtime_id: str = "runtime-local-1"
    runtime_role: str = "root"
    runtime_node_id: str = "local"
    runtime_capacity: int = Field(default=1, ge=1)
    model_api_key: str | None = None
    model_base_url: str | None = None
    model_name: str | None = None
    model_provider: str = "openai_compatible"
    development_model_mode: Literal["provider", "price-insight-scripted"] = "provider"
    model_timeout_seconds: float = 120.0
    model_tenant_token_limit_per_hour: int = Field(default=1_000_000, ge=1)
    # None omits the field; True/False maps to OpenAI-compatible thinking.type enabled/disabled.
    model_thinking_enabled: bool | None = None

    @model_validator(mode="after")
    def validate_identity_settings(self) -> Settings:
        if (
            self.deployment_profile == "production"
            and self.allow_insecure_identity_headers is True
        ):
            raise ValueError("insecure identity headers cannot be enabled in production")
        return self

    @model_validator(mode="after")
    def validate_artifact_backend(self) -> Settings:
        if self.artifact_backend != "seaweedfs":
            return self
        missing = []
        if not self.seaweedfs_host:
            missing.append("SEAWEEDFS_HOST")
        if not self.seaweedfs_bucket.strip():
            missing.append("SEAWEEDFS_BUCKET")
        if (
            self.seaweedfs_access_key is None
            or not self.seaweedfs_access_key.get_secret_value()
        ):
            missing.append("SEAWEEDFS_ACCESS_KEY")
        if (
            self.seaweedfs_secret_key is None
            or not self.seaweedfs_secret_key.get_secret_value()
        ):
            missing.append("SEAWEEDFS_SECRET_KEY")
        if missing:
            raise ValueError(f"SeaweedFS backend requires: {', '.join(missing)}")
        return self

    @property
    def resolved_db_dialect(self) -> Literal["mysql", "postgres"]:
        if self.storage_backend == "postgres":
            return "postgres"
        if self.storage_backend == "mysql":
            return "mysql"
        url = (self.database_url or "").lower()
        if (
            url.startswith("postgresql:")
            or url.startswith("postgres:")
            or "+asyncpg" in url
        ):
            return "postgres"
        if (
            url.startswith("mysql:")
            or "+aiomysql" in url
            or "+asyncmy" in url
            or "+pymysql" in url
        ):
            return "mysql"
        return self.db_dialect

    @property
    def resolved_database_url(self) -> str:
        if (
            self.db_host
            and self.db_user
            and self.db_password is not None
            and self.db_name
        ):
            user = quote(self.db_user, safe="")
            password = quote(self.db_password, safe="")
            database = quote(self.db_name, safe="")
            dialect = self.resolved_db_dialect
            if dialect == "mysql":
                return f"mysql+aiomysql://{user}:{password}@{self.db_host}:{self.db_port}/{database}"
            return f"postgresql+asyncpg://{user}:{password}@{self.db_host}:{self.db_port}/{database}"
        return self.database_url

    @property
    def resolved_migration_database_url(self) -> str:
        if self.migration_database_url is not None:
            return self.migration_database_url.get_secret_value()
        return self.resolved_database_url

    @property
    def sql_storage_enabled(self) -> bool:
        if self.storage_backend == "memory":
            return False
        if self.storage_backend in {"postgres", "mysql"}:
            return True
        return bool(self.db_host and self.db_user and self.db_name)

    @property
    def postgres_enabled(self) -> bool:
        """True when primary SQL storage is PostgreSQL (backward-compatible name)."""
        return self.sql_storage_enabled and self.resolved_db_dialect == "postgres"

    @property
    def mysql_enabled(self) -> bool:
        return self.sql_storage_enabled and self.resolved_db_dialect == "mysql"

    @property
    def storage_label(self) -> str:
        if not self.sql_storage_enabled:
            return "memory"
        return self.resolved_db_dialect

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
        return (
            f"{scheme}://{self.seaweedfs_host or '127.0.0.1'}:{self.seaweedfs_s3_port}"
        )

    @property
    def allowed_cors_origins(self) -> list[str]:
        configured = [
            origin.strip()
            for origin in self.cors_allow_origins.split(",")
            if origin.strip()
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
    def mcp_egress_servers(self) -> tuple[McpServerDefinition, ...]:
        raw = self.mcp_egress_servers_json
        if self.mcp_egress_servers_file:
            raw = Path(self.mcp_egress_servers_file).read_text(encoding="utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("MCP egress server configuration is invalid JSON") from exc
        return TypeAdapter(tuple[McpServerDefinition, ...]).validate_python(payload)

    @property
    def debug_vault_secrets(self) -> dict[str, str]:
        try:
            payload = json.loads(self.debug_vault_secrets_json)
        except json.JSONDecodeError as exc:
            raise ValueError("debug vault secrets configuration is invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("debug vault secrets must be a JSON object")
        return {str(key): str(value) for key, value in payload.items()}

    @property
    def java_api_servers(self) -> tuple[JavaApiServerDefinition, ...]:
        try:
            payload = json.loads(self.java_api_servers_json)
        except json.JSONDecodeError as exc:
            raise ValueError("Java API server configuration is invalid JSON") from exc
        return TypeAdapter(tuple[JavaApiServerDefinition, ...]).validate_python(payload)

    @property
    def model_gateway_configured(self) -> bool:
        return bool(self.model_api_key and self.model_base_url and self.model_name)

    @property
    def model_skill_source_configured(self) -> bool:
        return bool(
            self.model_skill_source_enabled
            and self.model_skill_mysql_host
            and self.model_skill_mysql_user
            and self.model_skill_mysql_password is not None
            and self.model_skill_mysql_password.get_secret_value()
            and self.model_skill_mysql_database
        )

    @property
    def resolved_price_insight_source(
        self,
    ) -> Literal["disabled", "fixture", "mysql"]:
        if self.price_insight_source != "auto":
            return self.price_insight_source
        if self.price_insight_mysql_configured:
            return "mysql"
        return "disabled"

    @property
    def price_insight_mysql_configured(self) -> bool:
        return bool(
            self.price_insight_mysql_host
            and self.price_insight_mysql_user
            and self.price_insight_mysql_password is not None
            and self.price_insight_mysql_password.get_secret_value()
            and self.price_insight_mysql_database
        )

    @property
    def insecure_identity_headers_enabled(self) -> bool:
        if self.deployment_profile == "production":
            return False
        return self.allow_insecure_identity_headers is True

    @property
    def agent_context_signing_keys(self) -> dict[str, bytes]:
        try:
            payload = json.loads(self.agent_context_signing_keys_json or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("agent context signing keys must be a JSON object") from exc
        if not isinstance(payload, dict):
            raise ValueError("agent context signing keys must be a JSON object")
        keys: dict[str, bytes] = {}
        for key_id, value in payload.items():
            encoded = str(value).encode()
            if len(encoded) < 32:
                raise ValueError("agent context signing key must contain at least 32 bytes")
            keys[str(key_id)] = encoded
        return keys

    @property
    def signed_identity_configured(self) -> bool:
        token = self.chaintower_workload_token
        return bool(
            token is not None
            and token.get_secret_value()
            and self.agent_context_signing_keys
        )

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
            "streaming-gateway": self.streaming_gateway_workload_token,
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
    load_secret_files()
    return Settings(_env_file=_resolve_settings_env_file())
