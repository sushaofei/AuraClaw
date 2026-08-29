from __future__ import annotations

import json
import os
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field, SecretStr, TypeAdapter, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from auraclaw.contracts.capabilities import JavaApiServerDefinition

# Shared agent-runtime pool registration role. Coordinator/worker/reviewer are
# assignment roles chosen by Orchestrator, not values for AURACLAW_RUNTIME_ROLE.
RUNTIME_POOL_ROLE = "agent"

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
    "AURACLAW_SKILL_SIGNING_KEY",
    "AURACLAW_CREDENTIAL_VAULT_TOKEN",
    "SEAWEEDFS_ACCESS_KEY",
    "SEAWEEDFS_SECRET_KEY",
    "OBS_AK",
    "OBS_SK",
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
    for candidate in (".env.dev",):
        if Path(candidate).is_file():
            return candidate
    return None


def _is_local_dev_env_file(path: str | Path | None) -> bool:
    """True only for local developer env files (not server .env.test / .env.prod)."""
    if path is None:
        return False
    return Path(path).name == ".env.dev"


def _parse_dotenv_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


def apply_local_dev_proxy_env(env_file: str | Path | None = None) -> None:
    """Apply NO_PROXY / clear HTTP(S)_PROXY for local `.env.dev` only.

    Corporate HTTP proxies on developer machines break access to private Kafka /
    MySQL / SeaweedFS / Vault hosts. Server test and production Compose do not
    need this — they run without local proxy interference.
    """
    path = Path(env_file) if env_file is not None else None
    if path is None:
        resolved = _resolve_settings_env_file()
        path = Path(resolved) if resolved else None
    if not _is_local_dev_env_file(path):
        return
    assert path is not None
    file_values = _parse_dotenv_values(path)
    for key in ("NO_PROXY", "no_proxy"):
        # Prefer .env.dev so private middleware hosts are always covered, even when
        # the shell already exports a generic NO_PROXY (e.g. 127.0.0.1,localhost).
        value = file_values.get(key) or os.environ.get(key)
        if value:
            os.environ[key] = value
    if os.environ.get("AURACLAW_MODEL_USE_PROXY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return
    for key in (
        "http_proxy",
        "https_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "all_proxy",
    ):
        os.environ.pop(key, None)


_POSTGRESQL_TO_DB = {
    "POSTGRESQL_HOST": "DB_HOST",
    "POSTGRESQL_PORT": "DB_PORT",
    "POSTGRESQL_DB_USER": "DB_USER",
    "POSTGRESQL_DB_PWD": "DB_PWD",
    "POSTGRESQL_AURACLAW_DB": "DB_NAME",
}

_KINGBASE_TO_DB = {
    "KINGBASE_HOST": "DB_HOST",
    "KINGBASE_PORT": "DB_PORT",
    "KINGBASE_DB_USER": "DB_USER",
    "KINGBASE_DB_PWD": "DB_PWD",
    "KINGBASE_AURACLAW_DB": "DB_NAME",
}


def _settings_backend(
    selected: Mapping[str, str],
    settings_env_file: str | Path | None,
    *extra_file_values: dict[str, str],
) -> str:
    settings_file_values = (
        _parse_dotenv_values(Path(settings_env_file))
        if settings_env_file is not None
        else {}
    )
    for source in (selected, settings_file_values, *extra_file_values):
        value = (source.get("AURACLAW_STORAGE_BACKEND") or "").strip().lower()
        if value:
            return value
    return ""


def apply_postgresql_env_aliases(
    environ: dict[str, str] | None = None,
    *,
    settings_env_file: str | Path | None = None,
) -> None:
    """Map local PostgreSQL credentials onto `DB_*` when backend is postgres.

    Loads `POSTGRESQL_*` from `.postgresql.local.env` (or
    `AURACLAW_POSTGRESQL_ENV_FILE` / `.postgresql.env`). Explicit process-env
    wins over the file. When `AURACLAW_STORAGE_BACKEND=postgres`, resolved
    values overwrite `DB_*`. Domain / store code continues to use only `DB_*`
    and `resolved_database_url`.
    """
    selected = os.environ if environ is None else environ
    postgresql_path = Path(
        selected.get("AURACLAW_POSTGRESQL_ENV_FILE")
        or (
            ".postgresql.local.env"
            if Path(".postgresql.local.env").is_file()
            else ".postgresql.env"
        )
    )
    postgresql_file_values = _parse_dotenv_values(postgresql_path)

    def postgresql_value(key: str) -> str | None:
        return selected.get(key) or postgresql_file_values.get(key)

    backend = _settings_backend(selected, settings_env_file, postgresql_file_values)
    has_postgresql = any(postgresql_value(source) for source in _POSTGRESQL_TO_DB)
    if backend == "postgres":
        for source, destination in _POSTGRESQL_TO_DB.items():
            value = postgresql_value(source)
            if value:
                selected[destination] = value
        selected.setdefault("AURACLAW_STORAGE_BACKEND", "postgres")
        return

    if has_postgresql and backend in {"", "auto"} and not selected.get("DB_HOST"):
        for source, destination in _POSTGRESQL_TO_DB.items():
            value = postgresql_value(source)
            if value and not selected.get(destination):
                selected[destination] = value


def apply_kingbase_env_aliases(
    environ: dict[str, str] | None = None,
    *,
    settings_env_file: str | Path | None = None,
) -> None:
    """Map KingBase credentials onto `DB_*` when the storage backend is kingbase.

    Reads `KINGBASE_*` from the active settings env file (``.env.test`` / ``.env.prod``)
    or process environment. When ``AURACLAW_STORAGE_BACKEND=kingbase``, resolved
    ``KINGBASE_*`` values overwrite ``DB_*``. Inline ``DB_*`` in the same env file
    are used when no ``KINGBASE_*`` alias is present. Domain / store code continues to
    use only ``DB_*`` and ``resolved_database_url``.
    """
    selected = os.environ if environ is None else environ
    settings_file_values = (
        _parse_dotenv_values(Path(settings_env_file))
        if settings_env_file is not None
        else {}
    )
    optional_path = selected.get("AURACLAW_KINGBASE_ENV_FILE")
    legacy_file_values = (
        _parse_dotenv_values(Path(optional_path)) if optional_path else {}
    )

    def kingbase_value(key: str) -> str | None:
        return (
            selected.get(key)
            or settings_file_values.get(key)
            or legacy_file_values.get(key)
        )

    backend = _settings_backend(
        selected, settings_env_file, settings_file_values, legacy_file_values
    )

    has_kingbase = any(kingbase_value(source) for source in _KINGBASE_TO_DB)
    if backend == "kingbase":
        for source, destination in _KINGBASE_TO_DB.items():
            value = kingbase_value(source)
            if value:
                selected[destination] = value
        selected.setdefault("AURACLAW_STORAGE_BACKEND", "kingbase")
        return

    # Fill empty DB_* from KingBase when backend is auto and no DB_HOST yet.
    if has_kingbase and backend in {"", "auto"} and not selected.get("DB_HOST"):
        for source, destination in _KINGBASE_TO_DB.items():
            value = kingbase_value(source)
            if value and not selected.get(destination):
                selected[destination] = value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env.dev", env_prefix="AURACLAW_", extra="ignore"
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
    test_uplink_insecure_identity: bool | None = None
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
    java_api_servers_json: str = "[]"
    debug_vault_secrets_json: str = "{}"
    mcp_reconcile_interval_seconds: float = Field(default=60.0, ge=5.0, le=3600.0)
    mcp_revision_reconcile_interval_seconds: float = Field(
        default=30.0, ge=5.0, le=3600.0
    )
    mcp_allow_private_auth_none: bool | None = None
    mcp_trust_remote_tool_annotations: bool = False
    skill_signing_key: SecretStr | None = None
    skill_admission_retention_days: int = Field(default=365, ge=30, le=3650)
    skill_admission_cleanup_interval_seconds: float = Field(
        default=3600.0, ge=60.0, le=86400.0
    )
    skill_admission_cleanup_batch_size: int = Field(default=1000, ge=1, le=10_000)
    skill_admission_metrics_window_hours: int = Field(default=24, ge=1, le=2160)
    skill_admission_quarantine_alert_ratio: float = Field(default=0.25, ge=0, le=1)
    skill_admission_quarantine_alert_min_samples: int = Field(
        default=20, ge=1, le=1_000_000
    )
    credential_vault_addr: str | None = None
    credential_vault_token: SecretStr | None = None
    credential_vault_mount: str = "secret"
    artifact_base_url: str = "http://127.0.0.1:8009"
    delivery_base_url: str = "http://127.0.0.1:8011"
    log_level: str = "INFO"
    storage_backend: Literal["auto", "memory", "postgres", "mysql", "kingbase"] = "auto"
    db_dialect: Literal["mysql", "postgres"] = "mysql"
    database_url: str = "mysql+aiomysql://auraclaw:auraclaw@localhost:3306/auraclaw"
    migration_database_url: SecretStr | None = None
    db_host: str | None = Field(default=None, validation_alias="DB_HOST")
    db_port: int = Field(default=3306, validation_alias="DB_PORT")
    db_user: str | None = Field(default=None, validation_alias="DB_USER")
    db_password: str | None = Field(default=None, validation_alias="DB_PWD")
    db_name: str | None = Field(default=None, validation_alias="DB_NAME")
    artifact_backend: Literal["auto", "local", "seaweedfs", "obs"] = "auto"
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
    obs_endpoint: str | None = Field(default=None, validation_alias="OBS_ENDPOINT")
    obs_bucket: str = Field(default="auraclaw-artifacts", validation_alias="OBS_BUCKET")
    obs_ak: SecretStr | None = Field(default=None, validation_alias="OBS_AK")
    obs_sk: SecretStr | None = Field(default=None, validation_alias="OBS_SK")
    obs_region: str = Field(default="us-east-1", validation_alias="OBS_REGION")
    obs_use_ssl: bool = Field(default=True, validation_alias="OBS_USE_SSL")
    obs_path_style: bool = Field(default=False, validation_alias="OBS_PATH_STYLE")
    obs_domain: str | None = Field(default=None, validation_alias="OBS_DOMAIN")
    obs_tenant_id: str | None = Field(default=None, validation_alias="OBS_TENANT_ID")
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
    runtime_poll_interval: float = 0.05
    # Shared production-topology worker ticks (Outbox → Feed / Projection).
    # Keep identical semantics across compose.test and compose.prod.
    # With worker_wake_enabled, idle uses worker_idle_interval; busy ticks drain
    # immediately after Session outbox HTTP wake.
    worker_wake_enabled: bool = True
    worker_idle_interval: float = Field(default=0.25, ge=0.05, le=60.0)
    projection_worker_interval: float = Field(default=0.1, ge=0.01, le=30.0)
    orchestrator_worker_interval: float = Field(default=0.1, ge=0.01, le=30.0)
    orchestrator_lease_ttl_seconds: int = Field(default=300, ge=30, le=3600)
    sync_invoke_default_timeout_seconds: int = Field(default=60, ge=1, le=3600)
    sync_invoke_max_timeout_seconds: int = Field(default=120, ge=1, le=3600)
    sync_invoke_poll_interval_seconds: float = Field(default=0.25, ge=0.05, le=5.0)
    sync_invoke_max_concurrent: int = Field(default=32, ge=1, le=1000)
    runtime_id: str = "runtime-local-1"
    runtime_role: str = RUNTIME_POOL_ROLE
    runtime_node_id: str = "local"
    runtime_capacity: int = Field(default=1, ge=1)
    model_api_key: str | None = None
    model_base_url: str | None = None
    model_name: str | None = None
    model_provider: str = "openai_compatible"
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
    def validate_runtime_pool_role(self) -> Settings:
        if self.runtime_role != RUNTIME_POOL_ROLE:
            raise ValueError(
                "AURACLAW_RUNTIME_ROLE must be "
                f"{RUNTIME_POOL_ROLE!r}; got {self.runtime_role!r}. "
                "Coordinator/worker/reviewer assignment roles are chosen by "
                "Orchestrator, not by this setting."
            )
        return self

    @model_validator(mode="after")
    def validate_artifact_backend(self) -> Settings:
        if self.artifact_backend == "seaweedfs":
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
        if self.artifact_backend != "obs":
            return self
        missing = []
        if not self.obs_endpoint:
            missing.append("OBS_ENDPOINT")
        if not self.obs_bucket.strip():
            missing.append("OBS_BUCKET")
        if self.obs_ak is None or not self.obs_ak.get_secret_value():
            missing.append("OBS_AK")
        if self.obs_sk is None or not self.obs_sk.get_secret_value():
            missing.append("OBS_SK")
        if not self.obs_region.strip():
            missing.append("OBS_REGION")
        if missing:
            raise ValueError(f"OBS backend requires: {', '.join(missing)}")
        return self

    @property
    def resolved_db_dialect(self) -> Literal["mysql", "postgres"]:
        if self.storage_backend in {"postgres", "kingbase"}:
            return "postgres"
        if self.storage_backend == "mysql":
            return "mysql"
        url = (self.database_url or "").lower()
        if (
            url.startswith("postgresql:")
            or url.startswith("postgres:")
            or url.startswith("kingbase:")
            or url.startswith("kingbase+")
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
        url = self.database_url
        lowered = url.lower()
        if lowered.startswith("kingbase+asyncpg://"):
            return "postgresql+asyncpg://" + url.split("://", 1)[1]
        if lowered.startswith("kingbase://"):
            return "postgresql+asyncpg://" + url.split("://", 1)[1]
        return url

    @property
    def resolved_migration_database_url(self) -> str:
        if self.migration_database_url is not None:
            return self.migration_database_url.get_secret_value()
        return self.resolved_database_url

    @property
    def sql_storage_enabled(self) -> bool:
        if self.storage_backend == "memory":
            return False
        if self.storage_backend in {"postgres", "mysql", "kingbase"}:
            return True
        return bool(self.db_host and self.db_user and self.db_name)

    @property
    def postgres_enabled(self) -> bool:
        """True when primary SQL storage is PostgreSQL-compatible (incl. KingBase)."""
        return self.sql_storage_enabled and self.resolved_db_dialect == "postgres"

    @property
    def mysql_enabled(self) -> bool:
        return self.sql_storage_enabled and self.resolved_db_dialect == "mysql"

    @property
    def kingbase_enabled(self) -> bool:
        return self.sql_storage_enabled and self.storage_backend == "kingbase"

    @property
    def storage_label(self) -> str:
        if not self.sql_storage_enabled:
            return "memory"
        if self.storage_backend == "kingbase":
            return "kingbase"
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
    def resolved_artifact_backend(self) -> Literal["local", "seaweedfs", "obs"]:
        if self.artifact_backend == "local":
            return "local"
        if self.artifact_backend == "seaweedfs":
            return "seaweedfs"
        if self.artifact_backend == "obs":
            return "obs"
        if self.obs_endpoint:
            return "obs"
        if self.seaweedfs_host is not None:
            return "seaweedfs"
        return "local"

    @property
    def object_storage_enabled(self) -> bool:
        return self.resolved_artifact_backend in {"seaweedfs", "obs"}

    @property
    def seaweedfs_enabled(self) -> bool:
        return self.resolved_artifact_backend == "seaweedfs"

    @property
    def obs_enabled(self) -> bool:
        return self.resolved_artifact_backend == "obs"

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
    def obs_s3_endpoint(self) -> str:
        scheme = "https" if self.obs_use_ssl else "http"
        endpoint = (self.obs_endpoint or "127.0.0.1").strip().rstrip("/")
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        return f"{scheme}://{endpoint}"

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
    def insecure_identity_headers_enabled(self) -> bool:
        if self.test_uplink_insecure_identity is True:
            return True
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


def _validate_local_dev_storage(settings: Settings, env_file: str | Path | None) -> None:
    if not _is_local_dev_env_file(env_file):
        return
    if settings.sql_storage_enabled:
        return
    raise ValueError(
        "Local development (.env.dev) requires SQL storage so registrations and "
        "projections survive process restarts. Set AURACLAW_STORAGE_BACKEND to "
        "postgres, mysql, or kingbase and configure DB_* credentials."
    )


@lru_cache
def get_settings() -> Settings:
    load_secret_files()
    env_file = _resolve_settings_env_file()
    apply_local_dev_proxy_env(env_file)
    apply_postgresql_env_aliases(settings_env_file=env_file)
    apply_kingbase_env_aliases(settings_env_file=env_file)
    settings = Settings(_env_file=env_file)
    _validate_local_dev_storage(settings, env_file)
    return settings
