import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from auraclaw.config import Settings, load_secret_files
from auraclaw.infrastructure.persistence.migration_runner import (
    MigrationError,
    discover_migrations,
)

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "compose.production.yml"
APPLICATION_SERVICES = {
    "task-api",
    "session",
    "projection-worker",
    "orchestrator",
    "agent-runtime",
    "model-gateway",
    "action-hands",
    "policy",
    "credential-proxy",
    "artifact-service",
    "streaming-gateway",
    "delivery-worker",
}


def _render_compose() -> dict[str, object]:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ROOT / ".env.prod.example"),
            "-f",
            str(COMPOSE),
            "--profile",
            "migrate",
            "config",
            "--format",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    return json.loads(result.stdout)  # type: ignore[no-any-return]


def test_production_compose_enforces_replica_resource_and_security_boundaries() -> None:
    rendered = _render_compose()
    services = rendered["services"]
    assert isinstance(services, dict)
    assert APPLICATION_SERVICES < services.keys()
    assert {"migrate", "ingress"} < services.keys()

    identities: set[str] = set()
    for name in APPLICATION_SERVICES:
        service = services[name]
        assert service["user"] == "10001:10001"
        assert service["read_only"] is True
        assert service["cap_drop"] == ["ALL"]
        assert service["security_opt"] == ["no-new-privileges:true"]
        assert service["healthcheck"]
        assert not service.get("ports")
        deploy = service["deploy"]
        assert deploy["replicas"] >= 2
        assert deploy["resources"]["reservations"]
        assert deploy["resources"]["limits"]
        assert deploy["update_config"]["parallelism"] == 1
        assert deploy["rollback_config"]["failure_action"] == "pause"
        identity = service["labels"]["auraclaw.service-identity"]
        assert identity == name
        identities.add(identity)
        assert service["labels"]["auraclaw.database-role"]
    assert identities == APPLICATION_SERVICES
    assert rendered["networks"]["auraclaw"]["internal"] is True
    assert rendered["networks"]["edge"].get("internal", False) is False
    assert rendered["networks"]["platform"]["external"] is True
    assert services["ingress"]["ports"] == [
        {"mode": "ingress", "target": 8080, "published": "8080", "protocol": "tcp"}
    ]
    assert services["ingress"]["healthcheck"]
    assert set(services["ingress"]["networks"]) == {"auraclaw", "edge"}


def test_ingress_reresolves_scaled_and_replaced_upstreams() -> None:
    configuration = (ROOT / "deploy/nginx.conf").read_text()
    assert "resolver 127.0.0.11" in configuration
    assert "zone auraclaw_task_api" in configuration
    assert "server task-api:8000 resolve;" in configuration
    assert "zone auraclaw_streaming_gateway" in configuration
    assert "server streaming-gateway:8010 resolve;" in configuration


def test_production_compose_mounts_least_privilege_secrets() -> None:
    rendered = _render_compose()
    services = rendered["services"]
    secrets = rendered["secrets"]

    def secret_sources(service: str) -> set[str]:
        return {item["source"] for item in services[service].get("secrets", [])}

    assert "migration_database_url" in secret_sources("migrate")
    assert all(
        "migration_database_url" not in secret_sources(service)
        for service in APPLICATION_SERVICES
    )
    assert "model_api_key" in secret_sources("model-gateway")
    assert all(
        "model_api_key" not in secret_sources(service)
        for service in APPLICATION_SERVICES - {"model-gateway"}
    )
    assert "vault_token" in secret_sources("credential-proxy")
    assert all(
        "vault_token" not in secret_sources(service)
        for service in APPLICATION_SERVICES - {"credential-proxy"}
    )
    assert {"seaweedfs_access_key", "seaweedfs_secret_key"} <= secret_sources(
        "artifact-service"
    )
    assert all(
        "seaweedfs_access_key" not in secret_sources(service)
        and "seaweedfs_secret_key" not in secret_sources(service)
        for service in APPLICATION_SERVICES - {"artifact-service"}
    )
    assert "runtime_workload_token" in secret_sources("agent-runtime")
    assert "streaming_gateway_workload_token" in secret_sources("session")
    assert "streaming_gateway_workload_token" in secret_sources("streaming-gateway")
    assert not any(
        item.endswith("database_url") for item in secret_sources("agent-runtime")
    )
    assert secrets["runtime_workload_token"]["file"].endswith(
        "/runtime_workload_token"
    )
    assert secrets["lease_signing_key"]["file"].endswith("/lease_signing_key")
    assert {
        "chaintower_workload_token",
        "agent_context_signing_keys_json",
    } <= secret_sources("task-api")
    assert all(
        "chaintower_workload_token" not in secret_sources(service)
        and "agent_context_signing_keys_json" not in secret_sources(service)
        for service in APPLICATION_SERVICES - {"task-api"}
    )


def test_secret_file_loading_is_allowlisted_precedence_safe_and_redacted(
    tmp_path: Path,
) -> None:
    secret = tmp_path / "token"
    secret.write_text("mounted-secret\n")
    environ = {"AURACLAW_RUNTIME_WORKLOAD_TOKEN_FILE": str(secret)}
    load_secret_files(environ)
    assert environ["AURACLAW_RUNTIME_WORKLOAD_TOKEN"] == "mounted-secret"

    environ["AURACLAW_RUNTIME_WORKLOAD_TOKEN"] = "direct-secret"
    secret.write_text("changed-secret")
    load_secret_files(environ)
    assert environ["AURACLAW_RUNTIME_WORKLOAD_TOKEN"] == "direct-secret"

    unavailable = tmp_path / "missing"
    with pytest.raises(
        ValueError, match="secret file is unavailable for AURACLAW_MODEL_API_KEY"
    ):
        load_secret_files({"AURACLAW_MODEL_API_KEY_FILE": str(unavailable)})


def test_migration_discovery_orders_versions_rejects_duplicates_and_ignores_down(
    tmp_path: Path,
) -> None:
    (tmp_path / "0002_second.sql").write_text("SELECT 2;")
    (tmp_path / "0001_first.sql").write_text("SELECT 1;")
    (tmp_path / "0002_second.down.sql").write_text("SELECT 0;")
    migrations = discover_migrations(tmp_path)
    assert [item.version for item in migrations] == ["0001", "0002"]
    assert all(len(item.checksum) == 64 for item in migrations)

    (tmp_path / "0002_duplicate.sql").write_text("SELECT 22;")
    with pytest.raises(MigrationError, match="duplicate migration version: 0002"):
        discover_migrations(tmp_path)


def test_committed_files_do_not_contain_environment_secret_values() -> None:
    secret_names = {
        "AURACLAW_MODEL_API_KEY",
        "AURACLAW_CREDENTIAL_VAULT_TOKEN",
        "SEAWEEDFS_ACCESS_KEY",
        "SEAWEEDFS_SECRET_KEY",
    }
    local_values = {
        value
        for name in secret_names
        if (value := os.environ.get(name)) and len(value) >= 12
    }
    committed = "".join(
        path.read_text()
        for path in (
            COMPOSE,
            ROOT / ".env.dev.example",
            ROOT / ".env.test.example",
            ROOT / ".env.prod.example",
        )
    )
    assert all(value not in committed for value in local_values)


def test_env_templates_are_ready_to_copy() -> None:
    import importlib.util

    from dotenv import dotenv_values

    spec = importlib.util.spec_from_file_location(
        "compose_preflight", ROOT / "scripts/compose_preflight.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    debug_settings = Settings(_env_file=ROOT / ".env.dev.example")
    assert debug_settings.storage_backend == "mysql"
    assert debug_settings.runtime_event_backend == "kafka"
    assert debug_settings.kafka_host == "10.244.16.132"
    assert debug_settings.artifact_backend == "seaweedfs"
    assert debug_settings.insecure_identity_headers_enabled
    assert debug_settings.deployment_profile == "development"
    assert debug_settings.lease_signing_key is not None

    debug = dotenv_values(ROOT / ".env.dev.example")
    test = dotenv_values(ROOT / ".env.test.example")
    production = dotenv_values(ROOT / ".env.prod.example")
    for label, values in (("test", test), ("production", production)):
        missing = [name for name in module.REQUIRED if not values.get(name)]
        assert missing == [], f"{label} missing {missing}"
        assert values["AURACLAW_DEPLOYMENT_PROFILE"] == "production"
        assert values["AURACLAW_ALLOW_INSECURE_IDENTITY_HEADERS"] == "false"

    local_only = {
        "AURACLAW_DEPLOYMENT_PROFILE",
        "AURACLAW_HOST",
        "AURACLAW_ALLOW_INSECURE_IDENTITY_HEADERS",
        "AURACLAW_MODEL_API_KEY",
        "AURACLAW_MODEL_BASE_URL",
        "AURACLAW_MODEL_NAME",
        "AURACLAW_CREDENTIAL_VAULT_ADDR",
        "AURACLAW_MIGRATIONS_DIRECTORY",
        "AURACLAW_PORT",
        "AURACLAW_RUNTIME_ID",
        "AURACLAW_RUNTIME_ROLE",
        "AURACLAW_RUNTIME_NODE_ID",
        "AURACLAW_RUNTIME_CAPACITY",
        # Local-dev HTTP proxy bypass only; never present on test/prod.
        "NO_PROXY",
        "no_proxy",
    }
    assert "NO_PROXY" in debug
    assert "NO_PROXY" not in test
    assert "NO_PROXY" not in production
    shared_keys = set(test) & set(debug) - local_only
    mismatches = [
        key for key in sorted(shared_keys) if test[key] != debug.get(key)
    ]
    assert mismatches == []

    tokens = [
        production[name] or ""
        for name in module.REQUIRED
        if name.endswith("_WORKLOAD_TOKEN")
    ]
    assert all(len(token) >= 32 for token in tokens)
    assert len(set(tokens)) == len(tokens)


def test_production_preflight_accepts_isolated_roles_and_unique_tokens(
    tmp_path: Path,
) -> None:
    roles = {
        "TASK_QUERY_DATABASE_URL": "auraclaw_task_query_ro",
        "SESSION_DATABASE_URL": "auraclaw_session",
        "PROJECTION_DATABASE_URL": "auraclaw_projection",
        "CONTROL_DATABASE_URL": "auraclaw_control",
        "HANDS_DATABASE_URL": "auraclaw_hands",
        "POLICY_DATABASE_URL": "auraclaw_policy",
        "CREDENTIAL_DATABASE_URL": "auraclaw_credential",
        "ARTIFACT_DATABASE_URL": "auraclaw_artifact",
        "STREAMING_DATABASE_URL": "auraclaw_streaming",
        "MODEL_DATABASE_URL": "auraclaw_model",
        "DELIVERY_DATABASE_URL": "auraclaw_delivery",
    }
    tokens = (
        "TASK_API",
        "PROJECTION",
        "ORCHESTRATOR",
        "RUNTIME",
        "MODEL_GATEWAY",
        "ACTION_HANDS",
        "CREDENTIAL_PROXY",
        "ARTIFACT_SERVICE",
        "POLICY",
        "DELIVERY",
        "STREAMING_GATEWAY",
    )
    lines = [
        "AURACLAW_IMAGE=registry.example/auraclaw:sha-0123456789",
        "AURACLAW_MIGRATION_DATABASE_URL=postgresql://migration:secret@db/auraclaw",
        "AURACLAW_LEASE_SIGNING_KEY=" + "l" * 48,
        "AURACLAW_MODEL_API_KEY=test-model-secret",
        "AURACLAW_MODEL_BASE_URL=https://models.example/v1",
        "AURACLAW_MODEL_NAME=test-model",
        "AURACLAW_CREDENTIAL_VAULT_ADDR=https://vault.example",
        "AURACLAW_CREDENTIAL_VAULT_TOKEN=test-vault-secret",
        "SEAWEEDFS_HOST=seaweed.example",
        "SEAWEEDFS_ACCESS_KEY=test-access",
        "SEAWEEDFS_SECRET_KEY=test-secret",
        "AURACLAW_CHAINTOWER_WORKLOAD_TOKEN=ct-" + "t" * 40,
        'AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON={"k1":"chaintower-agent-context-signing-key-01"}',
    ]
    lines.extend(
        f"{variable}=postgresql://{role}:secret@db/auraclaw"
        for variable, role in roles.items()
    )
    lines.extend(
        f"AURACLAW_{name}_WORKLOAD_TOKEN={index:02d}-" + "t" * 40
        for index, name in enumerate(tokens)
    )
    env_file = tmp_path / ".env.prod"
    secret_dir = tmp_path / "secrets"
    lines.append(f"AURACLAW_SECRET_DIR={secret_dir}")
    env_file.write_text("\n".join(lines))
    materialized = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/materialize_compose_secrets.py"),
            "--env-file",
            str(env_file),
            "--output-dir",
            str(secret_dir),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert materialized.returncode == 0, materialized.stdout + materialized.stderr
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/compose_preflight.py"),
            "--env-file",
            str(env_file),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "production Compose preflight passed"
