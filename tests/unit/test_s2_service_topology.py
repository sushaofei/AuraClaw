from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from auraclaw.composition.api import create_app
from auraclaw.composition.cli import _serve_topology, build_parser, main
from auraclaw.composition.services import (
    SERVICE_BY_COMMAND,
    create_service_app,
    service_spec,
)
from auraclaw.config import RUNTIME_POOL_ROLE, Settings, get_settings
from auraclaw.contracts.hands import HANDS_TOOLS_LIST
from auraclaw.contracts.internal import LeaseAssertion
from auraclaw.internal.security import LeaseAssertionSigner

ROOT = Path(__file__).resolve().parents[2]


def _settings(**values: object) -> Settings:
    return Settings(_env_file=None, **values)


def test_cli_defines_all_twelve_production_entrypoints() -> None:
    parser = build_parser()
    expected = {
        "api": "task-api",
        "session": "session",
        "projection": "projection-worker",
        "orchestrator": "orchestrator",
        "runtime": "agent-runtime",
        "model-gateway": "model-gateway",
        "hands": "action-hands",
        "policy": "policy",
        "credential-proxy": "credential-proxy",
        "artifact": "artifact-service",
        "streaming": "streaming-gateway",
        "delivery": "delivery-worker",
    }
    assert SERVICE_BY_COMMAND == expected
    settings = _settings()
    assert len({service_spec(command, settings).port for command in expected}) == 12
    assert settings.ingress_port == 8080
    assert settings.ingress_enabled is True

    assert parser.parse_args(["serve"]).command == "serve"
    assert parser.parse_args(["projection", "relay"]).command == "projection"
    assert parser.parse_args(["operations", "status"]).command == "operations"
    assert parser.parse_args(["migrate", "status"]).command == "migrate"


def test_serve_starts_all_twelve_production_entrypoints() -> None:
    started: list[str] = []
    uvicorn_calls: list[object] = []

    def fake_serve(settings: Settings, *, host: str) -> None:
        del settings
        assert host == "127.0.0.1"
        started.extend(SERVICE_BY_COMMAND)

    main(
        ["serve", "--host", "127.0.0.1"],
        uvicorn_runner=lambda *args, **kwargs: uvicorn_calls.append((args, kwargs)),
        serve_runner=fake_serve,
    )
    assert started == list(SERVICE_BY_COMMAND)
    assert uvicorn_calls == []


def test_runtime_pool_role_must_register_agent_pool() -> None:
    with pytest.raises(ValueError, match="AURACLAW_RUNTIME_ROLE must be 'agent'"):
        _settings(runtime_role="root")

    settings = _settings(runtime_role=RUNTIME_POOL_ROLE)
    assert settings.runtime_role == RUNTIME_POOL_ROLE


def test_single_service_run_rejected_in_development_profile() -> None:
    with pytest.raises(SystemExit, match="auraclaw serve"):
        main(["runtime", "run"], uvicorn_runner=lambda *_a, **_k: None)


def test_single_service_run_allowed_in_production_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AURACLAW_DEPLOYMENT_PROFILE", "production")
    get_settings.cache_clear()
    calls: list[tuple[object, ...]] = []

    def fake_uvicorn(*args: object, **kwargs: object) -> None:
        calls.append((args, kwargs))

    try:
        main(["runtime", "run"], uvicorn_runner=fake_uvicorn)
    finally:
        get_settings.cache_clear()

    assert len(calls) == 1


def test_serve_rejects_process_local_runtime_event_bus() -> None:
    settings = _settings(
        storage_backend="memory",
        runtime_event_backend="memory",
    )

    with pytest.raises(ValueError, match="requires SQL storage"):
        _serve_topology(settings, host="127.0.0.1")


def test_serve_rejects_sql_without_kafka() -> None:
    settings = _settings(
        storage_backend="postgres",
        db_host="localhost",
        db_user="auraclaw",
        db_password="auraclaw",
        db_name="auraclaw",
        runtime_event_backend="memory",
    )

    with pytest.raises(ValueError, match="requires Kafka"):
        _serve_topology(settings, host="127.0.0.1")


def test_projection_watch_interval_reaches_worker_lifecycle() -> None:
    app = create_service_app(
        "projection",
        _settings(storage_backend="memory"),
        worker_interval=2.5,
    )
    assert app.state.worker_interval == 2.5


def test_task_api_and_streaming_gateway_have_disjoint_public_routes() -> None:
    task_paths = set(create_app(profile="task-api").openapi()["paths"])
    stream_paths = set(create_app(profile="streaming-gateway").openapi()["paths"])
    assert "/v1/tasks" in task_paths
    assert "/v1/tasks/sync" in task_paths
    assert not any(path.startswith("/v1/streams") for path in task_paths)
    assert any(path.startswith("/v1/streams") for path in stream_paths)
    assert "/v1/tasks" not in stream_paths


def test_each_service_exposes_health_and_workers_stop_gracefully() -> None:
    settings = _settings(storage_backend="memory", artifact_backend="local")
    for command in SERVICE_BY_COMMAND:
        if command in {"api", "streaming"}:
            continue
        app = create_service_app(command, settings)
        if command == "streaming":
            paths = set(app.openapi()["paths"])
            assert "/health/live" in paths
            assert "/health/ready" in paths
            continue
        with TestClient(app) as client:
            live = client.get("/health/live")
            ready = client.get("/health/ready")
            assert live.status_code == 200, command
            assert ready.status_code in {200, 503}, command
            assert live.json()["status"] == "ok"
            if command not in {"api", "streaming"}:
                assert live.json()["service"] == SERVICE_BY_COMMAND[command]
        if command not in {"api", "streaming"}:
            assert app.state.stopping is True


def test_production_readiness_fails_when_hard_dependencies_are_missing() -> None:
    production = _settings(
        deployment_profile="production",
        storage_backend="memory",
        artifact_backend="local",
    )
    for command in ("session", "artifact", "hands", "model-gateway"):
        app = create_service_app(command, production)
        with TestClient(app) as client:
            response = client.get("/health/ready")
            assert response.status_code == 503
            assert response.json()["status"] == "degraded"
            serialized = json.dumps(response.json())
            assert "secret" not in serialized.lower()
            assert "api_key" not in serialized.lower()


def test_hands_exposes_authenticated_internal_contract() -> None:
    key = b"test-hands-capability-signing-key-0001"
    app = create_service_app(
        "hands",
        _settings(
            storage_backend="memory",
            artifact_backend="local",
            runtime_workload_token="runtime-token",
            lease_signing_key=key.decode(),
        ),
    )
    capability = LeaseAssertionSigner(key_id="development", signing_key=key).sign(
        LeaseAssertion(
            key_id="pending",
            audience="runtime",
            tenant_id="tenant-a",
            root_session_id="root-a",
            session_id="session-a",
            run_id="run-a",
            runtime_id="runtime-a",
            lease_id="lease-a",
            fencing_token=1,
            expires_at=datetime.now(UTC) + timedelta(minutes=1),
            signature="",
        )
    )
    with TestClient(app) as client:
        denied = client.post(HANDS_TOOLS_LIST, json={})
        assert denied.status_code == 401
        listed = client.post(
            HANDS_TOOLS_LIST,
            json={},
            headers={
                "Authorization": "Bearer runtime-token",
                "X-AuraClaw-Lease-Assertion": capability.model_dump_json(),
            },
        )
        assert listed.status_code == 200
        assert "items" in listed.json()


def test_compose_uses_one_image_twelve_commands_and_ingress_split() -> None:
    compose = (ROOT / "compose.test.yml").read_text()
    assert compose.count("<<: *auraclaw-service") == 13  # 12 app + migrate
    for command in (
        '["api", "run"',
        '["session", "run"',
        '["projection", "relay", "--watch"',
        '["orchestrator", "run"',
        '["runtime", "run"',
        '["model-gateway", "run"',
        '["hands", "run"',
        '["policy", "run"',
        '["credential-proxy", "run"',
        '["artifact", "run"',
        '["streaming", "run"',
        '["delivery", "run"',
    ):
        assert command in compose
    ingress = (ROOT / "deploy/nginx.conf").read_text()
    assert "location /v1/streams/" in ingress
    assert "streaming-gateway:8010" in ingress
    assert "task-api:8000" in ingress


def test_container_build_excludes_secrets_and_runs_unprivileged() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    dockerignore = (ROOT / ".dockerignore").read_text().splitlines()
    assert "USER auraclaw" in dockerfile
    assert ".env" in dockerignore
    assert ".venv" in dockerignore
    assert "__pycache__" in dockerignore


def test_session_outbox_projectors_include_approval_and_collaboration() -> None:
    from auraclaw.composition.providers import (
        get_approval_projection,
        get_collaboration_projection,
        get_task_projection,
        session_outbox_projectors,
    )

    writers = session_outbox_projectors()
    assert writers == (
        get_task_projection(),
        get_approval_projection(),
        get_collaboration_projection(),
    )
