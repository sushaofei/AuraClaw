from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from auraclaw.composition.api import create_app
from auraclaw.composition.cli import build_parser
from auraclaw.composition.services import (
    SERVICE_BY_COMMAND,
    create_service_app,
    service_spec,
)
from auraclaw.config import Settings
from auraclaw.contracts.mcp import MCP_PROTOCOL_VERSION

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

    for command in expected:
        argv = (
            ["projection", "relay", "--watch"]
            if command == "projection"
            else [command, "run"]
        )
        parsed = parser.parse_args(argv)
        assert parsed.command == command


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
    assert not any(path.startswith("/v1/streams") for path in task_paths)
    assert any(path.startswith("/v1/streams") for path in stream_paths)
    assert "/v1/tasks" not in stream_paths


def test_each_service_exposes_health_and_workers_stop_gracefully() -> None:
    settings = _settings(storage_backend="memory", artifact_backend="local")
    for command in SERVICE_BY_COMMAND:
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


def test_hands_exposes_authenticated_mcp_initialize() -> None:
    app = create_service_app(
        "hands",
        _settings(storage_backend="memory", artifact_backend="local"),
    )
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {"protocolVersion": MCP_PROTOCOL_VERSION},
    }
    with TestClient(app) as client:
        denied = client.post("/mcp", json=request)
        assert denied.status_code == 401
        initialized = client.post(
            "/mcp",
            json=request,
            headers={"Authorization": "Bearer development-runtime-token"},
        )
        assert initialized.status_code == 200
        assert initialized.json()["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION


def test_compose_uses_one_image_twelve_commands_and_ingress_split() -> None:
    compose = (ROOT / "compose.services.yml").read_text()
    assert compose.count("<<: *auraclaw-service") == 12
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
