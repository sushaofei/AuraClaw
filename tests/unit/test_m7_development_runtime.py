import asyncio
import time

from fastapi.testclient import TestClient

from auraclaw.composition.providers import (
    get_approval_projection,
    get_collaboration_projection,
    get_event_store,
    get_observability_service,
    get_observability_store,
    get_runtime_event_producer,
    get_runtime_replay_bus,
    get_streaming_gateway,
    get_streaming_ingestor,
    get_task_projection,
    get_task_service,
)
from auraclaw.config import Settings, get_settings
from auraclaw.main import create_app


def _clear_dependencies() -> None:
    for dependency in (
        get_task_service,
        get_event_store,
        get_task_projection,
        get_approval_projection,
        get_collaboration_projection,
        get_runtime_replay_bus,
        get_runtime_event_producer,
        get_streaming_ingestor,
        get_streaming_gateway,
        get_observability_store,
        get_observability_service,
    ):
        dependency.cache_clear()


def test_local_frontend_origin_passes_cors_preflight() -> None:
    settings = get_settings()
    previous = settings.development_runtime_enabled
    settings.development_runtime_enabled = False
    try:
        with TestClient(create_app()) as client:
            response = client.options(
                "/v1/tasks",
                headers={
                    "Origin": "http://127.0.0.1:3000",
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "content-type,idempotency-key,x-tenant-id",
                },
            )
        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == "http://127.0.0.1:3000"
        assert "idempotency-key" in response.headers["access-control-allow-headers"].lower()
    finally:
        settings.development_runtime_enabled = previous


def test_development_runtime_supports_external_development_backends() -> None:
    settings = Settings(
        _env_file=None,
        env="development",
        storage_backend="postgres",
        runtime_event_backend="kafka",
    )
    assert settings.development_runtime_active is True

    settings.env = "production"
    assert settings.development_runtime_active is False


def test_development_runtime_completes_task_and_replays_multiple_deltas() -> None:
    settings = get_settings()
    previous = {
        "env": settings.env,
        "storage_backend": settings.storage_backend,
        "runtime_event_backend": settings.runtime_event_backend,
        "development_runtime_enabled": settings.development_runtime_enabled,
        "development_runtime_poll_interval": settings.development_runtime_poll_interval,
        "development_stream_delay": settings.development_stream_delay,
    }
    settings.env = "development"
    settings.storage_backend = "memory"
    settings.runtime_event_backend = "memory"
    settings.development_runtime_enabled = True
    settings.development_runtime_poll_interval = 0.01
    settings.development_stream_delay = 0.001
    _clear_dependencies()
    try:
        with TestClient(create_app()) as client:
            created = client.post(
                "/v1/tasks",
                headers={
                    "Idempotency-Key": "m7-real-stream",
                    "X-Tenant-ID": "tenant-m7",
                    "X-Actor-ID": "tester",
                },
                json={"goal": "请介绍 AuraClaw 架构"},
            )
            assert created.status_code == 202
            session_id = created.json()["session_id"]

            deadline = time.monotonic() + 2
            task = None
            while time.monotonic() < deadline:
                task = client.get(
                    f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-m7"}
                ).json()
                if task["status"] == "completed":
                    break
                time.sleep(0.01)

            assert task is not None and task["status"] == "completed"
            result = client.get(
                f"/v1/tasks/{session_id}/result",
                headers={"X-Tenant-ID": "tenant-m7"},
            )
            assert result.status_code == 200
            events = asyncio.run(get_runtime_replay_bus().events("tenant-m7", session_id))
            deltas = [
                event.payload["delta"]
                for event in events
                if event.type == "model.output.delta"
            ]
            assert len(deltas) > 1
            assert "".join(deltas) == result.json()["result_summary"]
    finally:
        for key, value in previous.items():
            setattr(settings, key, value)
        _clear_dependencies()
