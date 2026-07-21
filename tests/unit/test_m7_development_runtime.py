import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from auraclaw.composition import providers
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
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.ports import ModelRequest, ModelResponse


class DeterministicModelClient:
    async def generate(self, request: ModelRequest) -> ModelResponse:
        prompt = str(request.messages[-1].get("content", ""))
        output = f"统一 Runtime 已收到问题：{prompt}。这是 Streaming 与 Result 一致性回答。"
        deltas = tuple(output[index : index + 8] for index in range(0, len(output), 8))
        return ModelResponse(
            model_call_id=request.model_call_id,
            provider="test",
            model="deterministic",
            completed_output=output,
            deltas=deltas,
            usage={"input_tokens": 1, "output_tokens": len(deltas)},
        )


def _clear_dependencies() -> None:
    for dependency in (
        get_task_service,
        get_event_store,
        get_task_projection,
        get_approval_projection,
        get_collaboration_projection,
        get_runtime_replay_bus,
        get_runtime_event_producer,
        providers.get_runtime_event_publisher,
        get_streaming_ingestor,
        get_streaming_gateway,
        providers.get_model_gateway,
        providers.get_control_store,
        get_observability_store,
        get_observability_service,
    ):
        cache_clear = getattr(dependency, "cache_clear", None)
        if cache_clear is not None:
            cache_clear()


def test_local_frontend_origin_passes_cors_preflight() -> None:
    settings = get_settings()
    previous = (settings.runtime_enabled, settings.cors_allow_origins)
    settings.runtime_enabled = False
    settings.cors_allow_origins = "http://127.0.0.1:3000"
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
        settings.runtime_enabled, settings.cors_allow_origins = previous


def test_runtime_logic_is_independent_of_resource_backends() -> None:
    settings = Settings(
        _env_file=None,
        storage_backend="postgres",
        runtime_event_backend="kafka",
    )
    assert settings.postgres_enabled is True
    assert settings.kafka_enabled is True
    assert settings.runtime_enabled is True


def test_unified_runtime_supports_three_runs_in_one_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    previous = {
        "storage_backend": settings.storage_backend,
        "runtime_event_backend": settings.runtime_event_backend,
        "runtime_enabled": settings.runtime_enabled,
        "runtime_poll_interval": settings.runtime_poll_interval,
        "model_api_key": settings.model_api_key,
        "model_base_url": settings.model_base_url,
        "model_name": settings.model_name,
    }
    settings.storage_backend = "memory"
    settings.runtime_event_backend = "memory"
    settings.runtime_enabled = True
    settings.runtime_poll_interval = 0.01
    settings.model_api_key = "test-secret"
    settings.model_base_url = "https://models.example/v1"
    settings.model_name = "test-model"
    _clear_dependencies()
    monkeypatch.setattr(providers, "get_model_gateway", lambda: DeterministicModelClient())
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
            run_ids = [created.json()["run_id"]]

            def wait_for_run(run_id: str) -> dict[str, object]:
                deadline = time.monotonic() + 2
                task: dict[str, object] = {}
                while time.monotonic() < deadline:
                    task = client.get(
                        f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-m7"}
                    ).json()
                    if task["run_id"] == run_id and task["run_status"] == "completed":
                        return task
                    time.sleep(0.01)
                raise AssertionError(f"Run did not complete: {run_id}; last task={task}")

            task = wait_for_run(run_ids[0])
            for index, message in enumerate(("请继续说明", "最后总结一下"), start=2):
                appended = client.post(
                    f"/v1/sessions/{session_id}/messages",
                    headers={
                        "Idempotency-Key": f"m7-message-{index}",
                        "X-Tenant-ID": "tenant-m7",
                        "X-Expected-Version": str(task["projection_version"]),
                    },
                    json={"message": message},
                )
                assert appended.status_code == 202
                after_message = client.get(
                    f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-m7"}
                ).json()
                requested = client.post(
                    f"/v1/sessions/{session_id}/runs",
                    headers={
                        "Idempotency-Key": f"m7-run-{index}",
                        "X-Tenant-ID": "tenant-m7",
                        "X-Expected-Version": str(after_message["projection_version"]),
                    },
                )
                assert requested.status_code == 202
                assert requested.json()["session_id"] == session_id
                run_ids.append(requested.json()["run_id"])
                task = wait_for_run(run_ids[-1])

            assert task["status"] == "ready"
            assert len(set(run_ids)) == 3
            result = client.get(
                f"/v1/tasks/{session_id}/result",
                headers={"X-Tenant-ID": "tenant-m7"},
            )
            assert result.status_code == 200
            events = asyncio.run(get_runtime_replay_bus().events("tenant-m7", session_id))
            deltas = [
                event.payload["delta"]
                for event in events
                if event.type == "model.output.delta" and event.run_id == run_ids[-1]
            ]
            assert len(deltas) > 1
            assert "".join(deltas) == result.json()["result_summary"]
            assert result.json()["run_id"] == run_ids[-1]
            assert result.json()["status"] == "completed"
            assert result.json()["session_status"] == "ready"
            sequences = [event.sequence for event in events]
            assert sequences == sorted(set(sequences))

            canonical = asyncio.run(get_event_store().load("tenant-m7", session_id))
            messages = AgentHarness._build_messages(canonical)
            assert [message["role"] for message in messages] == [
                "user",
                "assistant",
                "user",
                "assistant",
                "user",
                "assistant",
            ]
    finally:
        for key, value in previous.items():
            setattr(settings, key, value)
        _clear_dependencies()
