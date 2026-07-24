import asyncio
import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from auraclaw.composition import providers
from auraclaw.composition.services import RemoteRuntimeWorker
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.errors import (
    ModelAuthenticationError,
    ModelRateLimitError,
    ModelTimeoutError,
)
from auraclaw.infrastructure.model import OpenAICompatibleProvider
from auraclaw.main import create_app
from auraclaw.runtime.ports import ModelRequest, ModelResponse


class StreamingModelClient:
    async def generate(self, request: ModelRequest) -> ModelResponse:
        return ModelResponse(
            model_call_id=request.model_call_id,
            provider="openai_compatible",
            model="test-model",
            completed_output="production answer",
            deltas=("production ", "answer"),
            usage={"input_tokens": 2, "output_tokens": 2, "total_tokens": 4},
        )


def test_remote_runtime_records_canonical_failure_before_acking_assignment() -> None:
    class Assignment:
        tenant_id = "tenant-m8"
        session_id = "session-m8"
        run_id = "run-m8"

    class Control:
        def __init__(self) -> None:
            self.finished: list[tuple[str, str]] = []

        async def register(self) -> None:
            return None

        async def heartbeat(self) -> None:
            return None

        async def claim(self, *, limit: int) -> list[Assignment]:
            assert limit == 1
            return [Assignment()]

        async def finish_assignment(self, task_id: str, outcome: str) -> None:
            self.finished.append((task_id, outcome))

    class Harness:
        def __init__(self) -> None:
            self.failure_recorded = False

        async def execute(self, assignment: Assignment) -> None:
            del assignment
            raise RuntimeError("provider unavailable")

        async def record_failure(
            self, assignment: Assignment, error: Exception
        ) -> None:
            assert assignment.run_id == "run-m8"
            assert isinstance(error, RuntimeError)
            self.failure_recorded = True

    async def scenario() -> None:
        control = Control()
        harness = Harness()
        worker = RemoteRuntimeWorker(control, harness)  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match="provider unavailable"):
            await worker.tick()
        assert harness.failure_recorded is True
        assert control.finished == [
            ("tenant-m8:session-m8:run-m8", "failed")
        ]

    asyncio.run(scenario())


def test_remote_runtime_does_not_ack_when_canonical_failure_cannot_be_written() -> None:
    class Assignment:
        tenant_id = "tenant-m8"
        session_id = "session-m8"
        run_id = "run-m8"

    class Control:
        def __init__(self) -> None:
            self.finished = False

        async def register(self) -> None:
            return None

        async def claim(self, *, limit: int) -> list[Assignment]:
            del limit
            return [Assignment()]

        async def finish_assignment(self, task_id: str, outcome: str) -> None:
            del task_id, outcome
            self.finished = True

    class Harness:
        async def execute(self, assignment: Assignment) -> None:
            del assignment
            raise RuntimeError("provider unavailable")

        async def record_failure(
            self, assignment: Assignment, error: Exception
        ) -> None:
            del assignment, error
            raise ConnectionError("session unavailable")

    async def scenario() -> None:
        control = Control()
        worker = RemoteRuntimeWorker(control, Harness())  # type: ignore[arg-type]
        with pytest.raises(ConnectionError, match="session unavailable"):
            await worker.tick()
        assert control.finished is False

    asyncio.run(scenario())


def _clear_dependencies() -> None:
    for dependency in (
        providers.get_event_store,
        providers.get_task_projection,
        providers.get_approval_projection,
        providers.get_collaboration_projection,
        providers.get_task_service,
        providers.get_runtime_replay_bus,
        providers.get_runtime_event_producer,
        providers.get_runtime_event_publisher,
        providers.get_streaming_ingestor,
        providers.get_streaming_gateway,
        providers.get_model_gateway,
        providers.get_control_store,
        providers.get_observability_store,
        providers.get_observability_service,
    ):
        cache_clear = getattr(dependency, "cache_clear", None)
        if cache_clear is not None:
            cache_clear()


def test_settings_only_accept_provider_neutral_model_names(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "AURACLAW_MODEL_API_KEY",
        "AURACLAW_MODEL_BASE_URL",
        "AURACLAW_MODEL_NAME",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HUNYUAN" + "_API_KEY", "legacy-secret")
    legacy = Settings(_env_file=None)
    assert legacy.model_api_key is None
    assert legacy.model_gateway_configured is False

    monkeypatch.setenv("AURACLAW_MODEL_API_KEY", "neutral-secret")
    monkeypatch.setenv("AURACLAW_MODEL_BASE_URL", "https://models.example/v1")
    monkeypatch.setenv("AURACLAW_MODEL_NAME", "example-model")
    configured = Settings(_env_file=None)
    assert configured.model_gateway_configured is True
    assert configured.model_provider == "openai_compatible"


def test_named_env_files_select_resources_without_environment_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Process env (e.g. CI release-gate) must not shadow the named env files under test.
    monkeypatch.delenv("AURACLAW_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("AURACLAW_RUNTIME_EVENT_BACKEND", raising=False)

    development = tmp_path / ".env.development"
    production = tmp_path / ".env.production"
    development.write_text(
        "DB_NAME=auraclaw_development\nAURACLAW_STORAGE_BACKEND=memory\n"
    )
    production.write_text(
        "DB_NAME=auraclaw_production\nAURACLAW_STORAGE_BACKEND=postgres\n"
    )

    development_settings = Settings(_env_file=development)
    production_settings = Settings(_env_file=production)
    assert not hasattr(development_settings, "env")
    assert development_settings.db_name == "auraclaw_development"
    assert production_settings.db_name == "auraclaw_production"
    assert development_settings.postgres_enabled is False
    assert production_settings.postgres_enabled is True


def test_seaweedfs_settings_resolve_endpoints_and_auto_enable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "SEAWEEDFS_HOST",
        "SEAWEEDFS_MASTER_PORT",
        "SEAWEEDFS_FILER_PORT",
        "SEAWEEDFS_S3_PORT",
        "SEAWEEDFS_ACCESS_KEY",
        "SEAWEEDFS_SECRET_KEY",
        "SEAWEEDFS_BUCKET",
        "SEAWEEDFS_USE_SSL",
        "SEAWEEDFS_PATH_STYLE",
        "AURACLAW_ARTIFACT_BACKEND",
    ):
        monkeypatch.delenv(name, raising=False)

    local_only = Settings(_env_file=None)
    assert local_only.seaweedfs_enabled is False
    assert local_only.seaweedfs_s3_endpoint == "http://127.0.0.1:8333"

    monkeypatch.setenv("SEAWEEDFS_HOST", "seaweed.example")
    monkeypatch.setenv("SEAWEEDFS_ACCESS_KEY", "ak")
    monkeypatch.setenv("SEAWEEDFS_SECRET_KEY", "sk")
    monkeypatch.setenv("SEAWEEDFS_BUCKET", "auraclaw-dev")
    auto = Settings(_env_file=None)
    assert auto.seaweedfs_enabled is True
    assert auto.seaweedfs_master == "seaweed.example:9333"
    assert auto.seaweedfs_filer_url == "http://seaweed.example:8888"
    assert auto.seaweedfs_s3_endpoint == "http://seaweed.example:8333"
    assert auto.seaweedfs_bucket == "auraclaw-dev"
    assert auto.seaweedfs_path_style is True
    assert "ak" not in repr(auto.seaweedfs_access_key)
    assert "sk" not in repr(auto.seaweedfs_secret_key)

    monkeypatch.setenv("AURACLAW_ARTIFACT_BACKEND", "local")
    forced_local = Settings(_env_file=None)
    assert forced_local.seaweedfs_enabled is False

    monkeypatch.setenv("AURACLAW_ARTIFACT_BACKEND", "seaweedfs")
    monkeypatch.delenv("SEAWEEDFS_SECRET_KEY")
    with pytest.raises(ValueError, match="SEAWEEDFS_SECRET_KEY"):
        Settings(_env_file=None)


def test_openai_compatible_provider_streams_usage_and_tools() -> None:
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["Authorization"]
        captured["body"] = json.loads(request.content)
        chunks = [
            {"choices": [{"delta": {"content": "managed "}}]},
            {
                "choices": [
                    {
                        "delta": {
                            "content": "answer",
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-1",
                                    "function": {
                                        "name": "lookup",
                                        "arguments": '{"query":',
                                    },
                                }
                            ],
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {"index": 0, "function": {"arguments": '"state"}'}}
                            ]
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                },
            },
        ]
        body = "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks)
        return httpx.Response(200, text=f"{body}data: [DONE]\n\n")

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleProvider(
            base_url="https://models.example/v1/",
            model="example-model",
            client=client,
        )
        response = await provider.generate(
            ModelRequest(
                model_call_id="model-1",
                tenant_id="tenant-m8",
                run_id="run-m8",
                messages=({"role": "user", "content": "hello"},),
                max_output_tokens=32,
            ),
            credential="gateway-only-secret",
        )
        await client.aclose()
        assert response.completed_output == "managed answer"
        assert response.deltas == ("managed ", "answer")
        assert response.usage == {
            "input_tokens": 3,
            "output_tokens": 2,
            "total_tokens": 5,
        }
        assert response.tool_calls[0].arguments == {"query": "state"}
        assert captured["authorization"] == "Bearer gateway-only-secret"
        assert captured["body"]["stream"] is True
        assert captured["body"]["max_tokens"] == 32
        assert "thinking" not in captured["body"]

    asyncio.run(scenario())


def test_openai_compatible_provider_sends_thinking_disabled() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        chunk = {
            "choices": [{"delta": {"content": "ok"}, "finish_reason": "stop"}],
        }
        body = f"data: {json.dumps(chunk)}\n\ndata: [DONE]\n\n"
        return httpx.Response(200, text=body)

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleProvider(
            base_url="https://tokenhub.example/v1",
            model="glm-5.2",
            thinking_enabled=False,
            client=client,
        )
        response = await provider.generate(
            ModelRequest(
                model_call_id="model-think",
                tenant_id="tenant-m8",
                run_id="run-m8",
                messages=({"role": "user", "content": "hi"},),
            ),
            credential="secret",
        )
        await client.aclose()
        assert response.completed_output == "ok"
        assert captured["body"]["thinking"] == {"type": "disabled"}

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "error"),
    [(401, ModelAuthenticationError), (403, ModelAuthenticationError), (429, ModelRateLimitError)],
)
def test_openai_compatible_provider_maps_status_errors(
    status: int, error: type[Exception]
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleProvider(
            base_url="https://models.example/v1", model="model", client=client
        )
        with pytest.raises(error):
            await provider.generate(
                ModelRequest(
                    model_call_id="model-error",
                    tenant_id="tenant-m8",
                    run_id="run-m8",
                    messages=(),
                ),
                credential="secret",
            )
        await client.aclose()

    asyncio.run(scenario())


def test_openai_compatible_provider_maps_timeout() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleProvider(
            base_url="https://models.example/v1", model="model", client=client
        )
        with pytest.raises(ModelTimeoutError):
            await provider.generate(
                ModelRequest(
                    model_call_id="model-timeout",
                    tenant_id="tenant-m8",
                    run_id="run-m8",
                    messages=(),
                ),
                credential="secret",
            )
        await client.aclose()

    asyncio.run(scenario())


def test_unified_worker_completes_task_and_publishes_stream(
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
    settings.model_api_key = "production-secret"
    settings.model_base_url = "https://models.example/v1"
    settings.model_name = "test-model"
    _clear_dependencies()
    monkeypatch.setattr(providers, "get_model_gateway", lambda: StreamingModelClient())
    try:
        with TestClient(create_app()) as client:
            health = client.get("/health/ready").json()
            assert health["status"] == "ready"
            assert health["model_gateway_ready"] is True
            assert health["runtime_worker"] == "running"
            assert health["runtime_event_producer_ready"] is True
            assert health["runtime_event_ingestor_ready"] is True

            created = client.post(
                "/v1/tasks",
                headers={"Idempotency-Key": "m8-production", "X-Tenant-ID": "tenant-m8"},
                json={"goal": "exercise production runtime"},
            )
            assert created.status_code == 202
            session_id = created.json()["session_id"]
            deadline = time.monotonic() + 2
            task: dict[str, Any] = {}
            while time.monotonic() < deadline:
                task = client.get(
                    f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-m8"}
                ).json()
                if task.get("run_status") == "completed":
                    break
                time.sleep(0.01)
            assert task["run_status"] == "completed"
            result = client.get(
                f"/v1/tasks/{session_id}/result",
                headers={"X-Tenant-ID": "tenant-m8"},
            )
            assert result.json()["result_summary"] == "production answer"
            events = asyncio.run(
                providers.get_runtime_replay_bus().events("tenant-m8", session_id)
            )
            assert [event.payload["delta"] for event in events] == [
                "production ",
                "answer",
            ]
            assert "production-secret" not in repr(events)
    finally:
        for key, value in previous.items():
            setattr(settings, key, value)
        _clear_dependencies()
