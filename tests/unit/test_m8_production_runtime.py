import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from auraclaw.composition.services import RemoteRuntimeWorker
from auraclaw.config import Settings
from auraclaw.contracts.errors import (
    ModelAuthenticationError,
    ModelProviderError,
    ModelRateLimitError,
    ModelTimeoutError,
)
from auraclaw.infrastructure.model import OpenAICompatibleProvider
from auraclaw.runtime.ports import ModelRequest


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
    # Process env (e.g. CI release-gate / local .env) must not shadow named files.
    for name in (
        "AURACLAW_STORAGE_BACKEND",
        "AURACLAW_RUNTIME_EVENT_BACKEND",
        "AURACLAW_DB_DIALECT",
        "AURACLAW_DATABASE_URL",
        "DB_NAME",
        "DB_HOST",
        "DB_USER",
        "DB_PWD",
        "DB_PORT",
    ):
        monkeypatch.delenv(name, raising=False)

    development = tmp_path / ".env.development"
    production = tmp_path / ".env.prod"
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
    ("status", "error", "body"),
    [
        (401, ModelAuthenticationError, b""),
        (403, ModelAuthenticationError, b""),
        (429, ModelRateLimitError, b""),
        (
            400,
            ModelProviderError,
            (
                b'{"error":{"message":"The request parameter messages is '
                b'invalid or missing.","code":"400002"}}'
            ),
        ),
    ],
)
def test_openai_compatible_provider_maps_status_errors(
    status: int, error: type[Exception], body: bytes
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=body)

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleProvider(
            base_url="https://models.example/v1", model="model", client=client
        )
        with pytest.raises(error) as raised:
            await provider.generate(
                ModelRequest(
                    model_call_id="model-error",
                    tenant_id="tenant-m8",
                    run_id="run-m8",
                    messages=(),
                ),
                credential="secret",
            )
        if status == 400:
            assert "400002" in str(raised.value)
            assert "messages" in str(raised.value)
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
