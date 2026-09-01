from __future__ import annotations

import asyncio
from typing import Any

import pytest

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.internal import (
    InternalRequestContext,
    ModelGenerateRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.observability.stores import InMemoryObservabilityStore
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.model_gateway.ports import ModelCallReservation
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.ports import ModelResponse, ModelStreamChunk


class _StreamingModel:
    async def generate_stream(self, request: Any):
        del request
        yield ModelStreamChunk(kind="delta", delta="hi")
        yield ModelStreamChunk(
            kind="completed",
            response=ModelResponse(
                model_call_id="mdl_1",
                provider="test",
                model="test",
                completed_output="hi",
                deltas=("hi",),
                tool_calls=(),
                finish_reason="stop",
                usage={"input_tokens": 1, "output_tokens": 1},
            ),
        )


class _DenyPolicy:
    async def evaluate_action(self, **kwargs: Any) -> PolicyEvaluation:
        del kwargs
        await asyncio.sleep(0.01)
        return PolicyEvaluation(
            decision=PolicyDecision.DENY,
            decision_id="deny-1",
            policy_version="test",
        )


class _AllowPolicy:
    async def evaluate_action(self, **kwargs: Any) -> PolicyEvaluation:
        del kwargs
        await asyncio.sleep(0.01)
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="allow-1",
            policy_version="test",
        )


class _State:
    def __init__(self) -> None:
        self.reserved = False
        self.failed: str | None = None

    async def reserve(self, **kwargs: Any) -> ModelCallReservation:
        del kwargs
        await asyncio.sleep(0.01)
        self.reserved = True
        return ModelCallReservation(status="reserved")

    async def fail(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        error_code: str,
        claim_token: str | None = None,
    ) -> None:
        del tenant_id, model_call_id, claim_token
        self.failed = error_code

    async def complete(self, **kwargs: Any) -> None:
        del kwargs


def _request() -> ModelGenerateRequest:
    return ModelGenerateRequest(
        context=InternalRequestContext(
            tenant_id="t1",
            service_identity=ServiceIdentity.AGENT_RUNTIME,
            request_id="req_1",
            correlation_id="run_1",
            causation_id="req_1",
        ),
        model_call_id="mdl_1",
        run_id="run_1",
        session_id="session_1",
        messages=[{"role": "user", "content": "hi"}],
        tools=(),
        capability="general",
        preferred_model=None,
        allowed_providers=(),
        data_classification="internal",
        max_output_tokens=64,
    )


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="t1",
        root_session_id="session_1",
        session_id="session_1",
        run_id="run_1",
        runtime_id="runtime_1",
        lease_id="lease_1",
        fencing_token=1,
        role="root",
        resource_profile={},
    )


@pytest.mark.asyncio
async def test_policy_deny_releases_parallel_reservation() -> None:
    state = _State()
    service = ModelGatewayInternalService(
        _StreamingModel(),
        policy=_DenyPolicy(),
        state=state,
    )
    with pytest.raises(PolicyDeniedError):
        async for _ in service.generate_stream(_request()):
            pass
    assert state.reserved is True
    assert state.failed == "PolicyDeniedError"


@pytest.mark.asyncio
async def test_policy_and_reserve_run_concurrently() -> None:
    state = _State()
    service = ModelGatewayInternalService(
        _StreamingModel(),
        policy=_AllowPolicy(),
        state=state,
    )
    started = asyncio.get_running_loop().time()
    events = [event async for event in service.generate_stream(_request())]
    elapsed = asyncio.get_running_loop().time() - started
    assert any(event.type == "delta" for event in events)
    assert state.reserved is True
    # Serial would be ~0.02s+; concurrent should finish closer to one sleep.
    assert elapsed < 0.035


@pytest.mark.asyncio
async def test_model_gateway_records_runtime_skill_metrics_and_ttft() -> None:
    metrics = InMemoryObservabilityStore()
    request = _request().model_copy(
        update={
            "runtime_metrics": {
                "skill.runtime.active.count": 2.0,
                "skill.runtime.prompt.bytes": 2048.0,
                "not.allowed": 99.0,
            }
        }
    )
    service = ModelGatewayInternalService(_StreamingModel(), metric_writer=metrics)

    events = [event async for event in service.generate_stream(request)]
    snapshot = await metrics.metric_snapshot()
    by_name = {point.name: point for point in snapshot}

    assert any(event.type == "delta" for event in events)
    assert by_name["skill.runtime.active.count"].value == 2.0
    assert by_name["skill.runtime.prompt.bytes"].value == 2048.0
    assert by_name["model.ttft.seconds"].value >= 0
    assert by_name["model.ttft.seconds"].session_id == "session_1"
    assert "not.allowed" not in by_name


def test_runtime_metrics_do_not_change_model_request_idempotency_digest() -> None:
    first = _request().model_copy(
        update={"runtime_metrics": {"skill.runtime.prompt.bytes": 1024.0}}
    )
    retried = _request().model_copy(
        update={"runtime_metrics": {"skill.runtime.prompt.bytes": 2048.0}}
    )

    assert ModelGatewayInternalService._request_digest(
        first
    ) == ModelGatewayInternalService._request_digest(retried)


@pytest.mark.asyncio
async def test_trusted_messages_loads_skills_in_parallel() -> None:
    class _Client:
        def __init__(self) -> None:
            self.calls = 0
            self.max_inflight = 0
            self._inflight = 0

        async def load_skill_part(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            del args, kwargs
            self.calls += 1
            self._inflight += 1
            self.max_inflight = max(self.max_inflight, self._inflight)
            await asyncio.sleep(0.02)
            self._inflight -= 1
            return [{"text": "skill body"}]

    client = _Client()
    controller = RuntimeCapabilityController(client)  # type: ignore[arg-type]
    assignment = _assignment()
    state = {
        "active_skills": [
            {
                "binding": {
                    "publisher": "auraclaw",
                    "skill_name": "a",
                    "skill_version": "1.0.0",
                    "package_digest": "d1",
                    "resolved_skills": (),
                }
            },
            {
                "binding": {
                    "publisher": "auraclaw",
                    "skill_name": "b",
                    "skill_version": "1.0.0",
                    "package_digest": "d2",
                    "resolved_skills": (),
                }
            },
        ]
    }
    started = asyncio.get_running_loop().time()
    messages = await controller.trusted_messages(assignment, state)
    elapsed = asyncio.get_running_loop().time() - started
    assert len(messages) == 2
    assert client.calls == 2
    assert client.max_inflight == 2
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_run_skill_content_cache_reuses_body_but_not_binding_disposition() -> None:
    class _Client:
        def __init__(self) -> None:
            self.content_calls = 0
            self.disposition_calls = 0

        async def load_skill_part(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            del args, kwargs
            self.content_calls += 1
            return [{"text": "skill body"}]

        async def execute(self, assignment: Any, call: Any) -> dict[str, Any]:
            del assignment, call
            self.disposition_calls += 1
            return {"action": "continue"}

    client = _Client()
    controller = RuntimeCapabilityController(client)  # type: ignore[arg-type]
    assignment = _assignment()
    binding = {
        "publisher": "auraclaw",
        "skill_name": "a",
        "skill_version": "1.0.0",
        "package_digest": "d1",
        "resolved_skills": (),
    }
    state = {
        "active_skills": [
            {
                "binding": binding,
                "activation": {
                    "skill_activation_id": "activation_1",
                    "binding": binding,
                },
            }
        ]
    }

    for _ in range(2):
        disposition = await controller.binding_disposition(assignment, state)
        assert disposition is not None and disposition["action"] == "continue"
        assert await controller.trusted_messages(assignment, state)

    assert client.disposition_calls == 2
    assert client.content_calls == 1
    assert controller.trusted_message_metrics(assignment)[
        "skill.runtime.content_cache.hit.count"
    ] == 1.0

    await controller.release_run(assignment)
    assert controller.trusted_message_metrics(assignment) == {}
    assert await controller.trusted_messages(assignment, state)
    assert client.content_calls == 2


@pytest.mark.asyncio
async def test_release_run_cancels_inflight_skill_content_load() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class _Client:
        async def load_skill_part(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            del args, kwargs
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    controller = RuntimeCapabilityController(_Client())  # type: ignore[arg-type]
    assignment = _assignment()
    state = {
        "active_skills": [
            {
                "binding": {
                    "publisher": "auraclaw",
                    "skill_name": "a",
                    "skill_version": "1.0.0",
                    "package_digest": "d1",
                    "resolved_skills": (),
                }
            }
        ]
    }
    loading = asyncio.create_task(controller.trusted_messages(assignment, state))
    await started.wait()

    await controller.release_run(assignment)

    with pytest.raises(asyncio.CancelledError):
        await loading
    assert cancelled.is_set()
    assert controller.trusted_message_metrics(assignment) == {}


@pytest.mark.asyncio
async def test_claim_outbox_waits_for_arrival() -> None:
    from datetime import timedelta

    from auraclaw.contracts.commands import CommandContext
    from auraclaw.contracts.events import Actor, NewEvent
    from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore

    store = InMemoryEventStore()

    async def publish_soon() -> None:
        await asyncio.sleep(0.08)
        await store.append(
            root_session_id="s1",
            session_id="s1",
            run_id="run_1",
            context=CommandContext(
                command_id="cmd_1",
                tenant_id="t1",
                actor=Actor(type="user", id="u1"),
                correlation_id="run_1",
                expected_version=0,
                operation="create",
            ),
            events=(
                NewEvent(type="session.created", payload={"goal": "hi"}),
                NewEvent(type="run.requested", payload={"run_id": "run_1"}),
            ),
            command_result={},
        )

    publisher = asyncio.create_task(publish_soon())
    started = asyncio.get_running_loop().time()
    claimed = await store.claim_outbox(
        "control",
        "worker-1",
        limit=10,
        claim_ttl=timedelta(seconds=30),
        wait_seconds=0.5,
    )
    elapsed = asyncio.get_running_loop().time() - started
    await publisher
    assert claimed
    assert elapsed >= 0.05
    assert elapsed < 0.35


@pytest.mark.asyncio
async def test_openai_compatible_prewarm_creates_client() -> None:
    from unittest.mock import AsyncMock, MagicMock, patch

    import httpx

    from auraclaw.infrastructure.model.openai_compatible import OpenAICompatibleProvider

    provider = OpenAICompatibleProvider(
        base_url="http://provider.example/v1",
        model="test-model",
        name="test",
    )
    mock_response = MagicMock()
    mock_response.is_error = False
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.aclose = AsyncMock()

    with patch.object(httpx, "AsyncClient", return_value=mock_client):
        await provider.prewarm()
    mock_client.get.assert_awaited()
    assert provider._client is mock_client
    await provider.aclose()
