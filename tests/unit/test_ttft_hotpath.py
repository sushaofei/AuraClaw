from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.internal import (
    InternalRequestContext,
    ModelGenerateRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
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

    async def fail(self, *, tenant_id: str, model_call_id: str, error_code: str) -> None:
        del tenant_id, model_call_id
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
        messages=[{"role": "user", "content": "hi"}],
        tools=(),
        capability="general",
        preferred_model=None,
        allowed_providers=(),
        data_classification="internal",
        max_output_tokens=64,
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
    assignment = AsyncMock()
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
