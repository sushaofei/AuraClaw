from __future__ import annotations

import asyncio
from typing import Any

import pytest

from auraclaw.contracts.internal import (
    InternalRequestContext,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ModelStreamEvent,
    ServiceIdentity,
)
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.model_gateway.ports import ModelCallReservation
from auraclaw.runtime.ports import ModelPolicy, ModelRequest, ModelResponse, ModelStreamChunk


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


class _OrderedState:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.complete_started = asyncio.Event()
        self.allow_complete = asyncio.Event()

    async def reserve(self, **kwargs: Any) -> ModelCallReservation:
        del kwargs
        return ModelCallReservation(status="reserved")

    async def fail(self, **kwargs: Any) -> None:
        del kwargs

    async def complete(self, **kwargs: Any) -> None:
        del kwargs
        self.events.append("complete_started")
        self.complete_started.set()
        await self.allow_complete.wait()
        self.events.append("complete_finished")


def _gateway_request() -> ModelGenerateRequest:
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
async def test_gateway_persists_before_yielding_completed() -> None:
    state = _OrderedState()
    service = ModelGatewayInternalService(_StreamingModel(), state=state)
    events: list[str] = []

    async def consume() -> None:
        async for event in service.generate_stream(_gateway_request()):
            events.append(event.type)
    task = asyncio.create_task(consume())
    await state.complete_started.wait()
    assert events == ["delta"]
    state.allow_complete.set()
    await task
    assert events == ["delta", "completed"]
    assert state.events == ["complete_started", "complete_finished"]


@pytest.mark.asyncio
async def test_gateway_does_not_claim_completed_when_persist_fails() -> None:
    class _FailState:
        async def reserve(self, **kwargs: Any) -> ModelCallReservation:
            del kwargs
            return ModelCallReservation(status="reserved")

        async def fail(self, **kwargs: Any) -> None:
            del kwargs

        async def complete(self, **kwargs: Any) -> None:
            del kwargs
            raise RuntimeError("db unavailable")

    service = ModelGatewayInternalService(_StreamingModel(), state=_FailState())
    events: list[ModelStreamEvent] = []
    with pytest.raises(RuntimeError, match="db unavailable"):
        async for event in service.generate_stream(_gateway_request()):
            events.append(event)
    assert [event.type for event in events] == ["delta"]


@pytest.mark.asyncio
async def test_remote_client_reconnects_once_for_missing_completed() -> None:
    completed = ModelGenerateResponse(
        model_call_id="mdl_1",
        provider="test",
        model="test",
        completed_output="hello",
        deltas=("hello",),
        tool_calls=(),
        finish_reason="stop",
        usage={"input_tokens": 1, "output_tokens": 1},
    )
    calls = {"n": 0}

    async def fake_stream(
        path: str,
        request: Any,
        event_model: type[ModelStreamEvent],
    ):
        del path, request, event_model
        calls["n"] += 1
        if calls["n"] == 1:
            yield ModelStreamEvent(
                model_call_id="mdl_1",
                sequence=1,
                type="delta",
                payload={"delta": "hel"},
            )
            return
        yield ModelStreamEvent(
            model_call_id="mdl_1",
            sequence=1,
            type="delta",
            payload={"delta": "hello"},
        )
        yield ModelStreamEvent(
            model_call_id="mdl_1",
            sequence=2,
            type="completed",
            payload=completed.model_dump(mode="json"),
        )

    client = RemoteModelClient("http://model.test", bearer_token="token")
    client._contract.stream = fake_stream  # type: ignore[method-assign]
    chunks = [
        chunk
        async for chunk in client.generate_stream(
            ModelRequest(
                model_call_id="mdl_1",
                tenant_id="t1",
                run_id="run_1",
                messages=({"role": "user", "content": "hi"},),
                policy=ModelPolicy(),
            )
        )
    ]
    await client.aclose()
    assert calls["n"] == 2
    assert [chunk.kind for chunk in chunks] == ["delta", "completed"]
    assert chunks[0].delta == "hel"
    assert chunks[1].response is not None
    assert chunks[1].response.completed_output == "hello"
