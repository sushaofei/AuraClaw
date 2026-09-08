from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from auraclaw.contracts.errors import RuntimeCancelledError
from auraclaw.contracts.internal import (
    InternalRequestContext,
    ModelCancelRequest,
    ModelCancelResponse,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ModelStreamEvent,
    ServiceIdentity,
)
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.model_gateway.ports import (
    ModelCallExecution,
    ModelCallReservation,
    ModelCancellation,
)
from auraclaw.runtime.ports import (
    ModelPolicy,
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ProviderCancellationResult,
)
from auraclaw.runtime.rounds import ModelRoundExecutor


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


class _LifecycleState:
    def __init__(self) -> None:
        self.status = "new"
        self.owner = ""
        self.claim_token = "claim-1"
        self.cancel_requested = False

    async def reserve(self, **kwargs: Any) -> ModelCallReservation:
        self.status = "executing"
        self.owner = str(kwargs["execution_owner"])
        return ModelCallReservation("reserved", claim_token=self.claim_token)

    async def heartbeat(self, **kwargs: Any) -> ModelCallExecution:
        owned = (
            kwargs["execution_owner"] == self.owner
            and kwargs["claim_token"] == self.claim_token
        )
        return ModelCallExecution(
            status=self.status,
            owned=owned,
            cancel_requested=self.cancel_requested,
        )

    async def request_cancel(self, **kwargs: Any) -> ModelCancellation:
        del kwargs
        self.cancel_requested = True
        self.status = "cancel_requested"
        return ModelCancellation(self.status, True, self.owner)

    async def mark_cancelled(self, **kwargs: Any) -> bool:
        del kwargs
        self.status = "cancelled"
        return True

    async def mark_reconciling(self, **kwargs: Any) -> bool:
        del kwargs
        self.status = "reconciling"
        return True

    async def complete(self, **kwargs: Any) -> None:
        del kwargs
        self.status = "completed"

    async def fail(self, **kwargs: Any) -> None:
        del kwargs
        self.status = "failed"


class _BlockingCancellableModel:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    async def generate_stream(self, request: Any):
        task = asyncio.current_task()
        assert task is not None
        self._tasks[request.model_call_id] = task
        self.started.set()
        try:
            await asyncio.Event().wait()
            yield  # pragma: no cover
        finally:
            self._tasks.pop(request.model_call_id, None)

    async def cancel(self, model_call_id: str) -> ProviderCancellationResult:
        task = self._tasks.get(model_call_id)
        if task is None:
            return ProviderCancellationResult(stopped=False)
        task.cancel()
        return ProviderCancellationResult(
            stopped=True,
            usage={"total_tokens": 1},
            usage_final=True,
        )


class _BlockingNonCancellableModel:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.finish = asyncio.Event()

    async def generate_stream(self, request: Any):
        self.started.set()
        await self.finish.wait()
        yield ModelStreamChunk(
            kind="completed",
            response=ModelResponse(
                model_call_id=request.model_call_id,
                provider="test",
                model="test",
                completed_output="done",
                usage={"total_tokens": 2},
            ),
        )


class _BlockingUsageUnknownModel(_BlockingCancellableModel):
    async def cancel(self, model_call_id: str) -> ProviderCancellationResult:
        task = self._tasks.get(model_call_id)
        if task is None:
            return ProviderCancellationResult(stopped=False)
        task.cancel()
        return ProviderCancellationResult(stopped=True, usage_final=False)


def _cancel_request() -> ModelCancelRequest:
    request = _gateway_request()
    return ModelCancelRequest(
        context=request.context,
        model_call_id=request.model_call_id,
        run_id=request.run_id,
    )


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
async def test_completed_provider_result_enters_reconciliation_when_commit_fails() -> None:
    class ReconciliationState:
        def __init__(self) -> None:
            self.error_code: str | None = None

        async def reserve(self, **kwargs: Any) -> ModelCallReservation:
            del kwargs
            return ModelCallReservation("reserved", claim_token="claim-1")

        async def heartbeat(self, **kwargs: Any) -> ModelCallExecution:
            del kwargs
            return ModelCallExecution("executing", owned=True)

        async def complete(self, **kwargs: Any) -> None:
            del kwargs
            raise RuntimeError("db unavailable")

        async def mark_reconciling(self, **kwargs: Any) -> bool:
            self.error_code = str(kwargs["error_code"])
            return True

        async def fail(self, **kwargs: Any) -> None:
            del kwargs

    state = ReconciliationState()
    service = ModelGatewayInternalService(_StreamingModel(), state=state)
    with pytest.raises(RuntimeError, match="db unavailable"):
        async for _event in service.generate_stream(_gateway_request()):
            pass
    assert state.error_code == "completion_persistence_failed"


@pytest.mark.asyncio
async def test_cancel_on_another_replica_stops_persisted_owner() -> None:
    state = _LifecycleState()
    model = _BlockingCancellableModel()
    owner = ModelGatewayInternalService(
        model,
        state=state,
        gateway_id="gateway-a",
        heartbeat_interval=0.01,
    )
    peer = ModelGatewayInternalService(model, state=state, gateway_id="gateway-b")

    async def consume() -> None:
        async for _event in owner.generate_stream(_gateway_request()):
            pass

    running = asyncio.create_task(consume())
    await model.started.wait()
    response = await peer.cancel(_cancel_request())
    assert response.status == "cancel_requested"
    assert not response.cancelled
    with pytest.raises(asyncio.CancelledError):
        await running
    assert state.status == "cancelled"


@pytest.mark.asyncio
async def test_consumer_disconnect_is_reconciling_not_business_cancel() -> None:
    state = _LifecycleState()
    model = _BlockingCancellableModel()
    owner = ModelGatewayInternalService(
        model,
        state=state,
        gateway_id="gateway-a",
        heartbeat_interval=0.01,
    )

    async def consume() -> None:
        async for _event in owner.generate_stream(_gateway_request()):
            pass

    running = asyncio.create_task(consume())
    await model.started.wait()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert state.status == "reconciling"


@pytest.mark.asyncio
async def test_cancel_with_unknown_provider_usage_keeps_reconciliation() -> None:
    state = _LifecycleState()
    model = _BlockingUsageUnknownModel()
    owner = ModelGatewayInternalService(
        model,
        state=state,
        gateway_id="gateway-a",
        heartbeat_interval=0.01,
    )
    peer = ModelGatewayInternalService(model, state=state, gateway_id="gateway-b")

    async def consume() -> None:
        async for _event in owner.generate_stream(_gateway_request()):
            pass

    running = asyncio.create_task(consume())
    await model.started.wait()
    await peer.cancel(_cancel_request())
    with pytest.raises(asyncio.CancelledError):
        await running
    assert state.status == "reconciling"


@pytest.mark.asyncio
async def test_unsupported_provider_cancel_is_not_reported_as_cancelled() -> None:
    state = _LifecycleState()
    model = _BlockingNonCancellableModel()
    owner = ModelGatewayInternalService(
        model,
        state=state,
        gateway_id="gateway-a",
        heartbeat_interval=0.01,
    )
    peer = ModelGatewayInternalService(model, state=state, gateway_id="gateway-b")

    async def consume() -> list[str]:
        return [
            event.type
            async for event in owner.generate_stream(_gateway_request())
        ]

    running = asyncio.create_task(consume())
    await model.started.wait()
    response = await peer.cancel(_cancel_request())
    assert response.status == "cancel_requested"
    assert not response.cancelled
    assert not response.provider_cancellable
    model.finish.set()
    assert await running == ["completed"]
    assert state.status == "completed"


@pytest.mark.asyncio
async def test_runtime_business_cancel_propagates_to_active_model_call() -> None:
    class BlockingModel:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.cancelled_request: ModelRequest | None = None

        async def generate_stream(self, request: ModelRequest):
            del request
            self.started.set()
            await asyncio.Event().wait()
            yield  # pragma: no cover

        async def cancel(self, request: ModelRequest) -> Any:
            self.cancelled_request = request
            return SimpleNamespace(provider_cancellable=False)

    class CancelledControl:
        def __init__(self, started: asyncio.Event) -> None:
            self.started = started

        async def is_cancelled(self, *args: Any) -> bool:
            del args
            await self.started.wait()
            return True

    model = BlockingModel()
    model_round = ModelRoundExecutor(
        model=model,  # type: ignore[arg-type]
        control=CancelledControl(model.started),  # type: ignore[arg-type]
        runtime_events=object(),  # type: ignore[arg-type]
    )
    assignment = SimpleNamespace(
        tenant_id="t1",
        session_id="session-1",
        run_id="run-1",
        deadline=None,
    )
    request = ModelRequest(
        model_call_id="mdl-run-1",
        tenant_id="t1",
        run_id="run-1",
        messages=(),
    )
    with pytest.raises(RuntimeCancelledError, match="run cancelled"):
        await model_round.execute(assignment, request, sequence=0)  # type: ignore[arg-type]
    assert model.cancelled_request == request


@pytest.mark.asyncio
async def test_remote_model_client_sends_authenticated_cancel_contract() -> None:
    captured: dict[str, Any] = {}

    async def fake_call(path: str, request: Any, response_model: Any) -> Any:
        captured.update(path=path, request=request, response_model=response_model)
        return ModelCancelResponse(
            model_call_id=request.model_call_id,
            cancelled=False,
            status="cancel_requested",
            provider_cancellable=True,
        )

    client = RemoteModelClient("http://model.test", bearer_token="token")
    client._contract.call = fake_call  # type: ignore[method-assign]
    request = ModelRequest(
        model_call_id="mdl-cancel",
        tenant_id="tenant-1",
        run_id="run-1",
        messages=(),
    )
    response = await client.cancel(request)
    await client.aclose()
    assert captured["path"] == "/internal/v1/model/cancel"
    assert captured["response_model"] is ModelCancelResponse
    assert captured["request"].context.service_identity is ServiceIdentity.AGENT_RUNTIME
    assert response.status == "cancel_requested"


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
