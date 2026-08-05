from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from auraclaw.runtime.model_stream import iter_model_stream
from auraclaw.runtime.ports import (
    ModelPolicy,
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    RuntimeEvent,
)


class _DelayedStreamModel:
    def __init__(self) -> None:
        self.delta_released = asyncio.Event()
        self.completed = asyncio.Event()

    async def generate_stream(
        self, request: ModelRequest
    ) -> AsyncIterator[ModelStreamChunk]:
        del request
        yield ModelStreamChunk(kind="delta", delta="你好")
        self.delta_released.set()
        await asyncio.sleep(0)
        yield ModelStreamChunk(kind="delta", delta="世界")
        yield ModelStreamChunk(
            kind="completed",
            response=ModelResponse(
                model_call_id="mdl_1",
                provider="test",
                model="test-model",
                completed_output="你好世界",
                deltas=("你好", "世界"),
            ),
        )
        self.completed.set()


class _RecordingBus:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def publish(self, event: RuntimeEvent) -> None:
        self.events.append(event)


@pytest.mark.asyncio
async def test_iter_model_stream_emits_deltas_before_completion() -> None:
    model = _DelayedStreamModel()
    chunks: list[ModelStreamChunk] = []

    async def consume() -> None:
        async for chunk in iter_model_stream(
            model,
            ModelRequest(
                model_call_id="mdl_1",
                tenant_id="local",
                run_id="run_1",
                messages=({"role": "user", "content": "hi"},),
                policy=ModelPolicy(),
            ),
        ):
            chunks.append(chunk)
            if chunk.kind == "delta" and not model.completed.is_set():
                assert not model.completed.is_set()

    await consume()
    assert [c.kind for c in chunks] == ["delta", "delta", "completed"]
    assert [c.delta for c in chunks if c.kind == "delta"] == ["你好", "世界"]
    assert chunks[-1].response is not None
    assert chunks[-1].response.completed_output == "你好世界"


@pytest.mark.asyncio
async def test_harness_publishes_deltas_during_stream() -> None:
    from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
    from auraclaw.runtime.harness import AgentHarness

    class _Session:
        async def load(self, assignment: RuntimeAssignment) -> list[object]:
            del assignment
            return []

        async def append(self, *args: object, **kwargs: object) -> list[object]:
            del args, kwargs
            return []

    class _Control:
        async def assert_fencing(self, resource_id: str, fencing_token: int) -> None:
            del resource_id, fencing_token

        async def is_cancelled(self, tenant_id: str, session_id: str, run_id: str) -> bool:
            del tenant_id, session_id, run_id
            return False

        async def save_checkpoint(self, checkpoint: object) -> None:
            del checkpoint

        async def load_checkpoint(
            self, tenant_id: str, session_id: str, run_id: str
        ) -> None:
            del tenant_id, session_id, run_id
            return None

        async def finish_assignment(self, task_id: str, outcome: str) -> None:
            del task_id, outcome

    bus = _RecordingBus()
    model = _DelayedStreamModel()
    harness = AgentHarness(
        control_store=_Control(),  # type: ignore[arg-type]
        session=_Session(),  # type: ignore[arg-type]
        model=model,  # type: ignore[arg-type]
        tools=object(),  # type: ignore[arg-type]
        runtime_events=bus,  # type: ignore[arg-type]
    )
    assignment = RuntimeAssignment(
        tenant_id="local",
        root_session_id="ses_1",
        session_id="ses_1",
        run_id="run_1",
        runtime_id="rt_1",
        lease_id="lease_1",
        fencing_token=1,
        role="general",
        resource_profile={},
        budget=RuntimeBudget(),
    )
    response, sequence = await harness._generate_with_live_deltas(
        assignment,
        ModelRequest(
            model_call_id="mdl_1",
            tenant_id="local",
            run_id="run_1",
            messages=({"role": "user", "content": "hi"},),
        ),
        sequence=0,
    )
    assert response.completed_output == "你好世界"
    assert sequence == 2
    assert [event.payload["delta"] for event in bus.events] == ["你好", "世界"]
    assert all(event.type == "model.output.delta" for event in bus.events)
