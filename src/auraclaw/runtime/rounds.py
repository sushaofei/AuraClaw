from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from auraclaw.contracts.errors import ModelProviderError, RuntimeCancelledError
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.model_stream import iter_model_stream
from auraclaw.runtime.ports import (
    ModelClient,
    ModelRequest,
    ModelResponse,
    RuntimeControlClient,
    RuntimeEvent,
    RuntimeEventPublisher,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ModelRoundResult:
    response: ModelResponse
    sequence: int

    def __iter__(self) -> Iterator[ModelResponse | int]:
        """Keep tuple-unpacking compatibility for existing diagnostic tests."""

        yield self.response
        yield self.sequence


class ModelRoundExecutor:
    """Run one cancellable model stream and publish best-effort live deltas."""

    def __init__(
        self,
        *,
        model: ModelClient,
        control: RuntimeControlClient,
        runtime_events: RuntimeEventPublisher,
    ) -> None:
        self._model = model
        self._control = control
        self._runtime_events = runtime_events

    async def execute(
        self,
        assignment: RuntimeAssignment,
        request: ModelRequest,
        *,
        sequence: int,
        publish_deltas: bool = True,
        execute_started: float | None = None,
        prep: Awaitable[None] | None = None,
    ) -> ModelRoundResult:
        response: ModelResponse | None = None
        stream_started = time.perf_counter()
        first_delta_logged = False
        if execute_started is None:
            execute_started = stream_started

        async def next_chunk(iterator: Any) -> Any | None:
            try:
                return await anext(iterator)
            except StopAsyncIteration:
                return None

        async def wait_for_business_cancel() -> str:
            while True:
                if await self._control.is_cancelled(
                    assignment.tenant_id, assignment.session_id, assignment.run_id
                ):
                    return "run cancelled"
                if assignment.deadline is not None and datetime.now(UTC) >= assignment.deadline:
                    return "runtime deadline exceeded"
                await asyncio.sleep(1.0)

        stream = iter_model_stream(self._model, request)
        iterator = stream.__aiter__()
        cancel_watch = asyncio.create_task(wait_for_business_cancel())

        async def next_or_cancel(task: asyncio.Task[Any]) -> Any | None:
            done, _ = await asyncio.wait(
                {task, cancel_watch}, return_when=asyncio.FIRST_COMPLETED
            )
            if cancel_watch not in done:
                try:
                    return await task
                except BaseException:
                    cancel_watch.cancel()
                    with suppress(asyncio.CancelledError):
                        await cancel_watch
                    raise
            reason = await cancel_watch
            cancel = getattr(self._model, "cancel", None)
            grace = 0.0
            if callable(cancel):
                try:
                    result = await cancel(request)
                    if bool(getattr(result, "provider_cancellable", False)):
                        grace = 6.0
                except Exception:
                    logger.exception(
                        "failed to propagate model cancellation model_call=%s",
                        request.model_call_id,
                    )
            if grace:
                with suppress(BaseException):
                    await asyncio.wait_for(asyncio.shield(task), timeout=grace)
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()
            raise RuntimeCancelledError(f"{reason}: {assignment.run_id}")

        first_chunk_task = asyncio.create_task(next_chunk(iterator))
        try:
            if prep is not None:
                await prep
            chunk = await next_or_cancel(first_chunk_task)
        except BaseException:
            if not first_chunk_task.done():
                first_chunk_task.cancel()
                with suppress(asyncio.CancelledError):
                    await first_chunk_task
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()
            cancel_watch.cancel()
            with suppress(asyncio.CancelledError):
                await cancel_watch
            raise
        if chunk is None:
            cancel_watch.cancel()
            with suppress(asyncio.CancelledError):
                await cancel_watch
            raise ModelProviderError("model stream ended without a completed response")
        while True:
            if chunk.kind == "delta":
                if publish_deltas and chunk.delta:
                    sequence += 1
                    await self.publish_delta(assignment, sequence, chunk.delta)
                    if not first_delta_logged:
                        first_delta_logged = True
                        now = time.perf_counter()
                        logger.info(
                            "ttft.first_delta session=%s run=%s model_call=%s "
                            "execute_ms=%.2f stream_ms=%.2f",
                            assignment.session_id,
                            assignment.run_id,
                            request.model_call_id,
                            (now - execute_started) * 1_000,
                            (now - stream_started) * 1_000,
                        )
            elif chunk.kind == "completed":
                response = chunk.response
            next_chunk_task = asyncio.create_task(next_chunk(iterator))
            chunk = await next_or_cancel(next_chunk_task)
            if chunk is None:
                break
        cancel_watch.cancel()
        with suppress(asyncio.CancelledError):
            await cancel_watch
        if response is None:
            raise ModelProviderError("model stream ended without a completed response")
        return ModelRoundResult(response=response, sequence=sequence)

    async def publish_delta(
        self, assignment: RuntimeAssignment, sequence: int, delta: str
    ) -> None:
        try:
            await self._runtime_events.publish(
                RuntimeEvent(
                    event_id=f"rte_{uuid4().hex}",
                    tenant_id=assignment.tenant_id,
                    root_session_id=assignment.root_session_id,
                    session_id=assignment.session_id,
                    run_id=assignment.run_id,
                    sequence=sequence,
                    type="model.output.delta",
                    timestamp=datetime.now(UTC),
                    payload={"delta": delta},
                    visibility="user",
                )
            )
        except Exception:
            # Runtime Event streams deliberately do not guarantee result delivery.
            return


class ToolRoundDisposition(StrEnum):
    CONTINUE = "continue"
    WAITING_FOR_HUMAN = "waiting_for_human"


@dataclass(frozen=True, slots=True)
class ToolRoundResult:
    disposition: ToolRoundDisposition
    result: dict[str, Any]
    resumed_from_checkpoint: bool
