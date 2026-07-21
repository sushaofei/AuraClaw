from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import replace

from auraclaw.control.orchestrator import ManagedOrchestrator
from auraclaw.projection.ports import TaskReader
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.ports import ModelRequest, ModelResponse, RuntimeEvent
from auraclaw.session.ports import EventStore, OutboxRelayPort

logger = logging.getLogger(__name__)


class DevelopmentModelClient:
    """Deterministic local model used only by the in-memory development runtime."""

    async def generate(self, request: ModelRequest) -> ModelResponse:
        prompt = next(
            (
                str(message.get("content", ""))
                for message in reversed(request.messages)
                if message.get("role") == "user"
            ),
            "",
        )
        output = self._answer(prompt)
        deltas = tuple(output[index : index + 8] for index in range(0, len(output), 8))
        return ModelResponse(
            model_call_id=request.model_call_id,
            provider="development",
            model="auraclaw-deterministic-stream",
            completed_output=output,
            deltas=deltas,
            usage={"input_tokens": max(1, len(prompt) // 4), "output_tokens": len(deltas)},
        )

    @staticmethod
    def _answer(prompt: str) -> str:
        if "架构" in prompt or "AuraClaw" in prompt:
            return (
                "AuraClaw 以 Canonical Session Event 作为任务事实源，Projection 可随时重建；"
                "Orchestrator 只负责资源调度，Agent Runtime 通过受控网关访问模型和工具，"
                "Runtime Event 仅承载可丢弃的实时增量，最终结果以 Task / Result API 为准。"
            )
        return (
            f"AuraClaw 开发运行时已收到问题：{prompt}。"
            "这是用于验证 Streaming 与 Result 一致性的本地回答。"
        )


class DelayedRuntimeEventPublisher:
    """Makes deterministic development deltas observable as separate browser updates."""

    def __init__(
        self,
        publish: Callable[[RuntimeEvent], Awaitable[None]],
        *,
        delta_delay: float,
    ) -> None:
        self._publish = publish
        self._delta_delay = max(0.0, delta_delay)
        self._sequences: dict[tuple[str, str], int] = {}
        self._sequence_lock = asyncio.Lock()

    async def publish(self, event: RuntimeEvent) -> None:
        if event.type == "model.output.delta" and self._delta_delay:
            await asyncio.sleep(self._delta_delay)
        key = (event.tenant_id, event.session_id)
        async with self._sequence_lock:
            sequence = self._sequences.get(key, 0) + 1
            self._sequences[key] = sequence
        await self._publish(replace(event, sequence=sequence))


class DevelopmentRuntimeWorker:
    """In-process development worker preserving queue/orchestrator/runtime boundaries."""

    def __init__(
        self,
        *,
        event_store: EventStore,
        reader: TaskReader,
        relay: OutboxRelayPort,
        orchestrator: ManagedOrchestrator,
        harness: AgentHarness,
        poll_interval: float = 0.05,
    ) -> None:
        self._event_store = event_store
        self._reader = reader
        self._relay = relay
        self._orchestrator = orchestrator
        self._harness = harness
        self._poll_interval = max(0.01, poll_interval)
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        events = await self._event_store.load_all()
        session_keys = {(event.tenant_id, event.session_id) for event in events}
        tasks = []
        for tenant_id, session_id in session_keys:
            task = await self._reader.get_task(tenant_id, session_id)
            if task is not None and task.get("status") in {"pending", "runnable"}:
                tasks.append(task)
        await self._orchestrator.watch(tasks)

        completed = 0
        while assignment := await self._orchestrator.schedule_once():
            await self._harness.execute(assignment)
            await self._relay.relay_once()
            completed += 1
        return completed

    async def run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("runtime worker iteration failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._poll_interval)

    async def stop(self) -> None:
        self._stopped.set()


class ProductionRuntimeWorker(DevelopmentRuntimeWorker):
    """MVP in-process production worker; a separate worker process remains future work."""
