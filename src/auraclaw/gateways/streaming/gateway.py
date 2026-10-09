from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Protocol

from auraclaw.contracts.errors import NotFoundError
from auraclaw.projection.ports import TaskReader
from auraclaw.runtime.ports import RuntimeEvent


class StreamSubscription(Protocol):
    initial: list[RuntimeEvent]
    replay_missed: bool

    def events(self) -> AsyncIterator[RuntimeEvent]: ...


class RuntimeReplayBus(Protocol):
    async def ensure_accepting(self) -> None: ...

    async def subscribe(
        self, tenant_id: str, session_id: str, *, after_sequence: int | None = None
    ) -> StreamSubscription: ...


def _public_cursor(event: RuntimeEvent) -> str:
    return f"{event.session_id}:{event.sequence}"


def _event_data(event: RuntimeEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "tenant_id": event.tenant_id,
        "root_session_id": event.root_session_id,
        "session_id": event.session_id,
        "run_id": event.run_id,
        "sequence": event.sequence,
        "type": event.type,
        "timestamp": event.timestamp.isoformat(),
        "payload": event.payload,
        "durable": event.durable,
        "visibility": event.visibility,
    }


class StreamingGateway:
    """Tenant-authorized SSE bridge over a shared, bounded replay buffer."""

    def __init__(
        self,
        *,
        reader: TaskReader,
        bus: RuntimeReplayBus,
        delta_min_interval: float = 0.0,
        heartbeat_interval: float = 15.0,
    ) -> None:
        self._reader = reader
        self._bus = bus
        self._delta_min_interval = max(delta_min_interval, 0.0)
        self._heartbeat_interval = max(heartbeat_interval, 0.001)

    async def subscribe(
        self,
        *,
        tenant_id: str,
        session_id: str,
        last_event_id: str | None,
    ) -> StreamSubscription:
        await self.authorize(tenant_id=tenant_id, session_id=session_id)
        return await self._bus.subscribe(
            tenant_id,
            session_id,
            after_sequence=self._parse_cursor(session_id, last_event_id),
        )

    async def authorize(self, *, tenant_id: str, session_id: str) -> None:
        ensure_accepting = getattr(self._bus, "ensure_accepting", None)
        if ensure_accepting is not None:
            await ensure_accepting()
        if await self._reader.get_task(tenant_id, session_id) is None:
            raise NotFoundError(f"Session not found: {session_id}")

    async def sse(
        self,
        *,
        tenant_id: str,
        session_id: str,
        last_event_id: str | None,
    ) -> AsyncIterator[str]:
        subscription = await self.subscribe(
            tenant_id=tenant_id,
            session_id=session_id,
            last_event_id=last_event_id,
        )
        if subscription.replay_missed:
            data = json.dumps(
                {
                    "reason": "cursor_expired",
                    "query_url": f"/v1/tasks/{session_id}",
                },
                separators=(",", ":"),
            )
            yield f"event: stream.reset\ndata: {data}\n\n"
        last_delta_sent_at: float | None = None
        loop = asyncio.get_running_loop()
        events = subscription.events()
        pending: asyncio.Future[RuntimeEvent] = asyncio.ensure_future(anext(events))
        try:
            while True:
                done, _ = await asyncio.wait({pending}, timeout=self._heartbeat_interval)
                if not done:
                    # SSE comments are ignored by clients but keep every proxy hop active.
                    yield ": keepalive\n\n"
                    continue
                try:
                    event = pending.result()
                except StopAsyncIteration:
                    break
                if event.visibility == "user":
                    if event.type == "model.output.delta" and self._delta_min_interval > 0:
                        now = loop.time()
                        if last_delta_sent_at is not None:
                            delay = self._delta_min_interval - (now - last_delta_sent_at)
                            if delay > 0:
                                await asyncio.sleep(delay)
                        last_delta_sent_at = loop.time()
                    data = json.dumps(_event_data(event), separators=(",", ":"))
                    yield f"id: {_public_cursor(event)}\nevent: {event.type}\ndata: {data}\n\n"
                pending = asyncio.ensure_future(anext(events))
        finally:
            if not pending.done():
                pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)

    @staticmethod
    def _parse_cursor(session_id: str, value: str | None) -> int | None:
        if value is None:
            return None
        prefix, separator, sequence = value.rpartition(":")
        if separator != ":" or prefix != session_id:
            return None
        try:
            parsed = int(sequence)
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
