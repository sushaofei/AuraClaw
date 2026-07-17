from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Protocol

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.domain.ports import ProjectionWriter


class OutboxItem(Protocol):
    outbox_id: int
    event: CanonicalEvent


class OutboxSource(Protocol):
    async def pending_outbox(self) -> Sequence[OutboxItem]: ...

    async def mark_outbox_published(self, outbox_id: int) -> None: ...

    async def mark_outbox_failed(self, outbox_id: int) -> None: ...


class OutboxRelay:
    """At-least-once relay from the transactional outbox to projectors."""

    def __init__(self, source: OutboxSource, projector: ProjectionWriter) -> None:
        self._source = source
        self._projector = projector
        self._lock = asyncio.Lock()

    async def relay_once(self, *, limit: int = 100) -> int:
        async with self._lock:
            relayed = 0
            records = await self._source.pending_outbox()
            for record in records[:limit]:
                try:
                    await self._projector.project([record.event])
                except Exception:
                    await self._source.mark_outbox_failed(record.outbox_id)
                    continue
                await self._source.mark_outbox_published(record.outbox_id)
                relayed += 1
            return relayed
