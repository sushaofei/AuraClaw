from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import CanonicalEvent, NewEvent, utc_now
from auraclaw.domain.ports import AppendResult


@dataclass(frozen=True)
class OutboxRecord:
    event_id: str
    destination: str
    event: CanonicalEvent
    published: bool = False


class InMemoryEventStore:
    """Development adapter preserving Event Store and Outbox transaction semantics."""

    def __init__(self) -> None:
        self._streams: dict[tuple[str, str], list[CanonicalEvent]] = {}
        self._commands: dict[tuple[str, str], dict[str, Any]] = {}
        self._outbox: list[OutboxRecord] = []
        self._lock = asyncio.Lock()

    async def load(self, tenant_id: str, session_id: str) -> list[CanonicalEvent]:
        return list(self._streams.get((tenant_id, session_id), []))

    async def append(
        self,
        *,
        root_session_id: str,
        session_id: str,
        run_id: str | None,
        context: CommandContext,
        events: Sequence[NewEvent],
        command_result: dict[str, Any],
    ) -> AppendResult:
        command_key = (context.tenant_id, context.command_id)
        stream_key = (context.tenant_id, session_id)
        async with self._lock:
            previous = self._commands.get(command_key)
            if previous is not None:
                return AppendResult(events=[], command_result=dict(previous), deduplicated=True)

            stream = self._streams.setdefault(stream_key, [])
            if len(stream) != context.expected_version:
                raise VersionConflictError(
                    f"expected Session version {context.expected_version}, got {len(stream)}"
                )

            canonical: list[CanonicalEvent] = []
            for offset, event in enumerate(events, start=1):
                stored = CanonicalEvent(
                    event_id=f"evt_{uuid4().hex}",
                    tenant_id=context.tenant_id,
                    root_session_id=root_session_id,
                    session_id=session_id,
                    run_id=run_id,
                    aggregate_version=context.expected_version + offset,
                    type=event.type,
                    occurred_at=utc_now(),
                    actor=context.actor,
                    correlation_id=context.correlation_id,
                    causation_id=context.command_id,
                    visibility=event.visibility,
                    schema_version=1,
                    payload=dict(event.payload),
                )
                canonical.append(stored)

            # These writes are one critical section here and one DB transaction in production.
            stream.extend(canonical)
            self._commands[command_key] = dict(command_result)
            self._outbox.extend(
                OutboxRecord(event_id=event.event_id, destination="projection", event=event)
                for event in canonical
            )
            return AppendResult(events=canonical, command_result=dict(command_result))

    async def pending_outbox(self) -> list[OutboxRecord]:
        return list(self._outbox)
