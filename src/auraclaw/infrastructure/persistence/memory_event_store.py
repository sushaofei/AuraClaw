from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import CanonicalEvent, NewEvent, utc_now
from auraclaw.session.ports import AppendResult, ClaimedOutboxRecord, SessionSnapshot

DELIVERY_TRIGGER_EVENTS = {
    "run.completed",
    "run.failed",
    "run.cancelled",
    "approval.requested",
    "child.result_published",
}
CONTROL_TRIGGER_EVENTS = {"run.requested", "session.resumed", "dependency.changed"}


@dataclass
class OutboxRecord:
    outbox_id: int
    event_id: str
    destination: str
    event: CanonicalEvent
    published: bool = False
    publish_attempt: int = 0
    claimed_by: str | None = None
    claim_token: str | None = None
    claim_expires_at: datetime | None = None
    poisoned: bool = False


class InMemoryEventStore:
    """Development adapter preserving Event Store and Outbox transaction semantics."""

    def __init__(self) -> None:
        self._streams: dict[tuple[str, str], list[CanonicalEvent]] = {}
        self._commands: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._outbox: list[OutboxRecord] = []
        self._snapshots: dict[tuple[str, str], SessionSnapshot] = {}
        self._lock = asyncio.Lock()

    async def load(
        self,
        tenant_id: str,
        session_id: str,
        *,
        from_version: int = 1,
        event_types: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[CanonicalEvent]:
        allowed = set(event_types) if event_types is not None else None
        events = [
            event
            for event in self._streams.get((tenant_id, session_id), [])
            if event.aggregate_version >= from_version
            and (allowed is None or event.type in allowed)
        ]
        if limit is not None:
            return events[:limit]
        return events

    async def load_all(self, tenant_id: str | None = None) -> list[CanonicalEvent]:
        events = [
            event
            for (stream_tenant, _), stream in self._streams.items()
            if tenant_id is None or stream_tenant == tenant_id
            for event in stream
        ]
        return sorted(
            events,
            key=lambda event: (event.tenant_id, event.session_id, event.aggregate_version),
        )

    async def get_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        return self._snapshots.get((tenant_id, session_id))

    async def save_snapshot(self, snapshot: SessionSnapshot) -> None:
        async with self._lock:
            current = self._snapshots.get((snapshot.tenant_id, snapshot.session_id))
            if current is None or current.aggregate_version <= snapshot.aggregate_version:
                self._snapshots[(snapshot.tenant_id, snapshot.session_id)] = snapshot

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
        command_key = (context.tenant_id, context.operation, context.command_id)
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
                    causation_id=context.causation_id or context.command_id,
                    visibility=event.visibility,
                    schema_version=1,
                    payload=dict(event.payload),
                )
                canonical.append(stored)

            # These writes are one critical section here and one DB transaction in production.
            stream.extend(canonical)
            self._commands[command_key] = dict(command_result)
            for stored_event in canonical:
                self._outbox.append(
                    OutboxRecord(
                        outbox_id=len(self._outbox) + 1,
                        event_id=stored_event.event_id,
                        destination="projection",
                        event=stored_event,
                    )
                )
                if stored_event.type in DELIVERY_TRIGGER_EVENTS:
                    self._outbox.append(
                        OutboxRecord(
                            outbox_id=len(self._outbox) + 1,
                            event_id=stored_event.event_id,
                            destination="delivery",
                            event=stored_event,
                        )
                    )
                if stored_event.type in CONTROL_TRIGGER_EVENTS:
                    self._outbox.append(
                        OutboxRecord(
                            outbox_id=len(self._outbox) + 1,
                            event_id=stored_event.event_id,
                            destination="control",
                            event=stored_event,
                        )
                    )
            return AppendResult(events=canonical, command_result=dict(command_result))

    async def pending_outbox(self) -> list[OutboxRecord]:
        return [
            record
            for record in self._outbox
            if not record.published and record.destination == "projection"
        ]

    async def pending_delivery_outbox(self) -> list[OutboxRecord]:
        return [
            record
            for record in self._outbox
            if not record.published and record.destination == "delivery"
        ]

    async def mark_outbox_published(self, outbox_id: int) -> None:
        async with self._lock:
            for record in self._outbox:
                if record.outbox_id == outbox_id:
                    record.published = True
                    return

    async def mark_outbox_failed(self, outbox_id: int) -> None:
        async with self._lock:
            for record in self._outbox:
                if record.outbox_id == outbox_id:
                    record.publish_attempt += 1
                    return

    async def claim_outbox(
        self,
        destination: str,
        worker_id: str,
        *,
        limit: int,
        claim_ttl: timedelta,
        wait_seconds: float = 0,
    ) -> list[ClaimedOutboxRecord]:
        import time

        deadline = time.monotonic() + max(0.0, wait_seconds)
        while True:
            claimed = await self._claim_outbox_once(
                destination, worker_id, limit=limit, claim_ttl=claim_ttl
            )
            if claimed or wait_seconds <= 0:
                return claimed
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            await asyncio.sleep(min(0.05, remaining))

    async def _claim_outbox_once(
        self,
        destination: str,
        worker_id: str,
        *,
        limit: int,
        claim_ttl: timedelta,
    ) -> list[ClaimedOutboxRecord]:
        now = datetime.now(UTC)
        claimed: list[ClaimedOutboxRecord] = []
        blocked_sessions: set[tuple[str, str]] = set()
        async with self._lock:
            for record in self._outbox:
                if record.destination != destination or record.published:
                    continue
                session_key = (record.event.tenant_id, record.event.session_id)
                if session_key in blocked_sessions:
                    continue
                # The first unpublished record is the per-Session ordering barrier.
                # A live claim or poison record must block later events.
                blocked_sessions.add(session_key)
                claim_expired = (
                    record.claim_expires_at is not None
                    and record.claim_expires_at <= now
                )
                available = record.claimed_by is None or claim_expired
                if record.poisoned or not available:
                    continue
                token = f"clm_{uuid4().hex}"
                record.claimed_by = worker_id
                record.claim_token = token
                record.claim_expires_at = now + claim_ttl
                record.publish_attempt += 1
                claimed.append(
                    ClaimedOutboxRecord(
                        outbox_id=str(record.outbox_id),
                        event_id=record.event_id,
                        event=record.event,
                        claim_token=token,
                        attempt=record.publish_attempt,
                    )
                )
                if len(claimed) >= limit:
                    break
        return claimed

    async def disposition_outbox(
        self,
        destination: str,
        worker_id: str,
        outbox_id: str,
        claim_token: str,
        disposition: str,
        reason: str | None = None,
    ) -> bool:
        del reason
        async with self._lock:
            record = next(
                (item for item in self._outbox if str(item.outbox_id) == outbox_id),
                None,
            )
            if (
                record is None
                or record.destination != destination
                or record.claimed_by != worker_id
                or record.claim_token != claim_token
                or record.claim_expires_at is None
                or record.claim_expires_at <= datetime.now(UTC)
            ):
                return False
            if disposition == "ack":
                record.published = True
            elif disposition == "poison":
                record.poisoned = True
            elif disposition != "nack":
                return False
            record.claimed_by = None
            record.claim_token = None
            record.claim_expires_at = None
            return True
