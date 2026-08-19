from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, NoReturn

import httpx

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.contracts.internal import (
    EventInput,
    InternalRequestContext,
    OutboxClaimRequest,
    OutboxClaimResponse,
    OutboxDispositionRequest,
    OutboxDispositionResponse,
    ServiceIdentity,
    SessionAppendRequest,
    SessionAppendResponse,
    SessionFeedRequest,
    SessionFeedResponse,
)
from auraclaw.contracts.state import Visibility
from auraclaw.internal.http import HttpContractClient
from auraclaw.projection.relay import OutboxItem
from auraclaw.session.ports import (
    AppendResult,
    ClaimedOutboxRecord,
    SessionSnapshot,
)


def canonical_event_from_dict(payload: dict[str, Any]) -> CanonicalEvent:
    actor = dict(payload["actor"])
    return CanonicalEvent(
        event_id=str(payload["event_id"]),
        tenant_id=str(payload["tenant_id"]),
        root_session_id=str(payload["root_session_id"]),
        session_id=str(payload["session_id"]),
        run_id=str(payload["run_id"]) if payload.get("run_id") is not None else None,
        aggregate_version=int(payload["aggregate_version"]),
        type=str(payload["type"]),
        occurred_at=datetime.fromisoformat(str(payload["occurred_at"])),
        actor=Actor(type=str(actor["type"]), id=str(actor["id"])),
        correlation_id=str(payload["correlation_id"]),
        causation_id=str(payload["causation_id"]),
        visibility=Visibility(str(payload["visibility"])),
        schema_version=int(payload["schema_version"]),
        payload=dict(payload["payload"]),
    )


class RemoteSessionEventStore:
    """Task-facing EventStore adapter that owns no Session database credentials."""

    def __init__(
        self,
        base_url: str,
        *,
        service_identity: ServiceIdentity,
        bearer_token: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._identity = service_identity
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            transport=transport,
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _unsupported(operation: str) -> NoReturn:
        raise RuntimeError(f"remote Session adapter does not permit {operation}")

    async def load(
        self,
        tenant_id: str,
        session_id: str,
        *,
        from_version: int = 1,
        event_types: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[CanonicalEvent]:
        context = InternalRequestContext(
            tenant_id=tenant_id,
            service_identity=self._identity,
            request_id=f"feed:{session_id}:{from_version}",
            correlation_id=f"feed:{session_id}",
            causation_id=f"feed:{session_id}:{from_version}",
        )
        events: list[CanonicalEvent] = []
        cursor: int | None = from_version
        remaining = limit
        page_limit = 1000 if remaining is None else min(1000, remaining)
        types = tuple(event_types) if event_types is not None else None
        while cursor is not None:
            response = await self._contract.call(
                "/internal/v1/session/feed",
                SessionFeedRequest(
                    context=context,
                    session_id=session_id,
                    from_version=cursor,
                    limit=page_limit,
                    event_types=types,
                ),
                SessionFeedResponse,
            )
            batch = [canonical_event_from_dict(dict(event)) for event in response.events]
            events.extend(batch)
            if remaining is not None:
                remaining -= len(batch)
                if remaining <= 0:
                    return events[:limit]
                page_limit = min(1000, remaining)
            cursor = response.next_version
        return events

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
        response = await self._contract.call(
            "/internal/v1/session/append",
            SessionAppendRequest(
                context=InternalRequestContext(
                    tenant_id=context.tenant_id,
                    service_identity=self._identity,
                    request_id=context.command_id,
                    correlation_id=context.correlation_id,
                    causation_id=context.causation_id or context.command_id,
                ),
                root_session_id=root_session_id,
                session_id=session_id,
                run_id=run_id,
                command_id=context.command_id,
                expected_version=context.expected_version,
                operation=context.operation,
                actor_type=context.actor.type,
                actor_id=context.actor.id,
                events=tuple(
                    EventInput(
                        type=event.type,
                        payload=dict(event.payload),
                        visibility=event.visibility.value,
                    )
                    for event in events
                ),
                command_result=dict(command_result),
            ),
            SessionAppendResponse,
        )
        return AppendResult(
            events=[canonical_event_from_dict(dict(event)) for event in response.events],
            command_result=dict(response.command_result),
            deduplicated=response.deduplicated,
        )

    async def load_all(self, tenant_id: str | None = None) -> list[CanonicalEvent]:
        del tenant_id
        self._unsupported("load_all")

    async def get_snapshot(
        self, tenant_id: str, session_id: str
    ) -> SessionSnapshot | None:
        del tenant_id, session_id
        return None

    async def save_snapshot(self, snapshot: SessionSnapshot) -> None:
        del snapshot
        # Session owns snapshots. Callers rebuild aggregates from the versioned feed.

    async def claim_outbox(
        self,
        destination: str,
        worker_id: str,
        *,
        limit: int,
        claim_ttl: timedelta,
        wait_seconds: float = 0,
    ) -> list[ClaimedOutboxRecord]:
        context = InternalRequestContext(
            tenant_id="system",
            service_identity=self._identity,
            request_id=f"claim:{destination}:{worker_id}",
            correlation_id=f"outbox:{destination}",
            causation_id=f"claim:{worker_id}",
        )
        response = await self._contract.call(
            "/internal/v1/session/outbox/claim",
            OutboxClaimRequest(
                context=context,
                destination=destination,
                worker_id=worker_id,
                limit=limit,
                claim_ttl_seconds=max(1, int(claim_ttl.total_seconds())),
                wait_seconds=wait_seconds,
            ),
            OutboxClaimResponse,
        )
        return [
            ClaimedOutboxRecord(
                outbox_id=record.outbox_id,
                event_id=record.event_id,
                event=canonical_event_from_dict(record.event),
                claim_token=record.claim_token,
                attempt=record.attempt,
            )
            for record in response.records
        ]

    async def disposition_outbox(
        self,
        destination: str,
        worker_id: str,
        outbox_id: str,
        claim_token: str,
        disposition: str,
        reason: str | None = None,
    ) -> bool:
        context = InternalRequestContext(
            tenant_id="system",
            service_identity=self._identity,
            request_id=f"disposition:{outbox_id}:{claim_token}",
            correlation_id=f"outbox:{destination}",
            causation_id=f"claim:{worker_id}",
        )
        response = await self._contract.call(
            "/internal/v1/session/outbox/disposition",
            OutboxDispositionRequest(
                context=context,
                destination=destination,
                worker_id=worker_id,
                outbox_id=outbox_id,
                claim_token=claim_token,
                disposition=disposition,
                reason=reason,
            ),
            OutboxDispositionResponse,
        )
        return response.accepted


@dataclass
class RemoteOutboxItem:
    outbox_id: int
    event: CanonicalEvent


class RemoteSessionOutboxSource:
    """Projection relay source backed only by Session claim/disposition APIs."""

    def __init__(
        self,
        session: RemoteSessionEventStore,
        *,
        worker_id: str,
        wait_seconds: float = 0,
    ) -> None:
        self._session = session
        self._worker_id = worker_id
        self._wait_seconds = max(0.0, wait_seconds)
        self._claims: dict[int, str] = {}

    async def pending_outbox(self) -> Sequence[OutboxItem]:
        records = await self._session.claim_outbox(
            "projection",
            self._worker_id,
            limit=100,
            claim_ttl=timedelta(seconds=30),
            wait_seconds=self._wait_seconds,
        )
        items: list[OutboxItem] = []
        for record in records:
            outbox_id = int(record.outbox_id)
            self._claims[outbox_id] = record.claim_token
            items.append(RemoteOutboxItem(outbox_id=outbox_id, event=record.event))
        return items

    async def mark_outbox_published(self, outbox_id: int) -> None:
        token = self._claims.pop(outbox_id)
        accepted = await self._session.disposition_outbox(
            "projection", self._worker_id, str(outbox_id), token, "ack"
        )
        if not accepted:
            raise RuntimeError("Session rejected projection outbox acknowledgement")

    async def mark_outbox_failed(self, outbox_id: int) -> None:
        token = self._claims.pop(outbox_id)
        accepted = await self._session.disposition_outbox(
            "projection", self._worker_id, str(outbox_id), token, "nack"
        )
        if not accepted:
            raise RuntimeError("Session rejected projection outbox nack")


class RemoteSessionDeliveryOutboxSource:
    """Delivery source backed by claim tokens owned by Session."""

    def __init__(
        self,
        session: RemoteSessionEventStore,
        *,
        worker_id: str,
        wait_seconds: float = 0,
    ) -> None:
        self._session = session
        self._worker_id = worker_id
        self._wait_seconds = max(0.0, wait_seconds)
        self._claims: dict[int, str] = {}

    async def pending_delivery_outbox(self) -> Sequence[RemoteOutboxItem]:
        records = await self._session.claim_outbox(
            "delivery",
            self._worker_id,
            limit=100,
            claim_ttl=timedelta(seconds=30),
            wait_seconds=self._wait_seconds,
        )
        items: list[RemoteOutboxItem] = []
        for record in records:
            outbox_id = int(record.outbox_id)
            self._claims[outbox_id] = record.claim_token
            items.append(RemoteOutboxItem(outbox_id=outbox_id, event=record.event))
        return items

    async def mark_outbox_published(self, outbox_id: int) -> None:
        token = self._claims.pop(outbox_id)
        if not await self._session.disposition_outbox(
            "delivery", self._worker_id, str(outbox_id), token, "ack"
        ):
            raise RuntimeError("Session rejected delivery outbox acknowledgement")

    async def mark_outbox_failed(self, outbox_id: int) -> None:
        token = self._claims.pop(outbox_id)
        if not await self._session.disposition_outbox(
            "delivery", self._worker_id, str(outbox_id), token, "nack"
        ):
            raise RuntimeError("Session rejected delivery outbox nack")


class NoOpOutboxRelay:
    """Task API never projects Session writes in the production process."""

    async def relay_once(self, *, limit: int = 100) -> int:
        del limit
        return 0
