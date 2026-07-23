from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import CanonicalEvent, NewEvent
from auraclaw.contracts.tools import ApprovalRecord


@dataclass(frozen=True)
class AppendResult:
    events: list[CanonicalEvent]
    command_result: dict[str, Any]
    deduplicated: bool = False


@dataclass(frozen=True)
class SessionSnapshot:
    tenant_id: str
    session_id: str
    aggregate_version: int
    schema_version: int
    state: dict[str, Any]


@dataclass(frozen=True)
class ClaimedOutboxRecord:
    outbox_id: str
    event_id: str
    event: CanonicalEvent
    claim_token: str
    attempt: int


class EventStore(Protocol):
    async def load(
        self, tenant_id: str, session_id: str, *, from_version: int = 1
    ) -> list[CanonicalEvent]: ...

    async def load_all(self, tenant_id: str | None = None) -> list[CanonicalEvent]: ...

    async def get_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None: ...

    async def save_snapshot(self, snapshot: SessionSnapshot) -> None: ...

    async def append(
        self,
        *,
        root_session_id: str,
        session_id: str,
        run_id: str | None,
        context: CommandContext,
        events: Sequence[NewEvent],
        command_result: dict[str, Any],
    ) -> AppendResult: ...

    async def claim_outbox(
        self,
        destination: str,
        worker_id: str,
        *,
        limit: int,
        claim_ttl: timedelta,
    ) -> list[ClaimedOutboxRecord]: ...

    async def disposition_outbox(
        self,
        destination: str,
        worker_id: str,
        outbox_id: str,
        claim_token: str,
        disposition: str,
        reason: str | None = None,
    ) -> bool: ...


class OutboxRelayPort(Protocol):
    async def relay_once(self, *, limit: int = 100) -> int: ...


class AdmissionController(Protocol):
    async def admit(self, *, goal: str, context: CommandContext) -> None: ...


class HumanApprovalNotifier(Protocol):
    async def record_human_response(
        self, record: ApprovalRecord, *, decision: str, feedback: str | None
    ) -> None: ...
