from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import CanonicalEvent, NewEvent


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


class OutboxRelayPort(Protocol):
    async def relay_once(self, *, limit: int = 100) -> int: ...


class AdmissionController(Protocol):
    async def admit(self, *, goal: str, context: CommandContext) -> None: ...
