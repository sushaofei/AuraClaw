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


class EventStore(Protocol):
    async def load(self, tenant_id: str, session_id: str) -> list[CanonicalEvent]: ...

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


class ProjectionWriter(Protocol):
    async def project(self, events: Sequence[CanonicalEvent]) -> None: ...


class TaskReader(Protocol):
    async def get_task(self, tenant_id: str, session_id: str) -> dict[str, Any] | None: ...
