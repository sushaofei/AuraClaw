from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class RuntimeAssignment:
    session_id: str
    run_id: str
    runtime_id: str
    lease_id: str
    fencing_token: int
    role: str
    resource_profile: dict[str, Any]


class AgentRuntime(Protocol):
    async def execute(self, assignment: RuntimeAssignment) -> None: ...


class Orchestrator(Protocol):
    async def reconcile(self) -> None: ...
