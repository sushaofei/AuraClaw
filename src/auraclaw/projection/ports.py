from collections.abc import Sequence
from typing import Protocol

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.contracts.tools import ApprovalRecord


class ProjectionWriter(Protocol):
    async def project(self, events: Sequence[CanonicalEvent]) -> None: ...


class TaskReader(Protocol):
    async def get_task(self, tenant_id: str, session_id: str) -> dict[str, object] | None: ...


class CollaborationReader(Protocol):
    async def get(self, tenant_id: str, session_id: str) -> dict[str, object] | None: ...

    async def list_children(
        self, tenant_id: str, root_session_id: str
    ) -> list[dict[str, object]]: ...

    async def list_runnable(
        self, tenant_id: str, root_session_id: str
    ) -> list[dict[str, object]]: ...


class ProjectionRebuilder(Protocol):
    async def rebuild(
        self, events: Sequence[CanonicalEvent], tenant_id: str | None = None
    ) -> int: ...


class ApprovalViewReader(Protocol):
    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRecord | None: ...
