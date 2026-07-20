from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Protocol

from auraclaw.contracts.tools import ArtifactRef, ToolCapability, ToolInvocation

CredentialAdapter = Callable[[dict[str, Any], str], Awaitable[Any] | Any]


class HandsExecutor(Protocol):
    async def execute(self, invocation: ToolInvocation, capability: ToolCapability) -> Any: ...


class ArtifactWriter(Protocol):
    async def put(
        self,
        *,
        tenant_id: str,
        root_session_id: str,
        session_id: str,
        content: bytes,
        artifact_type: str,
        media_type: str,
        name: str,
        producer: str,
        lineage_refs: tuple[str, ...] = (),
        classification: str = "internal",
        acl: tuple[str, ...] = (),
        retention_until: datetime | None = None,
    ) -> ArtifactRef: ...


class CredentialInvoker(Protocol):
    async def invoke(
        self,
        *,
        tenant_id: str,
        session_id: str,
        tool_name: str,
        credential_ref: str,
        operation: str,
        request: dict[str, Any],
        adapter: CredentialAdapter,
    ) -> Any: ...

    def redact(self, value: Any) -> Any: ...
