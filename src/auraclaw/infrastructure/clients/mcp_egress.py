from __future__ import annotations

import uuid
from typing import Literal

import httpx

from auraclaw.contracts.internal import (
    InternalRequestContext,
    McpEgressCommandRequest,
    McpEgressCommandResponse,
    ServiceIdentity,
)
from auraclaw.contracts.mcp_registry import McpActiveSnapshotEntry
from auraclaw.internal.http import HttpContractClient


class RemoteMcpEgressClient:
    """Hands-side loader for Credential Proxy MCP egress adapters."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        service_identity: ServiceIdentity = ServiceIdentity.ACTION_HANDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)
        self._identity = service_identity

    async def aclose(self) -> None:
        await self._client.aclose()

    async def apply(self, entry: McpActiveSnapshotEntry) -> None:
        await self._command("apply", entry.server_id, entry=entry)

    async def revoke(self, server_id: str) -> None:
        await self._command("revoke", server_id)

    async def _command(
        self,
        operation: Literal["apply", "revoke"],
        server_id: str,
        *,
        entry: McpActiveSnapshotEntry | None = None,
    ) -> McpEgressCommandResponse:
        request_id = str(uuid.uuid4())
        return await self._contract.call(
            "/internal/v1/credentials/mcp-egress",
            McpEgressCommandRequest(
                context=InternalRequestContext(
                    tenant_id=(
                        entry.tenant_id
                        if entry is not None and entry.tenant_id
                        else "platform"
                    ),
                    service_identity=self._identity,
                    request_id=request_id,
                    correlation_id=f"mcp-egress-{server_id}",
                    causation_id=request_id,
                ),
                operation=operation,
                server_id=server_id,
                entry=None if entry is None else entry.model_dump(mode="json"),
            ),
            McpEgressCommandResponse,
        )
