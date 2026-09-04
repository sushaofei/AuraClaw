from __future__ import annotations

import asyncio
import ipaddress
import socket
import uuid
from typing import Literal

import httpx

from auraclaw.contracts.errors import CredentialAccessError
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
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        # Revoke includes the server's normal five-second drain window.
        # HTTPX's five-second default expires before that cleanup can finish.
        self._client = httpx.AsyncClient(
            base_url=base_url, transport=transport, timeout=timeout, trust_env=False
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)
        self._identity = service_identity
        self._base_url = httpx.URL(base_url)
        self._transport = transport

    async def aclose(self) -> None:
        await self._client.aclose()

    async def apply(self, entry: McpActiveSnapshotEntry) -> None:
        await self._command("apply", entry.server_id, entry=entry)

    async def revoke(self, server_id: str, *, expected_revision: int | None = None) -> None:
        await self._command("revoke", server_id, expected_revision=expected_revision)

    async def _command(
        self,
        operation: Literal["apply", "revoke"],
        server_id: str,
        *,
        entry: McpActiveSnapshotEntry | None = None,
        expected_revision: int | None = None,
    ) -> McpEgressCommandResponse:
        request_id = str(uuid.uuid4())
        request = McpEgressCommandRequest(
            context=InternalRequestContext(
                tenant_id=(
                    entry.tenant_id if entry is not None and entry.tenant_id else "platform"
                ),
                service_identity=self._identity,
                request_id=request_id,
                correlation_id=f"mcp-egress-{server_id}",
                causation_id=request_id,
            ),
            operation=operation,
            server_id=server_id,
            expected_revision=expected_revision,
            entry=None if entry is None else entry.model_dump(mode="json"),
        )
        # A probe is local adapter state. Loading only one address of a Docker
        # service leaves normal invocation connections to the other replicas cold.
        targets = await self._targets()
        results = await asyncio.gather(
            *(
                self._contract.call(
                    f"{target}/internal/v1/credentials/mcp-egress",
                    request,
                    McpEgressCommandResponse,
                )
                for target in targets
            ),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return results[0]  # type: ignore[return-value]

    async def _targets(self) -> tuple[str, ...]:
        base = str(self._base_url).rstrip("/")
        host = self._base_url.host
        if self._transport is not None or host == "localhost":
            return (base,)
        try:
            ipaddress.ip_address(host)
            return (base,)
        except ValueError:
            pass
        # HTTPS routing must retain its TLS authority; this fanout is for the
        # configured internal HTTP service discovery used by the deployment.
        if self._base_url.scheme != "http":
            return (base,)
        records = await asyncio.wait_for(
            asyncio.get_running_loop().getaddrinfo(
                host, self._base_url.port or 80, type=socket.SOCK_STREAM
            ),
            timeout=5.0,
        )
        addresses = sorted({str(record[4][0]) for record in records})
        if not addresses or len(addresses) > 16:
            raise CredentialAccessError("MCP egress replica discovery is unavailable")
        return tuple(
            str(self._base_url.copy_with(host=address)).rstrip("/") for address in addresses
        )
