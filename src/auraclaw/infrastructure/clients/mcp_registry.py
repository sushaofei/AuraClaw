from __future__ import annotations

from datetime import UTC, datetime

import httpx

from auraclaw.contracts.internal import (
    InternalRequestContext,
    McpRegistryAdminRequest,
    McpRegistryAdminResponse,
    ServiceIdentity,
)
from auraclaw.contracts.mcp_registry import (
    McpRegistryOperationKind,
    McpRegistryOperationStatus,
    McpServerLifecycleCommand,
    McpServerOperationRecord,
)
from auraclaw.internal.http import HttpContractClient


class RemoteMcpRegistryClient:
    """Task API lifecycle port. Hands is the unique MCP registry writer/runtime."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def test(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle("test", server_id, command)

    async def enable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle("enable", server_id, command)

    async def disable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle("disable", server_id, command)

    async def reconcile(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle("reconcile", server_id, command)

    async def retire(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle("retire", server_id, command)

    async def delete(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord:
        return await self._lifecycle("delete", server_id, command)

    async def _lifecycle(
        self,
        operation: str,
        server_id: str,
        command: McpServerLifecycleCommand,
    ) -> McpServerOperationRecord:
        response = await self._contract.call(
            "/internal/v1/mcp-registry/command",
            McpRegistryAdminRequest(
                context=InternalRequestContext(
                    tenant_id=command.tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=command.command_id,
                    correlation_id=command.correlation_id,
                    causation_id=command.causation_id,
                ),
                command_id=command.command_id,
                actor_id=command.actor_id,
                expected_revision=command.expected_revision,
                operation=operation,
                server_id=server_id,
                target_revision=command.target_revision,
            ),
            McpRegistryAdminResponse,
        )
        now = datetime.now(UTC)
        return McpServerOperationRecord(
            operation_id=response.operation_id,
            server_id=response.server_id,
            tenant_id=command.tenant_id,
            target_revision=response.target_revision,
            command_id=command.command_id,
            actor_id=command.actor_id,
            correlation_id=command.correlation_id,
            causation_id=command.causation_id,
            operation=McpRegistryOperationKind(operation),
            status=McpRegistryOperationStatus(response.status),
            safe_error_code=response.safe_error_code,
            result=dict(response.result),
            created_at=now,
            completed_at=now,
        )
