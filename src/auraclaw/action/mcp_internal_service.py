from __future__ import annotations

from typing import Any

from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.contracts.internal import (
    McpRegistryAdminRequest,
    McpRegistryAdminResponse,
    McpRegistrySnapshotRequest,
    McpRegistrySnapshotResponse,
)
from auraclaw.contracts.mcp_registry import (
    McpRegistryOperationKind,
    McpServerConfig,
    McpServerLifecycleCommand,
    McpServerWriteCommand,
)


class McpRegistryInternalService:
    def __init__(self, registry: McpServerRegistryService) -> None:
        self._registry = registry

    async def command(
        self, request: McpRegistryAdminRequest
    ) -> McpRegistryAdminResponse:
        kind = McpRegistryOperationKind(request.operation)
        if kind in {McpRegistryOperationKind.CREATE, McpRegistryOperationKind.UPDATE}:
            if request.config is None:
                raise ValueError("MCP write command requires config")
            config = McpServerConfig.model_validate(request.config)
            write = McpServerWriteCommand(
                command_id=request.command_id,
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
                expected_revision=request.expected_revision,
                config=config,
            )
            record = (
                await self._registry.create(write)
                if kind is McpRegistryOperationKind.CREATE
                else await self._registry.update(write)
            )
        else:
            if not request.server_id:
                raise ValueError("MCP lifecycle command requires server_id")
            lifecycle = McpServerLifecycleCommand(
                command_id=request.command_id,
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
                expected_revision=request.expected_revision,
                target_revision=request.target_revision,
            )
            handler = {
                McpRegistryOperationKind.TEST: self._registry.test,
                McpRegistryOperationKind.ENABLE: self._registry.enable,
                McpRegistryOperationKind.DISABLE: self._registry.disable,
                McpRegistryOperationKind.RECONCILE: self._registry.reconcile,
                McpRegistryOperationKind.RETIRE: self._registry.retire,
                McpRegistryOperationKind.DELETE: self._registry.delete,
            }[kind]
            record = await handler(request.server_id, lifecycle)
        return McpRegistryAdminResponse(
            operation_id=record.operation_id,
            status=record.status.value,
            server_id=record.server_id,
            target_revision=record.target_revision,
            result=dict(record.result),
            safe_error_code=record.safe_error_code,
        )

    async def snapshot(
        self, request: McpRegistrySnapshotRequest
    ) -> McpRegistrySnapshotResponse:
        del request
        entries = await self._registry.active_snapshot()
        return McpRegistrySnapshotResponse(
            servers=tuple(entry.model_dump(mode="json") for entry in entries)
        )

    async def get_server(self, tenant_id: str, server_id: str) -> dict[str, Any]:
        record = await self._registry.get_server(
            tenant_id=tenant_id, server_id=server_id, actor_id="internal"
        )
        return record.model_dump(mode="json")

    async def list_servers(self, tenant_id: str) -> tuple[dict[str, Any], ...]:
        records = await self._registry.list_servers(tenant_id=tenant_id)
        return tuple(record.model_dump(mode="json") for record in records)
