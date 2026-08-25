from __future__ import annotations

from typing import Annotated, Any, Protocol

from fastapi import APIRouter, Depends, Header, status

from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.api.dependencies import RequestIdentity, request_identity
from auraclaw.contracts.capabilities import CapabilityDescriptor
from auraclaw.contracts.mcp_registry import (
    McpServerConfig,
    McpServerLifecycleCommand,
    McpServerOperationRecord,
    McpServerWriteCommand,
)

Identity = Annotated[RequestIdentity, Depends(request_identity)]


class McpServerLifecycleOps(Protocol):
    async def test(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord: ...

    async def enable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord: ...

    async def disable(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord: ...

    async def reconcile(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord: ...

    async def retire(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord: ...


class McpServerToolCatalog(Protocol):
    async def list_server_tools(
        self, *, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]: ...


def create_mcp_admin_router(
    registry: McpServerRegistryService,
    *,
    lifecycle: McpServerLifecycleOps | None = None,
    catalog: McpServerToolCatalog | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/v1/admin", tags=["mcp-admin"])
    ops = lifecycle or registry

    def _write(
        identity: RequestIdentity,
        command_id: str,
        expected_revision: int,
        config: McpServerConfig,
    ) -> McpServerWriteCommand:
        return McpServerWriteCommand(
            command_id=command_id,
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            correlation_id=identity.correlation_id,
            causation_id=command_id,
            expected_revision=expected_revision,
            config=config,
        )

    def _lifecycle(
        identity: RequestIdentity,
        command_id: str,
        expected_revision: int,
        target_revision: int | None = None,
    ) -> McpServerLifecycleCommand:
        return McpServerLifecycleCommand(
            command_id=command_id,
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            correlation_id=identity.correlation_id,
            causation_id=command_id,
            expected_revision=expected_revision,
            target_revision=target_revision,
        )

    @router.post("/mcp-servers", status_code=status.HTTP_202_ACCEPTED)
    async def create_server(
        payload: dict[str, Any],
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(default=0, alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        config = McpServerConfig.model_validate(payload)
        record = await registry.create(
            _write(identity, command_id, expected_revision, config)
        )
        return record.model_dump(mode="json")

    @router.get("/mcp-servers")
    async def list_servers(identity: Identity) -> dict[str, Any]:
        servers = await registry.list_servers(tenant_id=identity.tenant_id)
        return {
            "servers": [item.model_dump(mode="json") for item in servers],
        }

    @router.get("/mcp-servers/{server_id}")
    async def get_server(server_id: str, identity: Identity) -> dict[str, Any]:
        record = await registry.get_server(
            tenant_id=identity.tenant_id,
            server_id=server_id,
            actor_id=identity.actor.id,
        )
        return record.model_dump(mode="json")

    @router.get("/mcp-servers/{server_id}/tools")
    async def list_server_tools(server_id: str, identity: Identity) -> dict[str, Any]:
        await registry.get_server(
            tenant_id=identity.tenant_id,
            server_id=server_id,
            actor_id=identity.actor.id,
        )
        tools = (
            ()
            if catalog is None
            else await catalog.list_server_tools(
                tenant_id=identity.tenant_id, server_id=server_id
            )
        )
        return {
            "server_id": server_id,
            "tools": [item.as_search_result() for item in tools],
        }

    @router.put("/mcp-servers/{server_id}", status_code=status.HTTP_202_ACCEPTED)
    async def update_server(
        server_id: str,
        payload: dict[str, Any],
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        config = McpServerConfig.model_validate({**payload, "server_id": server_id})
        record = await registry.update(
            _write(identity, command_id, expected_revision, config)
        )
        return record.model_dump(mode="json")

    async def _run(
        server_id: str,
        identity: RequestIdentity,
        command_id: str,
        expected_revision: int,
        handler: Any,
        target_revision: int | None = None,
    ) -> dict[str, Any]:
        record = await handler(
            server_id,
            _lifecycle(identity, command_id, expected_revision, target_revision),
        )
        dumped: dict[str, Any] = record.model_dump(mode="json")
        return dumped

    @router.post(
        "/mcp-servers/{server_id}:test", status_code=status.HTTP_202_ACCEPTED
    )
    async def test_server(
        server_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
        target_revision: int | None = None,
    ) -> dict[str, Any]:
        return await _run(
            server_id,
            identity,
            command_id,
            expected_revision,
            ops.test,
            target_revision,
        )

    @router.post(
        "/mcp-servers/{server_id}:enable", status_code=status.HTTP_202_ACCEPTED
    )
    async def enable_server(
        server_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
        target_revision: int | None = None,
    ) -> dict[str, Any]:
        return await _run(
            server_id,
            identity,
            command_id,
            expected_revision,
            ops.enable,
            target_revision,
        )

    @router.post(
        "/mcp-servers/{server_id}:disable", status_code=status.HTTP_202_ACCEPTED
    )
    async def disable_server(
        server_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        return await _run(
            server_id, identity, command_id, expected_revision, ops.disable
        )

    @router.post(
        "/mcp-servers/{server_id}:reconcile", status_code=status.HTTP_202_ACCEPTED
    )
    async def reconcile_server(
        server_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        return await _run(
            server_id, identity, command_id, expected_revision, ops.reconcile
        )

    @router.post(
        "/mcp-servers/{server_id}:retire", status_code=status.HTTP_202_ACCEPTED
    )
    async def retire_server(
        server_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        return await _run(
            server_id, identity, command_id, expected_revision, ops.retire
        )

    @router.get("/mcp-operations/{operation_id}")
    async def get_operation(operation_id: str, identity: Identity) -> dict[str, Any]:
        record = await registry.get_operation(
            tenant_id=identity.tenant_id, operation_id=operation_id
        )
        return record.model_dump(mode="json")

    return router
