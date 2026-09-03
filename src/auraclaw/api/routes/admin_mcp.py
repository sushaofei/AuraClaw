from __future__ import annotations

from typing import Annotated, Any, Protocol

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

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


def _server_config(payload: dict[str, Any]) -> McpServerConfig:
    try:
        return McpServerConfig.model_validate(payload)
    except ValidationError as exc:
        errors = exc.errors(include_url=False, include_context=False, include_input=False)
        for error in errors:
            error["loc"] = ("body", *error["loc"])
        raise RequestValidationError(errors) from exc


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

    async def delete(
        self, server_id: str, command: McpServerLifecycleCommand
    ) -> McpServerOperationRecord: ...

    async def test_capability(
        self,
        *,
        tenant_id: str,
        actor_id: str,
        dept_id: str | None,
        correlation_id: str,
        server_id: str,
        capability_id: str,
        input_payload: dict[str, Any],
        expected_output: Any = None,
    ) -> dict[str, Any]: ...


class McpServerToolCatalog(Protocol):
    async def list_server_tools(
        self, *, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]: ...

    async def publication_status(
        self, *, tenant_id: str, server_id: str
    ) -> dict[str, object] | None: ...

    async def list_server_capabilities(
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

    # Gateway-safe aliases for enterprise WAFs that block canonical lifecycle verbs.
    _LIFECYCLE_OPERATION_ALIASES: dict[str, str] = {
        "check": "test",
        "open": "enable",
        "close": "disable",
        "rollback": "reconcile",
        "finalize": "retire",
        "drop": "delete",
    }

    def _resolve_lifecycle_operation(raw: str) -> str:
        normalized = raw.strip().lower()
        return _LIFECYCLE_OPERATION_ALIASES.get(normalized, normalized)

    def _metadata_lifecycle_action(payload: dict[str, Any]) -> str | None:
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            return None
        value = metadata.get("mcp_action")
        return None if value is None else str(value)

    def _extract_lifecycle_operation(payload: dict[str, Any]) -> str | None:
        metadata_action = _metadata_lifecycle_action(payload)
        if metadata_action is not None:
            return _resolve_lifecycle_operation(metadata_action)
        for key in ("action", "lifecycle_action", "operation"):
            value = payload.get(key)
            if value is not None:
                return _resolve_lifecycle_operation(str(value))
        return None

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
        mcp_action: str | None = Header(default=None, alias="X-Mcp-Action"),
    ) -> dict[str, Any]:
        lifecycle = mcp_action or _extract_lifecycle_operation(payload)
        if lifecycle is not None:
            return await _lifecycle_from_payload(
                payload,
                identity=identity,
                command_id=command_id,
                expected_revision=expected_revision,
                operation=lifecycle,
            )
        config = _server_config(payload)
        record = await registry.create(
            _write(identity, command_id, expected_revision, config)
        )
        return record.model_dump(mode="json")

    @router.post("/mcp-servers/lifecycle", status_code=status.HTTP_202_ACCEPTED)
    async def lifecycle_command(
        payload: dict[str, Any],
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        operation = _extract_lifecycle_operation(payload) or ""
        return await _lifecycle_from_payload(
            payload,
            identity=identity,
            command_id=command_id,
            expected_revision=expected_revision,
            operation=operation,
        )

    async def _lifecycle_from_payload(
        payload: dict[str, Any],
        *,
        identity: RequestIdentity,
        command_id: str,
        expected_revision: int,
        operation: str,
    ) -> dict[str, Any]:
        server_id = str(payload.get("server_id", ""))
        if not server_id or not operation:
            raise HTTPException(
                status_code=400,
                detail="MCP lifecycle command requires server_id and operation",
            )
        target_revision = payload.get("target_revision")
        parsed_target = None if target_revision is None else int(target_revision)
        handlers = {
            "test": ops.test,
            "enable": ops.enable,
            "disable": ops.disable,
            "reconcile": ops.reconcile,
            "retire": ops.retire,
            "delete": getattr(ops, "delete", registry.delete),
        }
        handler = handlers.get(operation)
        if handler is None:
            raise HTTPException(
                status_code=400,
                detail=f"unsupported MCP lifecycle operation: {operation}",
            )
        return await _run(
            server_id,
            identity,
            command_id,
            expected_revision,
            handler,
            parsed_target,
        )

    @router.get("/mcp-servers")
    async def list_servers(identity: Identity) -> dict[str, Any]:
        servers = await registry.list_servers(tenant_id=identity.tenant_id)
        return {
            "servers": [
                await _server_payload(item, identity.tenant_id) for item in servers
            ],
        }

    @router.get("/mcp-servers/{server_id}")
    async def get_server(server_id: str, identity: Identity) -> dict[str, Any]:
        record = await registry.get_server(
            tenant_id=identity.tenant_id,
            server_id=server_id,
            actor_id=identity.actor.id,
        )
        return await _server_payload(record, identity.tenant_id)

    async def _server_payload(record: Any, tenant_id: str) -> dict[str, Any]:
        payload = dict(record.model_dump(mode="json"))
        payload["catalog_publication"] = (
            None
            if catalog is None
            else await catalog.publication_status(
                tenant_id=tenant_id, server_id=str(record.server_id)
            )
        )
        return payload

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

    @router.get("/mcp-servers/{server_id}/capabilities")
    async def list_server_capabilities(
        server_id: str, identity: Identity
    ) -> dict[str, Any]:
        await registry.get_server(
            tenant_id=identity.tenant_id,
            server_id=server_id,
            actor_id=identity.actor.id,
        )
        capabilities = (
            ()
            if catalog is None
            else await catalog.list_server_capabilities(
                tenant_id=identity.tenant_id, server_id=server_id
            )
        )
        return {
            "server_id": server_id,
            "capabilities": [
                _admin_capability_payload(item) for item in capabilities
            ],
        }

    @router.post("/mcp-servers/{server_id}/capabilities/{capability_id}:test")
    async def test_server_capability(
        server_id: str,
        capability_id: str,
        payload: dict[str, Any],
        identity: Identity,
    ) -> dict[str, Any]:
        await registry.get_server(
            tenant_id=identity.tenant_id,
            server_id=server_id,
            actor_id=identity.actor.id,
        )
        input_payload = payload.get("input", {})
        if not isinstance(input_payload, dict):
            raise HTTPException(status_code=422, detail="input must be a JSON object")
        tester = getattr(ops, "test_capability", None)
        if tester is None:
            raise HTTPException(
                status_code=501, detail="MCP capability testing is unavailable"
            )
        result = await tester(
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            dept_id=identity.dept_id,
            correlation_id=identity.correlation_id,
            server_id=server_id,
            capability_id=capability_id,
            input_payload=dict(input_payload),
            expected_output=payload.get("expected_output"),
        )
        return dict(result)

    @router.put("/mcp-servers/{server_id}", status_code=status.HTTP_202_ACCEPTED)
    async def update_server(
        server_id: str,
        payload: dict[str, Any],
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        lifecycle = _extract_lifecycle_operation(payload)
        if lifecycle is not None:
            return await _lifecycle_from_payload(
                {**payload, "server_id": server_id},
                identity=identity,
                command_id=command_id,
                expected_revision=expected_revision,
                operation=lifecycle,
            )
        config = _server_config({**payload, "server_id": server_id})
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

    def _register_lifecycle(
        action: str,
        handler: Any,
        *,
        allow_target_revision: bool = False,
    ) -> None:
        async def run_lifecycle(
            server_id: str,
            identity: Identity,
            command_id: str = Header(alias="Idempotency-Key"),
            expected_revision: int = Header(alias="X-Expected-Revision"),
            target_revision: int | None = None,
        ) -> dict[str, Any]:
            resolved_target = target_revision if allow_target_revision else None
            return await _run(
                server_id,
                identity,
                command_id,
                expected_revision,
                handler,
                resolved_target,
            )

        for path in (
            f"/mcp-servers/{{server_id}}:{action}",
            f"/mcp-servers/{{server_id}}/{action}",
        ):
            router.add_api_route(
                path,
                run_lifecycle,
                methods=["POST"],
                status_code=status.HTTP_202_ACCEPTED,
                name=f"mcp_{action.replace('-', '_')}_{path.rsplit('/', 1)[-1]}",
            )

    _register_lifecycle("test", ops.test, allow_target_revision=True)
    _register_lifecycle("enable", ops.enable, allow_target_revision=True)
    _register_lifecycle("disable", ops.disable)
    _register_lifecycle("reconcile", ops.reconcile)
    _register_lifecycle("retire", ops.retire)
    delete_handler = getattr(ops, "delete", registry.delete)
    _register_lifecycle("delete", delete_handler)

    @router.delete("/mcp-servers/{server_id}", status_code=status.HTTP_202_ACCEPTED)
    async def delete_server(
        server_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision"),
    ) -> dict[str, Any]:
        return await _run(
            server_id, identity, command_id, expected_revision, delete_handler
        )

    @router.get("/mcp-operations/{operation_id}")
    async def get_operation(operation_id: str, identity: Identity) -> dict[str, Any]:
        record = await registry.get_operation(
            tenant_id=identity.tenant_id, operation_id=operation_id
        )
        return record.model_dump(mode="json")

    return router


def _admin_capability_payload(item: CapabilityDescriptor) -> dict[str, Any]:
    payload = item.as_search_result()
    payload["read_only"] = item.permission == "read-only"
    payload["enabled"] = item.status.value == "active"
    source = item.metadata.get("source")
    if not isinstance(source, dict):
        return payload
    if item.kind.value == "tool":
        input_schema = source.get("inputSchema")
        output_schema = source.get("outputSchema")
        payload["input_schema"] = input_schema if isinstance(input_schema, dict) else {}
        payload["output_schema"] = output_schema if isinstance(output_schema, dict) else {}
    elif item.kind.value in {"resource", "resource_template"}:
        for source_key, target_key in (
            ("uri", "uri"),
            ("uri_template", "uri_template"),
            ("mime_type", "mime_type"),
            ("size", "size"),
        ):
            value = source.get(source_key)
            if value is not None:
                payload[target_key] = value
    elif item.kind.value == "prompt":
        arguments = source.get("arguments")
        payload["arguments"] = arguments if isinstance(arguments, list) else []
    return payload
