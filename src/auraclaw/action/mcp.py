from __future__ import annotations

import base64
import binascii
from datetime import datetime
from typing import Any

from auraclaw.action.mcp_primitives import McpPromptRegistry, McpResourceRegistry
from auraclaw.action.ports import McpResourceReader
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.mcp import (
    MCP_PROTOCOL_VERSION,
    McpJsonRpcError,
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTrustedContext,
)
from auraclaw.contracts.tools import ArtifactRef, ToolInvocation


class HandsMcpServer:
    def __init__(
        self,
        *,
        registry: ToolRegistry,
        gateway: ToolGateway,
        resources: McpResourceRegistry | None = None,
        resource_reader: McpResourceReader | None = None,
        prompts: McpPromptRegistry | None = None,
        page_size: int = 50,
    ) -> None:
        if page_size < 1 or page_size > 100:
            raise ValueError("MCP page_size must be between 1 and 100")
        self._registry = registry
        self._gateway = gateway
        self._resources = resources or McpResourceRegistry()
        self._resource_reader = resource_reader
        self._prompts = prompts or McpPromptRegistry()
        self._page_size = page_size
        self._initialized_runtimes: set[str] = set()

    async def handle(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        try:
            if request.method == "initialize":
                return self._initialize(request, trusted_context)
            if trusted_context.runtime_id not in self._initialized_runtimes:
                return self._error(request, -32002, "MCP session is not initialized")
            if request.method == "ping":
                return McpJsonRpcResponse(id=request.id, result={})
            if request.method == "tools/list":
                return self._list_tools(request)
            if request.method == "resources/list":
                return self._list_resources(request, trusted_context)
            if request.method == "resources/templates/list":
                return self._list_resource_templates(request, trusted_context)
            if request.method == "resources/read":
                return await self._read_resource(request, trusted_context)
            if request.method == "prompts/list":
                return self._list_prompts(request, trusted_context)
            if request.method == "prompts/get":
                return self._get_prompt(request, trusted_context)
            if request.method == "tools/call":
                return await self._call_tool(request, trusted_context)
            if request.method == "notifications/cancelled":
                invocation_id = str(request.params.get("toolInvocationId", ""))
                cancelled = await self._gateway.cancel(invocation_id)
                return McpJsonRpcResponse(id=request.id, result={"cancelled": cancelled})
            return self._error(request, -32601, "MCP method not found")
        except AuraClawError as exc:
            return self._error(request, -32001, exc.message, {"code": exc.code})
        except KeyError as exc:
            return self._error(
                request,
                -32004,
                "MCP capability not found",
                {"detail": str(exc)},
            )
        except (TypeError, ValueError) as exc:
            return self._error(request, -32602, "Invalid MCP params", {"detail": str(exc)})

    def _initialize(
        self,
        request: McpJsonRpcRequest,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        requested = str(request.params.get("protocolVersion", ""))
        if requested != MCP_PROTOCOL_VERSION:
            return self._error(
                request,
                -32602,
                "Unsupported MCP protocol version",
                {"supported": [MCP_PROTOCOL_VERSION]},
            )
        self._initialized_runtimes.add(trusted_context.runtime_id)
        return McpJsonRpcResponse(
            id=request.id,
            result={
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"subscribe": False, "listChanged": False},
                    "prompts": {"listChanged": False},
                    "progress": {},
                    "cancellation": {},
                },
                "serverInfo": {"name": "auraclaw-action-hands", "version": "1"},
            },
        )

    def _list_tools(self, request: McpJsonRpcRequest) -> McpJsonRpcResponse:
        tools = []
        for capability in self._registry.discover():
            tools.append(
                {
                    "name": capability.name,
                    "description": capability.description,
                    "inputSchema": capability.input_schema,
                    "outputSchema": capability.output_schema,
                    "annotations": {
                        "readOnlyHint": capability.permission.value == "read-only",
                        "destructiveHint": capability.risk_level.value == "critical",
                    },
                    "_meta": {
                        "auraclaw": {
                            "version": capability.version,
                            "riskLevel": capability.risk_level.value,
                        }
                    },
                }
            )
        page, next_cursor = self._page(tools, request.params.get("cursor"))
        return McpJsonRpcResponse(
            id=request.id,
            result=_page_result("tools", page, next_cursor),
        )

    def _list_resources(
        self,
        request: McpJsonRpcRequest,
        trusted: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        resources = [
            resource.as_mcp()
            for resource in self._resources.discover_resources(trusted.tenant_id)
        ]
        page, next_cursor = self._page(resources, request.params.get("cursor"))
        return McpJsonRpcResponse(
            id=request.id,
            result=_page_result("resources", page, next_cursor),
        )

    def _list_resource_templates(
        self,
        request: McpJsonRpcRequest,
        trusted: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        templates = [
            template.as_mcp()
            for template in self._resources.discover_templates(trusted.tenant_id)
        ]
        page, next_cursor = self._page(templates, request.params.get("cursor"))
        return McpJsonRpcResponse(
            id=request.id,
            result=_page_result("resourceTemplates", page, next_cursor),
        )

    async def _read_resource(
        self,
        request: McpJsonRpcRequest,
        trusted: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        uri = str(request.params["uri"])
        registered = (
            await self._resource_reader.read(trusted, uri)
            if self._resource_reader is not None
            else self._resources.read(trusted.tenant_id, uri)
        )
        contents = [content.as_mcp() for content in registered]
        return McpJsonRpcResponse(id=request.id, result={"contents": contents})

    def _list_prompts(
        self,
        request: McpJsonRpcRequest,
        trusted: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        prompts = [
            prompt.as_mcp() for prompt in self._prompts.discover(trusted.tenant_id)
        ]
        page, next_cursor = self._page(prompts, request.params.get("cursor"))
        return McpJsonRpcResponse(
            id=request.id,
            result=_page_result("prompts", page, next_cursor),
        )

    def _get_prompt(
        self,
        request: McpJsonRpcRequest,
        trusted: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        name = str(request.params["name"])
        raw_arguments = dict(request.params.get("arguments", {}))
        if any(not isinstance(value, str) for value in raw_arguments.values()):
            raise ValueError("Prompt argument values must be strings")
        arguments = {str(key): value for key, value in raw_arguments.items()}
        result = self._prompts.get(
            trusted.tenant_id,
            name,
            arguments,
            trusted,
        )
        return McpJsonRpcResponse(id=request.id, result=result.as_mcp())

    def _page(
        self,
        items: list[dict[str, Any]],
        cursor: object,
    ) -> tuple[list[dict[str, Any]], str | None]:
        offset = _decode_cursor(cursor)
        if offset > len(items):
            raise ValueError("MCP cursor is outside the result set")
        next_offset = offset + self._page_size
        next_cursor = _encode_cursor(next_offset) if next_offset < len(items) else None
        return items[offset:next_offset], next_cursor

    async def _call_tool(
        self,
        request: McpJsonRpcRequest,
        trusted: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        name = str(request.params["name"])
        arguments = dict(request.params.get("arguments", {}))
        meta = dict(request.params.get("_meta", {})).get("auraclaw", {})
        if not isinstance(meta, dict):
            raise ValueError("_meta.auraclaw must be an object")
        invocation_id = str(meta.get("toolInvocationId", ""))
        if not invocation_id:
            raise ValueError("toolInvocationId is required")
        deadline = trusted.deadline
        if meta.get("deadline"):
            requested_deadline = datetime.fromisoformat(str(meta["deadline"]))
            deadline = (
                min(deadline, requested_deadline)
                if deadline is not None
                else requested_deadline
            )
        invocation = ToolInvocation(
            tool_invocation_id=invocation_id,
            tenant_id=trusted.tenant_id,
            root_session_id=trusted.root_session_id,
            session_id=trusted.session_id,
            run_id=trusted.run_id,
            tool_name=name,
            tool_version=str(meta.get("toolVersion", "1")),
            arguments=arguments,
            expected_side_effect=str(meta.get("expectedSideEffect", "read")),
            idempotency_key=str(meta.get("idempotencyKey", invocation_id)),
            deadline=deadline,
            fencing_token=trusted.fencing_token,
            actor_id=trusted.runtime_id,
            approval_id=_optional_string(meta.get("approvalId")),
            credential_ref=_optional_string(meta.get("credentialRef")),
        )
        result = await self._gateway.execute(invocation)
        serialized = result.as_dict()
        content: list[dict[str, Any]] = [{"type": "text", "text": result.summary}]
        if isinstance(result.content, ArtifactRef):
            content.append(
                {
                    "type": "resource_link",
                    "name": result.content.artifact_id,
                    "uri": f"artifact://{result.content.artifact_id}/{result.content.version}",
                    "mimeType": result.content.media_type,
                    "size": result.content.size,
                }
            )
        return McpJsonRpcResponse(
            id=request.id,
            result={
                "content": content,
                "structuredContent": serialized,
                "isError": result.status.value != "success",
                "_meta": {"auraclaw": {"toolInvocationId": invocation_id}},
            },
        )

    @staticmethod
    def _error(
        request: McpJsonRpcRequest,
        code: int,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> McpJsonRpcResponse:
        return McpJsonRpcResponse(
            id=request.id,
            error=McpJsonRpcError(code=code, message=message, data=data or {}),
        )


def _optional_string(value: object) -> str | None:
    return str(value) if value is not None else None


def _encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(offset.to_bytes(8, "big")).decode("ascii")


def _decode_cursor(cursor: object) -> int:
    if cursor is None:
        return 0
    if not isinstance(cursor, str):
        raise ValueError("MCP cursor must be a string")
    try:
        decoded = base64.b64decode(cursor, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("MCP cursor is invalid") from exc
    if len(decoded) != 8:
        raise ValueError("MCP cursor is invalid")
    return int.from_bytes(decoded, "big")


def _page_result(
    key: str,
    items: list[dict[str, Any]],
    next_cursor: str | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {key: items}
    if next_cursor is not None:
        result["nextCursor"] = next_cursor
    return result
