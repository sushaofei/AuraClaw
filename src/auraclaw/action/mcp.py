from __future__ import annotations

import base64
import binascii
from datetime import UTC, datetime
from typing import Any

from auraclaw.action.mcp_primitives import McpPromptRegistry, McpResourceRegistry
from auraclaw.action.ports import McpResourceReader
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.mcp import (
    MCP_CLIENT_CAPABILITIES_META_KEY,
    MCP_CLIENT_INFO_META_KEY,
    MCP_LEGACY_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
    MCP_SERVER_INFO_META_KEY,
    MCP_SUPPORTED_PROTOCOL_VERSIONS,
    McpJsonRpcError,
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTrustedContext,
)
from auraclaw.contracts.tools import ArtifactRef, ToolInvocation


class _UnsupportedProtocolVersion(Exception):
    def __init__(self, requested: str) -> None:
        self.requested = requested


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

    async def handle(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        try:
            if request.method == "initialize":
                return self._initialize(request, trusted_context)
            modern = self._validate_protocol(request, trusted_context)
            if request.method == "server/discover":
                return self._decorate(self._discover(request), cacheable=True)
            if request.method == "ping":
                response = McpJsonRpcResponse(id=request.id, result={})
                return self._decorate(response) if modern else response
            if request.method == "tools/list":
                response = self._list_tools(request)
                return self._decorate(response, cacheable=True) if modern else response
            if request.method == "resources/list":
                response = self._list_resources(request, trusted_context)
                return self._decorate(response, cacheable=True) if modern else response
            if request.method == "resources/templates/list":
                response = self._list_resource_templates(request, trusted_context)
                return self._decorate(response, cacheable=True) if modern else response
            if request.method == "resources/read":
                response = await self._read_resource(request, trusted_context)
                return self._decorate(response, cacheable=True) if modern else response
            if request.method == "prompts/list":
                response = self._list_prompts(request, trusted_context)
                return self._decorate(response, cacheable=True) if modern else response
            if request.method == "prompts/get":
                response = self._get_prompt(request, trusted_context)
                return self._decorate(response) if modern else response
            if request.method == "tools/call":
                response = await self._call_tool(request, trusted_context)
                return self._decorate(response) if modern else response
            if request.method == "com.auraclaw/invocations/cancel":
                invocation_id = str(request.params.get("toolInvocationId", ""))
                cancelled = await self._gateway.cancel(invocation_id)
                response = McpJsonRpcResponse(
                    id=request.id, result={"cancelled": cancelled}
                )
                return self._decorate(response) if modern else response
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
        except _UnsupportedProtocolVersion as exc:
            return self._error(
                request,
                -32022,
                "Unsupported MCP protocol version",
                {
                    "requested": exc.requested,
                    "supported": list(MCP_SUPPORTED_PROTOCOL_VERSIONS),
                },
            )

    def _initialize(
        self,
        request: McpJsonRpcRequest,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        requested = str(request.params.get("protocolVersion", ""))
        if requested != MCP_LEGACY_PROTOCOL_VERSION:
            return self._error(
                request,
                -32022,
                "Unsupported MCP protocol version",
                {"supported": list(MCP_SUPPORTED_PROTOCOL_VERSIONS)},
            )
        del trusted_context
        return McpJsonRpcResponse(
            id=request.id,
            result={
                "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                "capabilities": self._capabilities(),
                "serverInfo": {"name": "auraclaw-action-hands", "version": "1"},
            },
        )

    def _validate_protocol(
        self,
        request: McpJsonRpcRequest,
        trusted_context: McpTrustedContext,
    ) -> bool:
        raw_meta = request.params.get("_meta")
        meta = raw_meta if isinstance(raw_meta, dict) else None
        requested = None if meta is None else meta.get(MCP_PROTOCOL_VERSION_META_KEY)
        if requested is None:
            # Legacy HTTP calls are authenticated before dispatch and may land
            # on a different replica than their initialize request. tools/call
            # still carries `_meta.auraclaw`; only the protocol version key
            # marks a request as the 2026-07-28 profile.
            del trusted_context
            return False
        assert meta is not None
        if requested != MCP_PROTOCOL_VERSION:
            raise _UnsupportedProtocolVersion(str(requested))
        client_info = meta.get(MCP_CLIENT_INFO_META_KEY)
        capabilities = meta.get(MCP_CLIENT_CAPABILITIES_META_KEY)
        if client_info is not None and not isinstance(client_info, dict):
            raise ValueError("MCP clientInfo metadata must be an object")
        if not isinstance(capabilities, dict):
            raise ValueError("MCP clientCapabilities metadata is required")
        return True

    def _discover(self, request: McpJsonRpcRequest) -> McpJsonRpcResponse:
        return McpJsonRpcResponse(
            id=request.id,
            result={
                "supportedVersions": [MCP_PROTOCOL_VERSION],
                "capabilities": self._capabilities(),
            },
        )

    @staticmethod
    def _capabilities() -> dict[str, Any]:
        return {
            "tools": {"listChanged": False},
            "resources": {"subscribe": False, "listChanged": False},
            "prompts": {"listChanged": False},
        }

    @staticmethod
    def _decorate(
        response: McpJsonRpcResponse,
        *,
        cacheable: bool = False,
    ) -> McpJsonRpcResponse:
        if response.result is None:
            return response
        result = dict(response.result)
        result.setdefault("resultType", "complete")
        raw_meta = result.get("_meta")
        result_meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}
        result_meta[MCP_SERVER_INFO_META_KEY] = {
            "name": "auraclaw-action-hands",
            "version": "1",
        }
        result["_meta"] = result_meta
        if cacheable:
            result.setdefault("ttlMs", 0)
            result.setdefault("cacheScope", "private")
        return response.model_copy(update={"result": result})

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
        deadline = _optional_utc(trusted.deadline)
        if meta.get("deadline"):
            requested_deadline = _as_utc(datetime.fromisoformat(str(meta["deadline"])))
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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _optional_utc(value: datetime | None) -> datetime | None:
    return None if value is None else _as_utc(value)


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
