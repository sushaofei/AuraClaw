from __future__ import annotations

from typing import Any
from uuid import uuid4

from auraclaw.action.ports import CredentialInvoker, ResourcePolicyEvaluator
from auraclaw.contracts.capabilities import McpServerDefinition
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsPromptArgument,
    HandsPromptDescriptor,
    HandsPromptMessage,
    HandsPromptResult,
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsToolDescriptor,
    HandsToolResult,
    HandsTrustedContext,
)
from auraclaw.contracts.tools import ToolResultStatus
from auraclaw.infrastructure.connectors.mcp.transport import ManagedRemoteMcpTransport
from auraclaw.infrastructure.connectors.mcp.wire import (
    MCP_AURACLAW_DEPT_ID_META_KEY,
    MCP_AURACLAW_INVOCATION_ID_META_KEY,
    MCP_AURACLAW_TENANT_ID_META_KEY,
    MCP_AURACLAW_USER_ID_META_KEY,
    MCP_CLIENT_CAPABILITIES_META_KEY,
    MCP_CLIENT_INFO_META_KEY,
    MCP_INITIALIZE_PROTOCOL_VERSIONS,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
    McpJsonRpcRequest,
    McpTrustedContext,
)

_TOOL_RESULT_STATUSES = {status.value for status in ToolResultStatus}


class ManagedMcpConnector:
    """Downstream MCP connector. Maps MCP wire types to Hands DTOs."""

    def __init__(
        self,
        server: McpServerDefinition,
        *,
        credentials: CredentialInvoker,
        policy: ResourcePolicyEvaluator,
        max_pages: int = 100,
        max_items: int = 10_000,
    ) -> None:
        if max_pages < 1 or max_items < 1:
            raise ValueError("MCP pagination limits must be positive")
        self._server = server
        self._admitted = True
        self._transport = ManagedRemoteMcpTransport(
            server, credentials=credentials, policy=policy
        )
        self._max_pages = max_pages
        self._max_items = max_items
        # Some already-deployed MCP servers use a legacy public name while the
        # AuraClaw Skill contract uses its canonical name.  Keep this mapping
        # explicit in the server definition and translate only at this boundary.
        self._remote_tool_names: dict[str, str] = {}
        self._tool_argument_wrappers: set[str] = set()
        self._read_only_tools: set[str] = set()

    @property
    def connector_id(self) -> str:
        return f"mcp:{self._server.server_id}"

    def set_admission(self, admitted: bool) -> None:
        self._admitted = admitted

    def _assert_admitted(self) -> None:
        if not self._admitted:
            raise PolicyDeniedError("mcp_execution_blocked: server is disabled or quarantined")

    def set_notification_handler(self, handler: Any) -> None:
        self._transport.set_notification_handler(handler)

    async def snapshot(self, trusted: HandsTrustedContext) -> CapabilitySnapshot:
        mcp_trusted = _mcp_trusted(trusted)
        extra: dict[str, Any] = {}
        if self._server.protocol_revision == MCP_PROTOCOL_VERSION:
            discovery = await self._send(mcp_trusted, "server/discover", {})
            supported_versions = discovery.get("supportedVersions", [])
            if not isinstance(supported_versions, list) or (
                MCP_PROTOCOL_VERSION not in supported_versions
            ):
                raise ValueError("remote MCP protocol version is incompatible")
            capabilities = discovery.get("capabilities", {})
            if not isinstance(capabilities, dict):
                raise ValueError("remote MCP capabilities are invalid")
            extra["capabilities"] = capabilities
            if isinstance(discovery.get("serverInfo"), dict):
                extra["server_info"] = dict(discovery["serverInfo"])
        elif self._server.protocol_revision in MCP_INITIALIZE_PROTOCOL_VERSIONS:
            discovery = await self._send(
                mcp_trusted,
                "initialize",
                {
                    "protocolVersion": self._server.protocol_revision,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "auraclaw-capability-reconciler",
                        "version": "1",
                    },
                },
            )
            if discovery.get("protocolVersion") != self._server.protocol_revision:
                raise ValueError("remote MCP protocol version is incompatible")
            capabilities = discovery.get("capabilities", {})
            if not isinstance(capabilities, dict):
                raise ValueError("remote MCP capabilities are invalid")
            extra["capabilities"] = capabilities
            if isinstance(discovery.get("serverInfo"), dict):
                extra["server_info"] = dict(discovery["serverInfo"])
        else:
            raise ValueError("remote MCP protocol version is not supported")
        capabilities = extra["capabilities"]
        standard_capability_gating = (
            self._server.protocol_revision in MCP_INITIALIZE_PROTOCOL_VERSIONS
        )
        raw_tools = (
            await self._list_all(mcp_trusted, "tools/list", "tools")
            if not standard_capability_gating or "tools" in capabilities
            else []
        )
        tools = (
            tuple(self._tool_descriptor(item) for item in raw_tools)
            if raw_tools
            else ()
        )
        resources = (
            tuple(
                _resource_descriptor(item)
                for item in await self._list_all(
                    mcp_trusted, "resources/list", "resources"
                )
            )
            if not standard_capability_gating or "resources" in capabilities
            else ()
        )
        templates = (
            tuple(
                _template_descriptor(item)
                for item in await self._list_all(
                    mcp_trusted, "resources/templates/list", "resourceTemplates"
                )
            )
            if not standard_capability_gating or "resources" in capabilities
            else ()
        )
        prompts = (
            tuple(
                _prompt_descriptor(item)
                for item in await self._list_all(mcp_trusted, "prompts/list", "prompts")
            )
            if not standard_capability_gating or "prompts" in capabilities
            else ()
        )
        extra["listed_resource_uris"] = [item.uri for item in resources if item.uri]
        if (
            self._server.protocol_revision in MCP_INITIALIZE_PROTOCOL_VERSIONS
            and extra.get("capabilities", {}).get("resources", {}).get("subscribe")
            is True
        ):
            for uri in extra["listed_resource_uris"][:100]:
                try:
                    await self._send(mcp_trusted, "resources/subscribe", {"uri": uri})
                except Exception:
                    break
        return CapabilitySnapshot(
            connector_id=self.connector_id,
            tools=tools,
            resources=resources,
            resource_templates=templates,
            prompts=prompts,
            extra=extra,
        )

    async def read_resource(
        self,
        trusted: HandsTrustedContext,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]:
        self._assert_admitted()
        result = await self._send(_mcp_trusted(trusted), "resources/read", {"uri": uri})
        contents = []
        for item in result.get("contents", []):
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            blob = item.get("blob")
            if (text is None) == (blob is None):
                continue
            contents.append(
                HandsResourceContent(
                    uri=str(item.get("uri", uri)),
                    mime_type=(
                        str(item["mimeType"]) if item.get("mimeType") is not None else None
                    ),
                    text=None if text is None else str(text),
                    blob=None if blob is None else str(blob),
                )
            )
        return tuple(contents)

    async def get_prompt(
        self,
        trusted: HandsTrustedContext,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> HandsPromptResult:
        self._assert_admitted()
        result = await self._send(
            _mcp_trusted(trusted),
            "prompts/get",
            {"name": name, "arguments": dict(arguments or {})},
        )
        messages = []
        for item in result.get("messages", ()):
            if isinstance(item, dict) and item.get("role") in {"user", "assistant"}:
                messages.append(
                    HandsPromptMessage(
                        role=item["role"],
                        content=dict(item.get("content") or {}),
                    )
                )
        description = result.get("description")
        return HandsPromptResult(
            description=None if description is None else str(description),
            messages=tuple(messages),
        )

    async def call_tool(
        self,
        trusted: HandsTrustedContext,
        *,
        name: str,
        arguments: dict[str, Any],
        invocation_id: str,
    ) -> HandsToolResult:
        self._assert_admitted()
        request_meta: dict[str, Any] = {
            MCP_AURACLAW_INVOCATION_ID_META_KEY: invocation_id,
            MCP_AURACLAW_TENANT_ID_META_KEY: trusted.tenant_id,
        }
        if trusted.user_id:
            request_meta[MCP_AURACLAW_USER_ID_META_KEY] = trusted.user_id
        if trusted.dept_id:
            request_meta[MCP_AURACLAW_DEPT_ID_META_KEY] = trusted.dept_id
        if self._server.protocol_revision == MCP_PROTOCOL_VERSION:
            request_meta.update(_modern_meta())
        remote_name = self._remote_tool_names.get(name, name)
        remote_arguments = (
            {"input": dict(arguments)}
            if name in self._tool_argument_wrappers and "input" not in arguments
            else dict(arguments)
        )
        params: dict[str, Any] = {
            "name": remote_name,
            "arguments": remote_arguments,
            "_meta": request_meta,
        }
        response = await self._transport.send(
            McpJsonRpcRequest(id=invocation_id, method="tools/call", params=params),
            trusted_context=_mcp_trusted(trusted),
            read_only=name in self._read_only_tools,
        )
        if response.error is not None:
            return HandsToolResult(
                status="error",
                summary=response.error.message,
                error_code=str(response.error.code),
                side_effect_status="unknown",
            )
        result = dict(response.result or {})
        if result.get("isError") is True:
            return HandsToolResult(
                status="error",
                summary=_tool_error_summary(result),
                error_code="mcp_tool_error",
                side_effect_status="unknown",
            )
        structured = result.get("structuredContent")
        if isinstance(structured, dict):
            status = structured.get("status")
            if isinstance(status, str) and status in _TOOL_RESULT_STATUSES:
                content = structured.get("content")
                return HandsToolResult(
                    status=status,
                    content=(
                        content
                        if isinstance(content, (str, dict)) or content is None
                        else dict(structured)
                    ),
                    summary=str(structured.get("summary", "")),
                    metadata=dict(structured.get("metadata") or {}),
                    error_code=(
                        None
                        if structured.get("error_code") is None
                        else str(structured.get("error_code"))
                    ),
                    side_effect_status=str(
                        structured.get("side_effect_status", "not_started")
                    ),
                )
            return HandsToolResult(status="success", content=structured, summary="")
        return HandsToolResult(status="success", content=result, summary="")

    def _tool_descriptor(self, item: dict[str, Any]) -> HandsToolDescriptor:
        remote_name = str(item.get("name", ""))
        aliases = self._server.metadata.get("tool_name_aliases", {})
        canonical_name = (
            str(aliases[remote_name])
            if isinstance(aliases, dict) and isinstance(aliases.get(remote_name), str)
            else remote_name
        )
        if canonical_name != remote_name:
            self._remote_tool_names[canonical_name] = remote_name
        schema = item.get("inputSchema")
        if (
            canonical_name
            and isinstance(schema, dict)
            and isinstance(schema.get("properties"), dict)
            and isinstance(schema["properties"].get("input"), dict)
            and schema.get("required") == ["input"]
        ):
            self._tool_argument_wrappers.add(canonical_name)
        descriptor = _tool_descriptor(item, name=canonical_name)
        if descriptor.read_only:
            self._read_only_tools.add(canonical_name)
        else:
            self._read_only_tools.discard(canonical_name)
        return descriptor

    async def aclose(self) -> None:
        self._admitted = False

    async def _list_all(
        self,
        trusted: McpTrustedContext,
        method: str,
        key: str,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        seen: set[str] = set()
        for _ in range(self._max_pages):
            params: dict[str, Any] = {} if cursor is None else {"cursor": cursor}
            result = await self._send(trusted, method, params)
            page = result.get(key, [])
            if not isinstance(page, list):
                raise ValueError(f"remote MCP {key} list is invalid")
            items.extend(item for item in page if isinstance(item, dict))
            if len(items) > self._max_items:
                raise ValueError(f"remote MCP {key} list exceeds limit")
            raw_cursor = result.get("nextCursor")
            if raw_cursor is None:
                return items
            cursor = str(raw_cursor)
            if not cursor or cursor in seen:
                raise ValueError("remote MCP pagination cursor did not advance")
            seen.add(cursor)
        raise ValueError("remote MCP pagination exceeded page limit")

    async def _send(
        self,
        trusted: McpTrustedContext,
        method: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        request_params = dict(params)
        identity_meta = _identity_meta(trusted)
        if self._server.protocol_revision == MCP_PROTOCOL_VERSION:
            request_params["_meta"] = {**identity_meta, **_modern_meta()}
        elif identity_meta:
            request_params["_meta"] = {
                **dict(request_params.get("_meta") or {}),
                **identity_meta,
            }
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=f"mcp:{method}:{uuid4().hex}",
                method=method,
                params=request_params,
            ),
            trusted_context=trusted,
            read_only=method in {"resources/read", "prompts/get"},
        )
        if response.error is not None:
            raise ValueError(
                f"remote MCP error {response.error.code}: {response.error.message}"
            )
        return dict(response.result or {})


def _modern_meta() -> dict[str, Any]:
    return {
        MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
        MCP_CLIENT_INFO_META_KEY: {
            "name": "auraclaw-action-hands",
            "version": "1",
        },
        MCP_CLIENT_CAPABILITIES_META_KEY: {},
    }


def _tool_error_summary(result: dict[str, Any]) -> str:
    for item in result.get("content", ()):
        if isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
            return str(item["text"])[:1_000]
    return "remote MCP Tool returned an execution error"


def _mcp_trusted(trusted: HandsTrustedContext) -> McpTrustedContext:
    return McpTrustedContext(
        tenant_id=trusted.tenant_id,
        root_session_id=trusted.root_session_id,
        session_id=trusted.session_id,
        run_id=trusted.run_id,
        runtime_id=trusted.runtime_id,
        lease_id=trusted.lease_id,
        fencing_token=trusted.fencing_token,
        deadline=trusted.deadline,
        lease_assertion=trusted.lease_assertion,
        user_id=trusted.user_id,
        dept_id=trusted.dept_id,
    )


def _identity_meta(trusted: McpTrustedContext) -> dict[str, Any]:
    meta: dict[str, Any] = {MCP_AURACLAW_TENANT_ID_META_KEY: trusted.tenant_id}
    if trusted.user_id:
        meta[MCP_AURACLAW_USER_ID_META_KEY] = trusted.user_id
    if trusted.dept_id:
        meta[MCP_AURACLAW_DEPT_ID_META_KEY] = trusted.dept_id
    return meta


def _tool_descriptor(
    item: dict[str, Any],
    *,
    name: str | None = None,
) -> HandsToolDescriptor:
    meta = item.get("_meta", {})
    auraclaw = meta.get("auraclaw", {}) if isinstance(meta, dict) else {}
    version = "1"
    if isinstance(auraclaw, dict) and auraclaw.get("version"):
        version = str(auraclaw["version"])
    annotations = item.get("annotations", {})
    read_only = False
    destructive = False
    if isinstance(annotations, dict):
        read_only = annotations.get("readOnlyHint") is True
        destructive = annotations.get("destructiveHint") is True
    input_schema = item.get("inputSchema", {"type": "object"})
    output_schema = item.get("outputSchema", {"type": "object"})
    return HandsToolDescriptor(
        name=str(item.get("name", "") if name is None else name),
        version=version,
        description=str(item.get("description", "")),
        input_schema=dict(input_schema) if isinstance(input_schema, dict) else {},
        output_schema=dict(output_schema) if isinstance(output_schema, dict) else {},
        read_only=read_only,
        destructive=destructive,
        risk_level=(
            str(auraclaw["riskLevel"])
            if isinstance(auraclaw, dict) and auraclaw.get("riskLevel")
            else None
        ),
    )


def _resource_descriptor(item: dict[str, Any]) -> HandsResourceDescriptor:
    return HandsResourceDescriptor(
        name=str(item.get("name", "resource")),
        uri=str(item.get("uri", "")),
        title=None if item.get("title") is None else str(item["title"]),
        description=None if item.get("description") is None else str(item["description"]),
        mime_type=None if item.get("mimeType") is None else str(item["mimeType"]),
        size=item.get("size") if isinstance(item.get("size"), int) else None,
    )


def _template_descriptor(item: dict[str, Any]) -> HandsResourceDescriptor:
    return HandsResourceDescriptor(
        name=str(item.get("name", "resource")),
        uri_template=str(item.get("uriTemplate", "")),
        title=None if item.get("title") is None else str(item["title"]),
        description=None if item.get("description") is None else str(item["description"]),
        mime_type=None if item.get("mimeType") is None else str(item["mimeType"]),
    )


def _prompt_descriptor(item: dict[str, Any]) -> HandsPromptDescriptor:
    arguments = []
    for argument in item.get("arguments", ()):
        if isinstance(argument, dict) and argument.get("name"):
            arguments.append(
                HandsPromptArgument(
                    name=str(argument["name"]),
                    title=None if argument.get("title") is None else str(argument["title"]),
                    description=(
                        None
                        if argument.get("description") is None
                        else str(argument["description"])
                    ),
                    required=bool(argument.get("required", False)),
                )
            )
    return HandsPromptDescriptor(
        name=str(item.get("name", "")),
        title=None if item.get("title") is None else str(item["title"]),
        description=None if item.get("description") is None else str(item["description"]),
        arguments=tuple(arguments),
    )
