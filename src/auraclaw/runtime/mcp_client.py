from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import httpx

from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.mcp import (
    MCP_PROTOCOL_VERSION,
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTransport,
    McpTrustedContext,
)
from auraclaw.contracts.skills import SkillBinding
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import ToolCall

_SKILL_RESOLVE_TOOL_NAME = "auraclaw.skills.resolve"


class HandsMcpClient:
    """Runtime Capability Client with no Tool handler or Sandbox dependency."""

    def __init__(self, transport: McpTransport) -> None:
        self._transport = transport
        self._request_id = 0
        self._initialized: set[str] = set()

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    @staticmethod
    def _trusted_context(assignment: RuntimeAssignment) -> McpTrustedContext:
        return McpTrustedContext(
            tenant_id=assignment.tenant_id,
            root_session_id=assignment.root_session_id,
            session_id=assignment.session_id,
            run_id=assignment.run_id,
            runtime_id=assignment.runtime_id,
            lease_id=assignment.lease_id,
            fencing_token=assignment.fencing_token,
            deadline=assignment.deadline,
            lease_assertion=assignment.lease_assertion,
        )

    async def initialize(self, assignment: RuntimeAssignment) -> dict[str, Any]:
        context = self._trusted_context(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="initialize",
                params={
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {"progress": {}, "cancellation": {}},
                    "clientInfo": {"name": "auraclaw-agent-runtime", "version": "1"},
                },
            ),
            trusted_context=context,
        )
        result = _unwrap(response)
        self._initialized.add(assignment.runtime_id)
        return result

    async def list_tools(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.list_tools_page(assignment, cursor=cursor)
            tools.extend(page)
            if cursor is None:
                return tools

    async def list_tools_page(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="tools/list",
                params=_cursor_params(cursor),
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return _unwrap_page(response, "tools")

    async def list_resources(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]:
        resources: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.list_resources_page(assignment, cursor=cursor)
            resources.extend(page)
            if cursor is None:
                return resources

    async def list_resources_page(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="resources/list",
                params=_cursor_params(cursor),
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return _unwrap_page(response, "resources")

    async def list_resource_templates(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]:
        templates: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.list_resource_templates_page(
                assignment, cursor=cursor
            )
            templates.extend(page)
            if cursor is None:
                return templates

    async def list_resource_templates_page(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="resources/templates/list",
                params=_cursor_params(cursor),
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return _unwrap_page(response, "resourceTemplates")

    async def read_resource(
        self,
        assignment: RuntimeAssignment,
        uri: str,
    ) -> list[dict[str, Any]]:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="resources/read",
                params={"uri": uri},
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return list(_unwrap(response).get("contents", []))

    async def list_prompts(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]:
        prompts: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.list_prompts_page(assignment, cursor=cursor)
            prompts.extend(page)
            if cursor is None:
                return prompts

    async def list_prompts_page(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="prompts/list",
                params=_cursor_params(cursor),
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return _unwrap_page(response, "prompts")

    async def get_prompt(
        self,
        assignment: RuntimeAssignment,
        name: str,
        *,
        arguments: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="prompts/get",
                params={
                    "name": name,
                    "arguments": dict(arguments or {}),
                },
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return _unwrap(response)

    async def load_skill_manifest(
        self,
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
    ) -> dict[str, Any]:
        contents = await self.load_skill_part(
            assignment,
            publisher=publisher,
            name=name,
            version=version,
            path="manifest",
        )
        if len(contents) != 1 or not isinstance(contents[0].get("text"), str):
            raise AuraClawError("Skill manifest Resource is not textual")
        try:
            value = json.loads(str(contents[0]["text"]))
        except json.JSONDecodeError as exc:
            raise AuraClawError("Skill manifest Resource is not valid JSON") from exc
        if not isinstance(value, dict):
            raise AuraClawError("Skill manifest Resource must contain an object")
        return value

    async def load_skill_part(
        self,
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
        path: str,
    ) -> list[dict[str, Any]]:
        if not path or path.startswith("/") or ".." in path.split("/") or "\\" in path:
            raise ValueError("Skill part path is unsafe")
        uri = f"skill://{publisher}/{name}/{version}/{path}"
        return await self.read_resource(assignment, uri)

    async def resolve_skill(
        self,
        assignment: RuntimeAssignment,
        *,
        name: str,
        version: str = "*",
        publisher: str | None = None,
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillBinding:
        result = await self.execute(
            assignment,
            ToolCall(
                tool_invocation_id=(
                    f"resolve_{assignment.run_id}_{name.replace('.', '_')}"
                ),
                name=_SKILL_RESOLVE_TOOL_NAME,
                version="1",
                arguments={
                    "name": name,
                    "version": version,
                    **({"publisher": publisher} if publisher is not None else {}),
                    "role": assignment.role,
                    "policy_version": "runtime",
                    "active_skill_names": list(active_skill_names),
                },
            ),
        )
        content = result.get("content")
        payload = dict(content) if isinstance(content, dict) else result
        binding = payload.get("binding")
        if not isinstance(binding, dict):
            raise AuraClawError("Skill resolver did not return a binding")
        return SkillBinding.model_validate(binding)

    async def execute(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
    ) -> dict[str, Any]:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="tools/call",
                params={
                    "name": call.name,
                    "arguments": dict(call.arguments),
                    "_meta": {
                        "auraclaw": {
                            "toolInvocationId": call.tool_invocation_id,
                            "toolVersion": call.version,
                            "expectedSideEffect": call.expected_side_effect,
                            "idempotencyKey": call.idempotency_key or call.tool_invocation_id,
                            "approvalId": call.approval_id,
                            "credentialRef": call.credential_ref,
                            "deadline": (
                                assignment.deadline.isoformat()
                                if assignment.deadline is not None
                                else None
                            ),
                        }
                    },
                },
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return dict(_unwrap(response).get("structuredContent", {}))

    async def cancel(self, assignment: RuntimeAssignment, tool_invocation_id: str) -> bool:
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=self._next_id(),
                method="notifications/cancelled",
                params={"toolInvocationId": tool_invocation_id},
            ),
            trusted_context=self._trusted_context(assignment),
        )
        return bool(_unwrap(response).get("cancelled"))

    async def _ensure_initialized(self, assignment: RuntimeAssignment) -> None:
        if assignment.runtime_id not in self._initialized:
            await self.initialize(assignment)


class HttpMcpTransport:
    def __init__(self, client: httpx.AsyncClient, *, bearer_tokens: Mapping[str, str]) -> None:
        self._client = client
        self._bearer_tokens = dict(bearer_tokens)

    async def send(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        token = self._bearer_tokens.get(trusted_context.runtime_id)
        if token is None:
            raise RuntimeError("no workload token configured for Runtime")
        response = await self._client.post(
            "/mcp",
            json=request.model_dump(mode="json"),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
                **(
                    {
                        "X-AuraClaw-Lease-Assertion": (
                            trusted_context.lease_assertion.model_dump_json()
                        )
                    }
                    if trusted_context.lease_assertion is not None
                    else {}
                ),
            },
        )
        response.raise_for_status()
        return McpJsonRpcResponse.model_validate(response.json())


def _unwrap(response: McpJsonRpcResponse) -> dict[str, Any]:
    if response.error is not None:
        raise AuraClawError(
            response.error.message,
            detail=str(response.error.data),
        )
    return dict(response.result or {})


def _unwrap_page(
    response: McpJsonRpcResponse,
    key: str,
) -> tuple[list[dict[str, Any]], str | None]:
    result = _unwrap(response)
    raw_cursor = result.get("nextCursor")
    cursor = str(raw_cursor) if raw_cursor is not None else None
    return list(result.get(key, [])), cursor


def _cursor_params(cursor: str | None) -> dict[str, Any]:
    return {"cursor": cursor} if cursor is not None else {}
