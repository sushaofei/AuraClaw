from __future__ import annotations

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
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import ToolCall


class HandsMcpClient:
    """Agent Runtime MCP Client; it has no Tool handler or Sandbox dependency."""

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
        await self._ensure_initialized(assignment)
        response = await self._transport.send(
            McpJsonRpcRequest(id=self._next_id(), method="tools/list"),
            trusted_context=self._trusted_context(assignment),
        )
        return list(_unwrap(response).get("tools", []))

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
