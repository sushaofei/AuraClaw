from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

from auraclaw.action.ports import CredentialInvoker, ResourcePolicyEvaluator
from auraclaw.contracts.capabilities import CapabilityStatus, McpServerDefinition
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.mcp import (
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTrustedContext,
)
from auraclaw.contracts.tools import PolicyDecision, ToolCapability, ToolInvocation


class ManagedRemoteMcpTransport:
    """Hands-side remote MCP transport; Credential Proxy owns network and OAuth."""

    def __init__(
        self,
        server: McpServerDefinition,
        *,
        credentials: CredentialInvoker,
        policy: ResourcePolicyEvaluator,
    ) -> None:
        if (
            not server.enabled
            or server.status
            not in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
            or server.credential_ref is None
            or server.oauth is None
        ):
            raise ValueError("remote MCP server is not callable")
        self._server = server
        self._credential_ref = server.credential_ref
        assert self._credential_ref is not None
        self._credentials = credentials
        self._policy = policy
        self._notification_handler: (
            Callable[[str, str, dict[str, Any]], Awaitable[bool]] | None
        ) = None

    def set_notification_handler(
        self,
        handler: Callable[
            [str, str, dict[str, Any]],
            Awaitable[bool],
        ],
    ) -> None:
        self._notification_handler = handler

    async def send(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        if (
            self._server.tenant_id is not None
            and self._server.tenant_id != trusted_context.tenant_id
        ):
            raise PolicyDeniedError("remote MCP server is outside tenant scope")
        request_payload = request.model_dump(mode="json")
        input_digest = hashlib.sha256(
            json.dumps(
                request_payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        evaluation = await self._policy.evaluate_action(
            tenant_id=trusted_context.tenant_id,
            subject=trusted_context.runtime_id,
            action="mcp.remote.invoke",
            resource=f"mcp:{self._server.server_id}",
            input_digest=input_digest,
            correlation_id=trusted_context.run_id,
            attributes={
                "method": request.method,
                "server_id": self._server.server_id,
                "trust_level": self._server.trust_level.value,
            },
        )
        if evaluation.decision not in {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        }:
            raise PolicyDeniedError("remote MCP policy denied invocation")
        raw_response = await self._credentials.invoke(
            tenant_id=trusted_context.tenant_id,
            session_id=trusted_context.session_id,
            tool_name=f"mcp:{self._server.server_id}",
            credential_ref=self._credential_ref,
            operation="mcp.invoke",
            request={
                **request_payload,
                "server_id": self._server.server_id,
            },
            policy_decision_id=evaluation.decision_id,
        )
        if not isinstance(raw_response, dict):
            raise ValueError("remote MCP response is not an object")
        response = dict(raw_response)
        notifications = response.pop("_auraclaw_notifications", ())
        if self._notification_handler is not None and isinstance(
            notifications, list
        ):
            for notification in notifications:
                if not isinstance(notification, dict):
                    continue
                method = notification.get("method")
                params = notification.get("params", {})
                if isinstance(method, str) and isinstance(params, dict):
                    await self._notification_handler(
                        self._server.server_id,
                        method,
                        dict(params),
                    )
        return McpJsonRpcResponse.model_validate(response)


class RemoteMcpToolExecutor:
    def __init__(
        self,
        server: McpServerDefinition,
        transport: ManagedRemoteMcpTransport,
    ) -> None:
        self._server = server
        self._transport = transport
        self.route_owner = f"mcp:{server.server_id}:tools"

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        response = await self._transport.send(
            McpJsonRpcRequest(
                id=invocation.tool_invocation_id,
                method="tools/call",
                params={
                    "name": invocation.tool_name,
                    "arguments": invocation.arguments,
                },
            ),
            trusted_context=McpTrustedContext(
                tenant_id=invocation.tenant_id,
                root_session_id=invocation.root_session_id,
                session_id=invocation.session_id,
                run_id=invocation.run_id,
                runtime_id=invocation.actor_id,
                lease_id=f"tool:{invocation.tool_invocation_id}",
                fencing_token=invocation.fencing_token,
                deadline=invocation.deadline,
            ),
        )
        if response.error is not None:
            return {
                "isError": True,
                "error": {
                    "code": response.error.code,
                    "message": response.error.message,
                },
            }
        result = dict(response.result or {})
        if result.get("isError") is True:
            raise RuntimeError("remote MCP Tool returned an execution error")
        structured = result.get("structuredContent")
        return dict(structured) if isinstance(structured, dict) else result
