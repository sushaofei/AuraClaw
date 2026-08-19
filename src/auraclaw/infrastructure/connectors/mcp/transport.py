from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

from auraclaw.action.ports import CredentialInvoker, ResourcePolicyEvaluator
from auraclaw.contracts.capabilities import CapabilityStatus, McpAuthStrategy, McpServerDefinition
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.infrastructure.connectors.mcp.wire import (
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTrustedContext,
)


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
        ):
            raise ValueError("remote MCP server is not callable")
        if (
            server.resolved_auth_strategy is McpAuthStrategy.OAUTH_CLIENT_CREDENTIALS
            and server.oauth is None
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
        arguments = request.params.get("arguments")
        if isinstance(arguments, dict):
            declared_tenant = arguments.get("tenant_id")
            declared_user = arguments.get("user_id")
            if (
                declared_tenant is not None
                and str(declared_tenant) != trusted_context.tenant_id
            ):
                raise PolicyDeniedError("tool argument tenant_id is not an authorization source")
            if (
                declared_user is not None
                and trusted_context.user_id is not None
                and str(declared_user) != trusted_context.user_id
            ):
                raise PolicyDeniedError("tool argument user_id is not an authorization source")
        request_payload = request.model_dump(mode="json")
        identity = {
            "tenant_id": trusted_context.tenant_id,
            "user_id": trusted_context.user_id,
            "session_id": trusted_context.session_id,
            "run_id": trusted_context.run_id,
        }
        if (
            self._server.resolved_auth_strategy
            is McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT
            and not identity["user_id"]
            and request.method in {"tools/call", "resources/read", "prompts/get"}
        ):
            raise PolicyDeniedError("chaintower MCP call is missing trusted user context")
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
                "_auraclaw_identity": identity,
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
        parsed = McpJsonRpcResponse.model_validate(response)
        if parsed.id != request.id:
            raise ValueError("remote MCP response id does not match request")
        if (parsed.result is None) == (parsed.error is None):
            raise ValueError("remote MCP response must contain exactly one result or error")
        return parsed
