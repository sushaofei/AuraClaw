from __future__ import annotations

import hashlib
import json

from auraclaw.action.ports import CredentialInvoker, ResourcePolicyEvaluator
from auraclaw.contracts.capabilities import CapabilityStatus, McpServerDefinition
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.mcp import (
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTrustedContext,
)
from auraclaw.contracts.tools import PolicyDecision


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
        response = await self._credentials.invoke(
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
        return McpJsonRpcResponse.model_validate(response)
