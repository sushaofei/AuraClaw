from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.contracts.internal import (
    CredentialInvokeRequest,
    CredentialInvokeResponse,
    CredentialResourceRequest,
    CredentialResourceResponse,
    ServiceIdentity,
)
from auraclaw.contracts.tools import CredentialReference
from auraclaw.infrastructure.credentials.proxy import CredentialProxy

CredentialTargetAdapter = Callable[[dict[str, Any], str], Awaitable[Any] | Any]

# Hands evaluates a remote-invoke policy action; Credential Proxy records a
# narrower egress operation. Validation must use the decision that Hands stored.
_POLICY_ACTION_BY_OPERATION = {
    "mcp.invoke": "mcp.remote.invoke",
    "http.invoke": "java-api.remote.invoke",
}


class PolicyDecisionValidator(Protocol):
    async def validate_decision(
        self,
        *,
        tenant_id: str,
        decision_id: str,
        action: str,
        resource: str,
    ) -> bool: ...


class CredentialProxyInternalService:
    """Owns secret resolution and the allowlisted outbound target adapters."""

    def __init__(
        self,
        proxy: CredentialProxy,
        *,
        adapters: dict[str, CredentialTargetAdapter] | None = None,
        policy: PolicyDecisionValidator | None = None,
    ) -> None:
        self._proxy = proxy
        self._adapters = adapters if adapters is not None else {}
        self._policy = policy

    async def invoke(self, request: CredentialInvokeRequest) -> CredentialInvokeResponse:
        if request.context.service_identity not in {
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
        }:
            raise CredentialAccessError("workload may not invoke credentials")
        if self._policy is not None and not await self._policy.validate_decision(
            tenant_id=request.context.tenant_id,
            decision_id=request.policy_decision_id,
            action=_POLICY_ACTION_BY_OPERATION.get(request.operation, request.operation),
            resource=request.target,
        ):
            raise CredentialAccessError("policy decision is invalid or expired")
        adapter = self._adapters.get(request.target)
        if adapter is None:
            raise CredentialAccessError("credential target is not allowlisted")
        usage_id = str(uuid.uuid4())
        response = await self._proxy.invoke(
            tenant_id=request.context.tenant_id,
            session_id=request.session_id,
            tool_name=request.target,
            credential_ref=request.credential_ref,
            operation=request.operation,
            request=request.request,
            adapter=adapter,
            policy_decision_id=request.policy_decision_id,
            usage_id=usage_id,
        )
        body = response if isinstance(response, dict) else {"value": response}
        return CredentialInvokeResponse(
            usage_id=usage_id,
            status="completed",
            response=body,
        )

    async def resource(
        self, request: CredentialResourceRequest
    ) -> CredentialResourceResponse:
        if request.context.service_identity is not ServiceIdentity.TASK_API:
            raise CredentialAccessError("credential lifecycle is restricted to Task Ops")
        if request.operation == "revoke":
            await self._proxy.revoke_reference(
                request.context.tenant_id, request.credential_ref
            )
            return CredentialResourceResponse(
                credential_ref=request.credential_ref, status="revoked"
            )
        await self._proxy.save_reference(
            request.context.tenant_id,
            CredentialReference(
                credential_ref=request.credential_ref,
                provider=request.resource,
                account_scope=request.resource,
                allowed_operations=request.allowed_operations,
                expires_at=request.expires_at
                or datetime.now(UTC) + timedelta(hours=1),
            ),
        )
        return CredentialResourceResponse(
            credential_ref=request.credential_ref, status="active"
        )
