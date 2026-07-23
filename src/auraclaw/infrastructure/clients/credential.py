from __future__ import annotations

import uuid
from typing import Any

import httpx

from auraclaw.action.ports import CredentialAdapter
from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.contracts.internal import (
    CredentialInvokeRequest,
    CredentialInvokeResponse,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.infrastructure.credentials.proxy import SecretRedactor
from auraclaw.internal.http import HttpContractClient


class RemoteCredentialProxy:
    """Hands-side credential port; secrets and target adapters stay remote."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        service_identity: ServiceIdentity = ServiceIdentity.ACTION_HANDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)
        self._identity = service_identity
        self._redactor = SecretRedactor()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def invoke(
        self,
        *,
        tenant_id: str,
        session_id: str,
        tool_name: str,
        credential_ref: str,
        operation: str,
        request: dict[str, Any],
        adapter: CredentialAdapter | None = None,
        policy_decision_id: str | None = None,
    ) -> Any:
        del adapter
        if not policy_decision_id:
            raise CredentialAccessError("credential invocation requires policy decision")
        response = await self._contract.call(
            "/internal/v1/credentials/invoke",
            CredentialInvokeRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=self._identity,
                    request_id=str(uuid.uuid4()),
                    correlation_id=session_id,
                    causation_id=policy_decision_id,
                ),
                session_id=session_id,
                credential_ref=credential_ref,
                operation=operation,
                target=tool_name,
                method=operation,
                policy_decision_id=policy_decision_id,
                request=request,
            ),
            CredentialInvokeResponse,
        )
        return response.response

    def redact(self, value: Any) -> Any:
        return self._redactor.redact(value)
