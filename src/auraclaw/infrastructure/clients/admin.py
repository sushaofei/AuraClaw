from __future__ import annotations

import uuid
from typing import Any

import httpx

from auraclaw.contracts.internal import (
    AdminOperationRequest,
    AdminOperationResponse,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.internal.http import HttpContractClient


class RemoteAdminClient:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float = 300.0,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def execute(
        self,
        owner: ServiceIdentity,
        operation: str,
        parameters: dict[str, Any],
        *,
        tenant_id: str = "system",
        operation_id: str | None = None,
    ) -> AdminOperationResponse:
        selected_id = operation_id or str(uuid.uuid4())
        return await self._contract.call(
            "/internal/v1/admin/operations",
            AdminOperationRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=selected_id,
                    correlation_id=selected_id,
                    causation_id=selected_id,
                ),
                operation_id=selected_id,
                owner_service=owner,
                operation=operation,
                parameters=parameters,
            ),
            AdminOperationResponse,
        )
