from __future__ import annotations

from typing import Protocol

import httpx

from auraclaw.contracts.internal import InternalRequestContext, ServiceIdentity
from auraclaw.internal.http import HttpContractClient


class InternalCommandContext(Protocol):
    @property
    def tenant_id(self) -> str: ...

    @property
    def command_id(self) -> str: ...

    @property
    def correlation_id(self) -> str: ...

    @property
    def causation_id(self) -> str: ...


class InternalContractSession:
    """Shared timeout, authentication and bounded retry policy for internal clients."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.05,
    ) -> None:
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            transport=transport,
        )
        self.contract = HttpContractClient(
            self.client,
            bearer_token=bearer_token,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        )

    async def aclose(self) -> None:
        await self.client.aclose()


def command_context(
    command: InternalCommandContext,
    *,
    service_identity: ServiceIdentity = ServiceIdentity.TASK_API,
) -> InternalRequestContext:
    return InternalRequestContext(
        tenant_id=command.tenant_id,
        service_identity=service_identity,
        request_id=command.command_id,
        correlation_id=command.correlation_id,
        causation_id=command.causation_id,
    )


def query_context(
    tenant_id: str,
    request_id: str,
    *,
    service_identity: ServiceIdentity = ServiceIdentity.TASK_API,
) -> InternalRequestContext:
    return InternalRequestContext(
        tenant_id=tenant_id,
        service_identity=service_identity,
        request_id=request_id,
        correlation_id=request_id,
        causation_id=request_id,
    )
