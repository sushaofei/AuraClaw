from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Protocol

from auraclaw.contracts.internal import ModelGenerateResponse


@dataclass(frozen=True)
class ModelCallReservation:
    status: Literal[
        "reserved",
        "completed",
        "in_progress",
        "conflict",
        "quota_exceeded",
        "cancelled",
        "reconciling",
    ]
    cached_response: ModelGenerateResponse | None = None
    claim_token: str | None = None


@dataclass(frozen=True)
class ModelCallExecution:
    status: str
    owned: bool
    cancel_requested: bool = False
    error_code: str | None = None


@dataclass(frozen=True)
class ModelCancellation:
    status: str
    requested: bool
    execution_owner: str | None = None


class ModelStateStore(Protocol):
    async def reserve(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        run_id: str,
        request_digest: str,
        reserved_tokens: int,
        token_limit: int,
        execution_owner: str = "model-gateway",
        provider_request_ref: str | None = None,
        actor: str = "model-gateway",
        correlation_id: str = "model-call",
        causation_id: str = "model-call",
        claim_ttl: timedelta = timedelta(seconds=30),
        window: timedelta = timedelta(hours=1),
    ) -> ModelCallReservation: ...

    async def complete(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        response: ModelGenerateResponse,
        claim_token: str | None = None,
    ) -> None: ...

    async def fail(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        error_code: str,
        claim_token: str | None = None,
    ) -> None: ...

    async def heartbeat(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        execution_owner: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> ModelCallExecution: ...

    async def request_cancel(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        run_id: str,
        actor: str = "agent-runtime",
        correlation_id: str = "model-cancel",
        causation_id: str = "model-cancel",
    ) -> ModelCancellation: ...

    async def mark_cancelled(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        execution_owner: str,
        claim_token: str,
        usage: dict[str, int | float],
    ) -> bool: ...

    async def mark_reconciling(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        execution_owner: str,
        claim_token: str,
        error_code: str,
    ) -> bool: ...
