from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Protocol

from auraclaw.contracts.internal import ModelGenerateResponse


@dataclass(frozen=True)
class ModelCallReservation:
    status: Literal["reserved", "completed", "in_progress", "conflict", "quota_exceeded"]
    cached_response: ModelGenerateResponse | None = None


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
        window: timedelta = timedelta(hours=1),
    ) -> ModelCallReservation: ...

    async def complete(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        response: ModelGenerateResponse,
    ) -> None: ...

    async def fail(
        self, *, tenant_id: str, model_call_id: str, error_code: str
    ) -> None: ...
