from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from auraclaw.contracts.internal import AdminOperationRequest, AdminOperationResponse


@dataclass(frozen=True)
class AdminOperationClaim:
    acquired: bool = False
    claim_token: str | None = None
    response: AdminOperationResponse | None = None


class AdminOperationStore(Protocol):
    async def get(self, operation_id: str) -> AdminOperationResponse | None: ...

    async def claim(
        self,
        request: AdminOperationRequest,
        *,
        request_digest: str,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> AdminOperationClaim: ...

    async def complete(
        self,
        request: AdminOperationRequest,
        response: AdminOperationResponse,
        *,
        claim_token: str,
        error_code: str | None = None,
    ) -> bool: ...

    async def renew(
        self,
        request: AdminOperationRequest,
        *,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> bool: ...
