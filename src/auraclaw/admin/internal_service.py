from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from auraclaw.admin.ports import AdminOperationStore
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.internal import (
    AdminOperationRequest,
    AdminOperationResponse,
    ServiceIdentity,
)

AdminHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


def admin_operation_request_digest(request: AdminOperationRequest) -> str:
    payload = {
        "tenant_id": request.context.tenant_id,
        "owner_service": request.owner_service.value,
        "operation": request.operation,
        "parameters": request.parameters,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class OwnerAdminService:
    def __init__(
        self,
        owner: ServiceIdentity,
        handlers: dict[str, AdminHandler] | None = None,
        store: AdminOperationStore | None = None,
        instance_id: str | None = None,
        claim_ttl: timedelta = timedelta(minutes=5),
    ) -> None:
        self._owner = owner
        self._handlers = dict(handlers or {})
        self._results: dict[str, AdminOperationResponse] = {}
        self._store = store
        self._instance_id = instance_id or f"{owner.value}-{secrets.token_hex(8)}"
        self._claim_ttl = claim_ttl

    async def _heartbeat_claim(
        self,
        request: AdminOperationRequest,
        claim_token: str,
        stop: asyncio.Event,
    ) -> bool:
        assert self._store is not None
        interval = max(0.05, self._claim_ttl.total_seconds() / 3)
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return True
            except TimeoutError:
                try:
                    renewed = await self._store.renew(
                        request,
                        claimed_by=self._instance_id,
                        claim_token=claim_token,
                        claim_ttl=self._claim_ttl,
                    )
                except Exception:
                    return False
                if not renewed:
                    return False

    async def execute(self, request: AdminOperationRequest) -> AdminOperationResponse:
        previous = (
            self._results.get(request.operation_id) if self._store is None else None
        )
        if previous is not None:
            return previous
        claim_token: str | None = None
        if self._store is not None:
            claim_token = secrets.token_urlsafe(24)
            claim = await self._store.claim(
                request,
                request_digest=admin_operation_request_digest(request),
                claimed_by=self._instance_id,
                claim_token=claim_token,
                claim_ttl=self._claim_ttl,
            )
            if not claim.acquired:
                assert claim.response is not None
                return claim.response
        if request.owner_service is not self._owner:
            response = AdminOperationResponse(
                operation_id=request.operation_id,
                status="failed",
                result={"error": "operation sent to wrong owner"},
            )
        elif request.operation not in self._handlers:
            response = AdminOperationResponse(
                operation_id=request.operation_id,
                status="failed",
                result={"error": "operation is not supported by owner"},
            )
        else:
            heartbeat_stop = asyncio.Event()
            heartbeat = (
                asyncio.create_task(
                    self._heartbeat_claim(request, claim_token, heartbeat_stop),
                    name=f"admin-operation-heartbeat-{request.operation_id}",
                )
                if self._store is not None and claim_token is not None
                else None
            )
            try:
                result = await self._handlers[request.operation](dict(request.parameters))
                response = AdminOperationResponse(
                    operation_id=request.operation_id,
                    status="completed",
                    result=result,
                )
            except Exception as exc:
                error_code = exc.code if isinstance(exc, AuraClawError) else type(exc).__name__
                response = AdminOperationResponse(
                    operation_id=request.operation_id,
                    status="failed",
                    result={"error": "admin operation failed", "error_code": error_code},
                )
            finally:
                heartbeat_stop.set()
                if heartbeat is not None and not await heartbeat:
                    response = AdminOperationResponse(
                        operation_id=request.operation_id,
                        status="failed",
                        result={
                            "error": "admin operation requires manual recovery",
                            "error_code": "unknown_side_effect",
                        },
                    )
        if self._store is None:
            self._results[request.operation_id] = response
        if self._store is not None and claim_token is not None:
            completed = await self._store.complete(
                request,
                response,
                claim_token=claim_token,
                error_code=(
                    str(response.result.get("error_code"))
                    if response.status == "failed" and response.result.get("error_code")
                    else None
                ),
            )
            if not completed:
                response = AdminOperationResponse(
                    operation_id=request.operation_id,
                    status="failed",
                    result={
                        "error": "admin operation requires manual recovery",
                        "error_code": "unknown_side_effect",
                    },
                )
                if self._store is None:
                    self._results[request.operation_id] = response
        return response
