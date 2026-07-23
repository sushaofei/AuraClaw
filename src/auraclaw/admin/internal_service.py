from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from auraclaw.contracts.internal import (
    AdminOperationRequest,
    AdminOperationResponse,
    ServiceIdentity,
)

AdminHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class AdminOperationStore(Protocol):
    async def get(self, operation_id: str) -> AdminOperationResponse | None: ...

    async def save(
        self, request: AdminOperationRequest, response: AdminOperationResponse
    ) -> None: ...


class OwnerAdminService:
    def __init__(
        self,
        owner: ServiceIdentity,
        handlers: dict[str, AdminHandler] | None = None,
        store: AdminOperationStore | None = None,
    ) -> None:
        self._owner = owner
        self._handlers = dict(handlers or {})
        self._results: dict[str, AdminOperationResponse] = {}
        self._store = store

    async def execute(self, request: AdminOperationRequest) -> AdminOperationResponse:
        previous = self._results.get(request.operation_id)
        if previous is None and self._store is not None:
            previous = await self._store.get(request.operation_id)
        if previous is not None:
            return previous
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
            result = await self._handlers[request.operation](dict(request.parameters))
            response = AdminOperationResponse(
                operation_id=request.operation_id,
                status="completed",
                result=result,
            )
        self._results[request.operation_id] = response
        if self._store is not None:
            await self._store.save(request, response)
        return response
