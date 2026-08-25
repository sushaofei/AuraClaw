from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from auraclaw.contracts.errors import SyncInvokeBusyError
from auraclaw.contracts.state import TERMINAL_RUN_STATUSES

WaitOutcome = Literal[
    "completed",
    "failed",
    "cancelled",
    "timeout",
    "needs_human",
    "needs_resume",
]

_TERMINAL_OUTCOMES = {status.value for status in TERMINAL_RUN_STATUSES}
_INTERRUPT_OUTCOMES: dict[str, WaitOutcome] = {
    "waiting_for_human": "needs_human",
    "paused": "needs_resume",
}
_MAX_POLL_INTERVAL_SECONDS = 1.0


class ResultReader(Protocol):
    def get_result(
        self, tenant_id: str, session_id: str
    ) -> Awaitable[dict[str, Any]]: ...


@dataclass(frozen=True)
class WaitedResult:
    outcome: WaitOutcome
    result: dict[str, Any]


def classify_result(result: dict[str, Any]) -> WaitOutcome | None:
    """Return a wait outcome if polling should stop, otherwise None."""
    status = result.get("status")
    if isinstance(status, str) and status in _TERMINAL_OUTCOMES:
        return status  # type: ignore[return-value]
    session_status = result.get("session_status")
    for candidate in (status, session_status):
        if isinstance(candidate, str) and candidate in _INTERRUPT_OUTCOMES:
            return _INTERRUPT_OUTCOMES[candidate]
    return None


def decorate_result(
    result: dict[str, Any],
    *,
    session_id: str,
    outcome: WaitOutcome,
) -> dict[str, Any]:
    body = dict(result)
    body["wait_outcome"] = outcome
    body["status_url"] = f"/v1/tasks/{session_id}"
    body["result_url"] = f"/v1/tasks/{session_id}/result"
    body["stream_url"] = f"/v1/streams/{session_id}"
    if outcome == "needs_human":
        body["code"] = "needs_human"
        body["message"] = "Run is waiting for human approval"
    elif outcome == "needs_resume":
        body["code"] = "needs_resume"
        body["message"] = "Run is paused and must be resumed"
    return body


class TaskResultWaiter:
    """Poll the Result projection until a Run terminal, interrupt, or timeout.

    Disconnects raise CancelledError to the HTTP layer and must not cancel the
    Session. This waiter never reads Runtime Event / SSE streams.
    """

    def __init__(
        self,
        query: ResultReader,
        *,
        poll_interval: float = 0.25,
        max_concurrent: int = 32,
        default_timeout_seconds: int = 60,
        max_timeout_seconds: int = 120,
    ) -> None:
        self._query = query
        self._poll_interval = max(poll_interval, 0.05)
        self._limit = max(max_concurrent, 1)
        self._default_timeout = default_timeout_seconds
        self._max_timeout = max_timeout_seconds
        self._inflight = 0
        self._lock = asyncio.Lock()

    def clamp_timeout(self, requested: int | None) -> float:
        value = self._default_timeout if requested is None else requested
        return float(min(max(value, 1), self._max_timeout))

    async def wait(
        self,
        tenant_id: str,
        session_id: str,
        *,
        timeout_seconds: float,
    ) -> WaitedResult:
        async with self._lock:
            if self._inflight >= self._limit:
                raise SyncInvokeBusyError()
            self._inflight += 1
        try:
            return await self._poll(
                tenant_id,
                session_id,
                timeout_seconds=timeout_seconds,
            )
        finally:
            async with self._lock:
                self._inflight -= 1

    async def _poll(
        self,
        tenant_id: str,
        session_id: str,
        *,
        timeout_seconds: float,
    ) -> WaitedResult:
        deadline = time.monotonic() + max(timeout_seconds, 0.0)
        interval = self._poll_interval
        last: dict[str, Any] | None = None
        while True:
            last = await self._query.get_result(tenant_id, session_id)
            outcome = classify_result(last)
            if outcome is not None:
                return WaitedResult(outcome=outcome, result=last)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return WaitedResult(outcome="timeout", result=last)
            await asyncio.sleep(min(interval, remaining))
            interval = min(interval * 1.5, _MAX_POLL_INTERVAL_SECONDS)
