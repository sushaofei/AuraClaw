import asyncio

import pytest

from auraclaw.contracts.errors import SyncInvokeBusyError
from auraclaw.gateways.query.waiter import TaskResultWaiter, classify_result


class ScriptedQuery:
    def __init__(self, results: list[dict[str, object]]) -> None:
        self._results = results
        self.calls = 0

    async def get_result(self, tenant_id: str, session_id: str) -> dict[str, object]:
        del tenant_id, session_id
        index = min(self.calls, len(self._results) - 1)
        self.calls += 1
        return dict(self._results[index])


class GateQuery:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def get_result(self, tenant_id: str, session_id: str) -> dict[str, object]:
        del tenant_id, session_id
        self.entered.set()
        await self.release.wait()
        return {"status": "completed", "session_status": "ready"}


def test_classify_result_stops_on_terminal_and_human_gates() -> None:
    assert classify_result({"status": "completed"}) == "completed"
    assert classify_result({"status": "failed"}) == "failed"
    assert classify_result({"status": "cancelled"}) == "cancelled"
    assert classify_result({"status": "waiting_for_human"}) == "needs_human"
    assert classify_result({"status": "running", "session_status": "paused"}) == (
        "needs_resume"
    )
    assert classify_result({"status": "retry_wait"}) is None
    assert classify_result({"status": "running"}) is None


def test_waiter_returns_terminal_result() -> None:
    async def scenario() -> None:
        query = ScriptedQuery(
            [
                {"status": "pending"},
                {"status": "running"},
                {"status": "completed", "result_summary": "done"},
            ]
        )
        waiter = TaskResultWaiter(query, poll_interval=0.01)
        waited = await waiter.wait("tenant-1", "ses_1", timeout_seconds=1)
        assert waited.outcome == "completed"
        assert waited.result["result_summary"] == "done"
        assert query.calls == 3

    asyncio.run(scenario())


def test_waiter_times_out_without_mutating_result() -> None:
    async def scenario() -> None:
        query = ScriptedQuery([{"status": "running", "run_id": "run_1"}])
        waiter = TaskResultWaiter(query, poll_interval=0.01)
        waited = await waiter.wait("tenant-1", "ses_1", timeout_seconds=0.05)
        assert waited.outcome == "timeout"
        assert waited.result["status"] == "running"

    asyncio.run(scenario())


def test_waiter_returns_needs_human_without_waiting_out_timeout() -> None:
    async def scenario() -> None:
        query = ScriptedQuery([{"status": "waiting_for_human"}])
        waiter = TaskResultWaiter(query, poll_interval=0.01)
        waited = await waiter.wait("tenant-1", "ses_1", timeout_seconds=5)
        assert waited.outcome == "needs_human"

    asyncio.run(scenario())


def test_waiter_propagates_cancellation_without_finishing() -> None:
    async def scenario() -> None:
        query = GateQuery()
        waiter = TaskResultWaiter(query, poll_interval=0.01)
        task = asyncio.create_task(waiter.wait("tenant-1", "ses_1", timeout_seconds=5))
        await query.entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert query.release.is_set() is False

    asyncio.run(scenario())


def test_waiter_rejects_excess_concurrent_waits() -> None:
    async def scenario() -> None:
        query = GateQuery()
        waiter = TaskResultWaiter(query, poll_interval=0.01, max_concurrent=1)
        first = asyncio.create_task(waiter.wait("tenant-1", "ses_1", timeout_seconds=5))
        await query.entered.wait()
        with pytest.raises(SyncInvokeBusyError) as exc:
            await waiter.wait("tenant-1", "ses_2", timeout_seconds=1)
        assert exc.value.status_code == 429
        assert exc.value.retry_after == 2
        query.release.set()
        waited = await first
        assert waited.outcome == "completed"

    asyncio.run(scenario())
