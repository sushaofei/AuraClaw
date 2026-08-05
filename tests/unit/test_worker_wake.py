from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from auraclaw.composition.services import create_service_app
from auraclaw.composition.worker_wake import WorkerWakeGate
from auraclaw.config import Settings
from auraclaw.infrastructure.clients.worker_wake import (
    HttpWorkerWakeClient,
    OutboxWakeNotifier,
)
from auraclaw.session.internal_service import outbox_wake_destinations


def test_outbox_wake_destinations_for_create_task() -> None:
    assert outbox_wake_destinations(("session.created", "run.requested")) == {
        "projection",
        "control",
    }


def test_outbox_wake_destinations_for_scheduled_run() -> None:
    assert outbox_wake_destinations(("run.scheduled",)) == {"projection", "runtime"}


def test_outbox_wake_destinations_for_completion() -> None:
    assert outbox_wake_destinations(("run.completed",)) == {"projection", "delivery"}


@pytest.mark.asyncio
async def test_worker_wake_gate_interrupts_idle_wait() -> None:
    gate = WorkerWakeGate()

    async def signal_soon() -> None:
        await asyncio.sleep(0.05)
        gate.signal()

    asyncio.create_task(signal_soon())
    started = asyncio.get_running_loop().time()
    woke = await gate.wait(2.0)
    elapsed = asyncio.get_running_loop().time() - started
    assert woke is True
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_outbox_wake_notifier_posts_destinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            return httpx.Response(204)

    transport = _Transport()
    notifier = OutboxWakeNotifier(
        {
            "projection": HttpWorkerWakeClient(
                "http://projection", transport=transport
            ),
            "control": HttpWorkerWakeClient("http://orchestrator", transport=transport),
        }
    )
    notifier.schedule(("projection", "control"))
    await asyncio.sleep(0.05)
    await notifier.aclose()
    assert any("/internal/v1/worker/wake" in url and "projection" in url for url in calls)
    assert any("/internal/v1/worker/wake" in url and "orchestrator" in url for url in calls)
    assert sum(1 for url in calls if "orchestrator" in url) >= 3


def test_orchestrator_wake_endpoint_signals_gate() -> None:
    settings = Settings(
        deployment_profile="development",
        storage_backend="memory",
        worker_wake_enabled=True,
    )
    app = create_service_app("orchestrator", settings)
    assert isinstance(app.state.worker_wake, WorkerWakeGate)

    async def _noop() -> int:
        return 0

    app.state.tick = _noop
    with TestClient(app) as client:
        signaled = asyncio.Event()

        original = app.state.worker_wake.signal

        def _signal() -> None:
            original()
            signaled.set()

        app.state.worker_wake.signal = _signal  # type: ignore[method-assign]
        response = client.post("/internal/v1/worker/wake")
        assert response.status_code == 204
        assert signaled.is_set()
