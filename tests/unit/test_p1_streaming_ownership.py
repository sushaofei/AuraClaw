import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auraclaw.api.routes.health import router as health_router
from auraclaw.config import Settings
from auraclaw.contracts.errors import ServiceDrainingError
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.infrastructure.kafka.runtime_events import ReplayRuntimeEventBus


class _Reader:
    async def get_task(self, tenant_id: str, session_id: str) -> dict[str, str]:
        return {"tenant_id": tenant_id, "session_id": session_id}


def test_in_memory_gateway_drain_rejects_new_and_closes_existing_subscriptions() -> None:
    async def scenario() -> None:
        bus = ReplayRuntimeEventBus()
        gateway = StreamingGateway(reader=_Reader(), bus=bus)  # type: ignore[arg-type]
        subscription = await gateway.subscribe(
            tenant_id="tenant-1",
            session_id="session-1",
            last_event_id=None,
        )
        events = subscription.events()

        assert await bus.begin_drain(retry_after_seconds=7) == 1
        with pytest.raises(StopAsyncIteration):
            await anext(events)
        with pytest.raises(ServiceDrainingError) as error:
            await gateway.authorize(tenant_id="tenant-1", session_id="session-1")
        assert error.value.status_code == 503
        assert error.value.retry_after == 7
        assert await bus.readiness() == (False, "draining")

    asyncio.run(scenario())


def test_gateway_readiness_uses_live_ownership_probe() -> None:
    app = FastAPI()
    app.include_router(health_router)
    app.state.service_name = "streaming-gateway"
    app.state.service_ready = True

    async def probe() -> tuple[bool, str]:
        return False, "streaming gateway ownership was lost"

    app.state.readiness_probe = probe
    with TestClient(app) as client:
        response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json() == {
        "status": "degraded",
        "service": "streaming-gateway",
        "storage": "memory",
        "detail": "streaming gateway ownership was lost",
    }


def test_gateway_readiness_fails_closed_when_probe_raises() -> None:
    app = FastAPI()
    app.include_router(health_router)
    app.state.service_name = "streaming-gateway"
    app.state.service_ready = True

    async def probe() -> tuple[bool, str]:
        raise RuntimeError("database URL must not leak")

    app.state.readiness_probe = probe
    with TestClient(app) as client:
        response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json() == {
        "status": "degraded",
        "service": "streaming-gateway",
        "storage": "memory",
        "detail": "readiness probe failed",
    }


def test_streaming_ownership_intervals_fail_closed() -> None:
    with pytest.raises(ValueError, match="TTL must exceed"):
        Settings(
            _env_file=None,
            streaming_connection_ttl_seconds=10,
            streaming_gateway_heartbeat_interval_seconds=5,
        )
    with pytest.raises(ValueError, match="retry delay"):
        Settings(
            _env_file=None,
            streaming_drain_timeout_seconds=5,
            streaming_drain_retry_after_seconds=6,
        )
