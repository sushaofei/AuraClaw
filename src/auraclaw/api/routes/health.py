from typing import Any

from fastapi import APIRouter, Request

from auraclaw.config import get_settings

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request) -> dict[str, Any]:
    settings = get_settings()
    runtime_events_ready = bool(
        getattr(request.app.state, "runtime_event_bus_ready", False)
    )
    return {
        "status": "ready" if runtime_events_ready else "degraded",
        "storage": "postgres" if settings.postgres_enabled else "memory",
        "runtime_events": "kafka" if settings.kafka_enabled else "memory",
        "runtime_event_bus_ready": runtime_events_ready,
        "development_runtime": (
            "running"
            if getattr(request.app.state, "development_runtime_ready", False)
            else "disabled"
        ),
    }
