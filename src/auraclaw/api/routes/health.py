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
    model_gateway_ready = bool(
        getattr(request.app.state, "model_gateway_ready", False)
    )
    development_runtime_ready = bool(
        getattr(request.app.state, "development_runtime_ready", False)
    )
    production_runtime_ready = bool(
        getattr(request.app.state, "production_runtime_ready", False)
    )
    executor_ready = (
        development_runtime_ready
        if settings.development_runtime_active
        else production_runtime_ready
    )
    ready = runtime_events_ready and executor_ready
    return {
        "status": "ready" if ready else "degraded",
        "storage": "postgres" if settings.postgres_enabled else "memory",
        "runtime_events": "kafka" if settings.kafka_enabled else "memory",
        "runtime_event_bus_ready": runtime_events_ready,
        "runtime_event_producer_ready": bool(
            getattr(request.app.state, "runtime_event_producer_ready", False)
        ),
        "runtime_event_ingestor_ready": bool(
            getattr(request.app.state, "runtime_event_ingestor_ready", False)
        ),
        "model_gateway": settings.model_provider,
        "model_gateway_ready": model_gateway_ready,
        "development_runtime": (
            "running"
            if development_runtime_ready
            else "disabled"
        ),
        "production_runtime": "running" if production_runtime_ready else "disabled",
    }
