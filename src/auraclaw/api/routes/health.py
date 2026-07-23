from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from auraclaw.config import get_settings

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request) -> JSONResponse:
    settings = get_settings()
    service_name = str(getattr(request.app.state, "service_name", "development-combined"))
    if service_name in {"task-api", "streaming-gateway"}:
        ready = bool(getattr(request.app.state, "service_ready", False))
        return JSONResponse(
            status_code=200 if ready else 503,
            content={
                "status": "ready" if ready else "degraded",
                "service": service_name,
                "storage": getattr(
                    request.app.state,
                    "storage_label",
                    "postgres" if settings.postgres_enabled else "memory",
                ),
            },
        )
    runtime_events_ready = bool(
        getattr(request.app.state, "runtime_event_bus_ready", False)
    )
    model_gateway_ready = bool(
        getattr(request.app.state, "model_gateway_ready", False)
    )
    runtime_worker_ready = bool(
        getattr(request.app.state, "runtime_worker_ready", False)
    )
    executor_ready = not settings.runtime_enabled or runtime_worker_ready
    ready = runtime_events_ready and executor_ready
    return JSONResponse(
        status_code=200 if ready else 503,
        content={
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
            "runtime_worker": (
                "running"
                if runtime_worker_ready
                else "disabled" if not settings.runtime_enabled else "unconfigured"
            ),
        },
    )
