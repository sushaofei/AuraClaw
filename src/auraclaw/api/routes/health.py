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
    service_name = str(getattr(request.app.state, "service_name", "task-api"))
    ready = bool(getattr(request.app.state, "service_ready", False))
    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "ready" if ready else "degraded",
            "service": service_name,
            "storage": getattr(
                request.app.state,
                "storage_label",
                settings.storage_label,
            ),
        },
    )
