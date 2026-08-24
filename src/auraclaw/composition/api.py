import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response

from auraclaw import __version__
from auraclaw.api.dependencies import (
    get_collaboration_projection as collaboration_projection_dependency,
)
from auraclaw.api.dependencies import (
    get_observability_service as observability_service_dependency,
)
from auraclaw.api.dependencies import (
    get_streaming_gateway as streaming_gateway_dependency,
)
from auraclaw.api.dependencies import (
    get_task_command_gateway as task_command_gateway_dependency,
)
from auraclaw.api.dependencies import (
    get_task_projection as task_projection_dependency,
)
from auraclaw.api.dependencies import (
    get_task_query_service as task_query_service_dependency,
)
from auraclaw.api.routes.health import router as health_router
from auraclaw.api.routes.operations import router as operations_router
from auraclaw.api.routes.streams import router as stream_router
from auraclaw.api.routes.tasks import router as task_router
from auraclaw.composition import providers
from auraclaw.composition.identity import build_identity_verifier
from auraclaw.config import get_settings
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.observability import TraceContext
from auraclaw.infrastructure.observability.stores import StructuredLogger

ApiProfile = Literal["task-api", "streaming-gateway"]


@asynccontextmanager
async def task_api_lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.service_name = "task-api"
    app.state.service_ready = bool(getattr(app.state, "config_ready", True))
    try:
        yield
    finally:
        app.state.service_ready = False
        for closeable in getattr(app.state, "closeables", ()):
            close = getattr(closeable, "aclose", None)
            if close is None:
                close = closeable.close
            await close()


@asynccontextmanager
async def streaming_lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.service_name = "streaming-gateway"
    app.state.service_ready = False
    ingestor = providers.get_streaming_ingestor()
    if ingestor is not None:
        await asyncio.wait_for(ingestor.start(), timeout=10)
    app.state.runtime_event_bus_ready = True
    app.state.service_ready = True
    try:
        yield
    finally:
        app.state.service_ready = False
        if ingestor is not None:
            with suppress(Exception):
                await asyncio.wait_for(ingestor.close(), timeout=10)
        await providers.get_runtime_replay_bus().close()
        close_identity = getattr(app.state.identity_verifier, "close", None)
        if close_identity is not None:
            await close_identity()


def create_app(*, profile: ApiProfile) -> FastAPI:
    settings = get_settings()
    selected_lifespan = {
        "task-api": task_api_lifespan,
        "streaming-gateway": streaming_lifespan,
    }[profile]
    app = FastAPI(
        title=f"AuraClaw {profile}",
        version=__version__,
        description="Canonical-event-driven Managed Agent backend",
        lifespan=selected_lifespan,
    )
    app.dependency_overrides.update(
        {
            task_command_gateway_dependency: providers.get_task_command_gateway,
            task_projection_dependency: providers.get_task_projection,
            task_query_service_dependency: providers.get_task_query_service,
            collaboration_projection_dependency: providers.get_collaboration_projection,
            streaming_gateway_dependency: providers.get_streaming_gateway,
            observability_service_dependency: providers.get_observability_service,
        }
    )
    app.state.identity_verifier = build_identity_verifier(settings)
    if settings.allowed_cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.allowed_cors_origins,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "Idempotency-Key",
                "If-None-Match",
                "Last-Event-ID",
                "X-Actor-ID",
                "X-Correlation-ID",
                "X-CT-Agent-Context",
                "X-Expected-Version",
                "X-Tenant-ID",
            ],
            expose_headers=["ETag", "Retry-After", "traceparent"],
        )
    app.include_router(health_router)
    if profile == "task-api":
        app.include_router(task_router)
        app.include_router(operations_router)
    else:
        app.include_router(stream_router)
    structured_logger = StructuredLogger()

    @app.middleware("http")
    async def observe_request(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        trace_id = request.headers.get("traceparent", "").split("-")[1:2]
        trace = trace_id[0] if trace_id and len(trace_id[0]) == 32 else uuid4().hex
        span_id = uuid4().hex[:16]
        tenant_id = "unauthenticated"
        started_at = datetime.now(UTC)
        started = time.perf_counter()
        status = "error"
        status_code = 500
        try:
            response = await call_next(request)
            status_code = int(response.status_code)
            status = "ok" if status_code < 500 else "error"
            response.headers["traceparent"] = f"00-{trace}-{span_id}-01"
            return response
        finally:
            tenant_id = getattr(request.state, "tenant_id", None) or tenant_id
            duration_ms = (time.perf_counter() - started) * 1_000
            context = TraceContext(trace_id=trace, span_id=span_id, tenant_id=tenant_id)
            try:
                observability = getattr(
                    request.app.state,
                    "observability_service",
                    None,
                ) or providers.get_observability_service()
                await observability.record_span(
                    context=context,
                    component="task_gateway",
                    operation=f"{request.method} {request.url.path}",
                    started_at=started_at,
                    status=status,
                    attributes={
                        "http_status": status_code,
                        "duration_ms": duration_ms,
                        "identity_kid": getattr(request.state, "identity_kid", None),
                        "identity_jti": getattr(request.state, "identity_jti", None),
                    },
                )
                await observability.metric(
                    "http.request.duration_ms",
                    duration_ms,
                    context=context,
                    labels={"method": request.method, "status": str(status_code)},
                )
            except Exception:
                structured_logger.emit(
                    logging.ERROR,
                    "observability_write_failed",
                    trace_id=trace,
                    span_id=span_id,
                    tenant_id=tenant_id,
                )

    @app.exception_handler(AuraClawError)
    async def handle_auraclaw_error(_: Request, exc: AuraClawError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": exc.code, "message": exc.message, "detail": exc.detail},
        )

    return app
