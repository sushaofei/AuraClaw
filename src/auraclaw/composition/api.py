import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
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
from auraclaw.api.routes import router
from auraclaw.composition import providers
from auraclaw.config import get_settings
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.observability import TraceContext
from auraclaw.infrastructure.observability.stores import StructuredLogger


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    ingestor = providers.get_streaming_ingestor()
    development_worker = None
    development_worker_task: asyncio.Task[None] | None = None
    app.state.development_runtime_ready = False
    app.state.runtime_event_bus_ready = ingestor is None
    if ingestor is not None:
        try:
            await asyncio.wait_for(ingestor.start(), timeout=10)
            app.state.runtime_event_bus_ready = True
        except Exception:
            # Streaming is best-effort; Canonical Session APIs must remain available.
            app.state.runtime_event_bus_ready = False
    if settings.development_runtime_active:
        development_worker = providers.build_development_runtime_worker()
        development_worker_task = asyncio.create_task(development_worker.run())
        app.state.development_runtime_ready = True
        logging.getLogger(__name__).info(
            "development runtime worker started (storage=%s, runtime_events=%s)",
            "postgres" if settings.postgres_enabled else "memory",
            "kafka" if settings.kafka_enabled else "memory",
        )
    yield
    if development_worker is not None and development_worker_task is not None:
        await development_worker.stop()
        with suppress(Exception):
            await asyncio.wait_for(development_worker_task, timeout=10)
    if ingestor is not None:
        with suppress(Exception):
            await asyncio.wait_for(ingestor.close(), timeout=10)
    producer = providers.get_runtime_event_producer()
    close = getattr(producer, "close", None)
    if close is not None:
        with suppress(Exception):
            await asyncio.wait_for(close(), timeout=10)
    await providers.get_runtime_replay_bus().close()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="AuraClaw Managed Agent API",
        version=__version__,
        description="Canonical-event-driven Managed Agent backend",
        lifespan=lifespan,
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
    if settings.allowed_cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.allowed_cors_origins,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=[
                "Content-Type",
                "Idempotency-Key",
                "If-None-Match",
                "Last-Event-ID",
                "X-Actor-ID",
                "X-Correlation-ID",
                "X-Expected-Version",
                "X-Tenant-ID",
            ],
            expose_headers=["ETag", "Retry-After", "traceparent"],
        )
    app.include_router(router)
    structured_logger = StructuredLogger()

    @app.middleware("http")
    async def observe_request(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        trace_id = request.headers.get("traceparent", "").split("-")[1:2]
        trace = trace_id[0] if trace_id and len(trace_id[0]) == 32 else uuid4().hex
        span_id = uuid4().hex[:16]
        tenant_id = request.headers.get("X-Tenant-ID", "local")
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
            duration_ms = (time.perf_counter() - started) * 1_000
            context = TraceContext(trace_id=trace, span_id=span_id, tenant_id=tenant_id)
            try:
                await providers.get_observability_service().record_span(
                    context=context,
                    component="task_gateway",
                    operation=f"{request.method} {request.url.path}",
                    started_at=started_at,
                    status=status,
                    attributes={"http_status": status_code, "duration_ms": duration_ms},
                )
                await providers.get_observability_service().metric(
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


app = create_app()
