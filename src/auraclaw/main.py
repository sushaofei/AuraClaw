import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from auraclaw import __version__
from auraclaw.api.dependencies import (
    get_runtime_event_producer,
    get_runtime_replay_bus,
    get_streaming_ingestor,
)
from auraclaw.api.routes import router
from auraclaw.contracts.errors import AuraClawError


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    ingestor = get_streaming_ingestor()
    app.state.runtime_event_bus_ready = ingestor is None
    if ingestor is not None:
        try:
            await asyncio.wait_for(ingestor.start(), timeout=10)
            app.state.runtime_event_bus_ready = True
        except Exception:
            # Streaming is best-effort; Canonical Session APIs must remain available.
            app.state.runtime_event_bus_ready = False
    yield
    if ingestor is not None:
        with suppress(Exception):
            await asyncio.wait_for(ingestor.close(), timeout=10)
    producer = get_runtime_event_producer()
    close = getattr(producer, "close", None)
    if close is not None:
        with suppress(Exception):
            await asyncio.wait_for(close(), timeout=10)
    await get_runtime_replay_bus().close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="AuraClaw Managed Agent API",
        version=__version__,
        description="Canonical-event-driven Managed Agent backend",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.exception_handler(AuraClawError)
    async def handle_auraclaw_error(_: Request, exc: AuraClawError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": exc.code, "message": exc.message, "detail": exc.detail},
        )

    return app


app = create_app()
