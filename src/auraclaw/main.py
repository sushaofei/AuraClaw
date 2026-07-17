from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from auraclaw import __version__
from auraclaw.api.routes import router
from auraclaw.contracts.errors import AuraClawError


def create_app() -> FastAPI:
    app = FastAPI(
        title="AuraClaw Managed Agent API",
        version=__version__,
        description="Canonical-event-driven Managed Agent backend",
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
