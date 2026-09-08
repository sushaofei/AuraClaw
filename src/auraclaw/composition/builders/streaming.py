from __future__ import annotations

from fastapi import FastAPI

from auraclaw.api.dependencies import get_streaming_gateway
from auraclaw.composition import providers
from auraclaw.composition.api import create_app
from auraclaw.composition.services import ServiceSpec
from auraclaw.config import Settings
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection


def build_streaming_gateway_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    del spec
    if not settings.sql_storage_enabled:
        raise ValueError(
            "streaming-gateway requires SQL storage; use `auraclaw serve` with .env.dev "
            "configured for PostgreSQL or Kingbase"
        )
    app = create_app(profile="streaming-gateway")
    projection = PostgresTaskProjection(settings.resolved_database_url)
    gateway = StreamingGateway(
        reader=projection,
        bus=providers.get_runtime_replay_bus(),
        delta_min_interval=settings.stream_delta_min_interval_seconds,
    )
    app.dependency_overrides[get_streaming_gateway] = lambda: gateway
    app.state.closeables = (projection,)
    app.state.storage_label = "projection-read-only"
    app.state.session_access = "database"
    return app
