from __future__ import annotations

from contextlib import suppress

import httpx
from fastapi import FastAPI

from auraclaw.composition import providers
from auraclaw.composition.services import (
    RemoteRuntimeWorker,
    ServiceSpec,
    _agent_runtime_token,
    _base_service_app,
    _runtime_instance_identity,
)
from auraclaw.config import Settings
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.infrastructure.clients.runtime import (
    RemoteCollaborationClient,
    RemoteRuntimeControlClient,
    RemoteRuntimeSessionClient,
)
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.collaboration_controller import RuntimeCollaborationController
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.harness import AgentHarness


def build_agent_runtime_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    token = _agent_runtime_token(settings) or ""
    bearer_token = token
    runtime_id, node_id = _runtime_instance_identity(settings)
    control = RemoteRuntimeControlClient(
        settings.control_base_url,
        bearer_token=bearer_token,
        runtime_id=runtime_id,
        role=settings.runtime_role,
        node_id=node_id,
        capacity=settings.runtime_capacity,
    )
    session = RemoteRuntimeSessionClient(
        settings.session_base_url,
        bearer_token=bearer_token,
    )
    collaboration = RemoteCollaborationClient(
        settings.session_base_url,
        bearer_token=bearer_token,
    )
    model = RemoteModelClient(
        settings.model_gateway_base_url,
        bearer_token=bearer_token,
        timeout=settings.model_timeout_seconds,
    )
    hands_http = httpx.AsyncClient(
        base_url=settings.hands_url,
        timeout=settings.model_timeout_seconds,
    )
    hands = HandsRuntimeAdapter(
        HttpHandsClient(
            hands_http,
            bearer_tokens={runtime_id: bearer_token},
        )
    )
    harness = AgentHarness(
        control_store=control,
        session=session,
        model=model,
        tools=hands,
        runtime_events=providers.get_runtime_event_publisher(),
        capability_controller=RuntimeCapabilityController(
            hands,
            skill_content_cache_max_bytes=(
                settings.runtime_skill_content_cache_max_bytes
            ),
            skill_content_cache_max_entries=(
                settings.runtime_skill_content_cache_max_entries
            ),
            skill_content_cache_ttl_seconds=(
                settings.runtime_skill_content_cache_ttl_seconds
            ),
            skill_prompt_max_bytes=settings.runtime_skill_prompt_max_bytes,
            skill_prompt_max_estimated_tokens=(
                settings.runtime_skill_prompt_max_estimated_tokens
            ),
        ),
        collaboration_controller=RuntimeCollaborationController(collaboration),
    )
    worker = RemoteRuntimeWorker(control, harness)
    app = _base_service_app(
        spec,
        settings,
        tick=worker.tick,
        worker_interval=settings.runtime_poll_interval,
        closeables=(control, session, collaboration, model, hands_http),
    )
    app.state.data_access = "remote-only"

    async def prewarm_runtime_links() -> None:
        with suppress(Exception):
            await hands_http.get("/health/live")
        with suppress(Exception):
            await model.prewarm()

    app.state.initialize = prewarm_runtime_links
    return app
