from __future__ import annotations

import secrets

from fastapi import FastAPI

from auraclaw.composition import providers
from auraclaw.composition.services import (
    ServiceSpec,
    UnavailableModelClient,
    _base_service_app,
    _configured_identities,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.infrastructure.clients.policy import RemotePolicyClient
from auraclaw.infrastructure.observability.stores import PostgresObservabilityStore
from auraclaw.infrastructure.persistence.postgres_model_store import PostgresModelStateStore
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import model_routes, model_stream_routes
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService


def build_model_gateway_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    policy: RemotePolicyClient | None = None
    state = (
        PostgresModelStateStore(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    metric_store = (
        PostgresObservabilityStore(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    token = settings.workload_token_value(ServiceIdentity.MODEL_GATEWAY.value)
    policy = RemotePolicyClient(
        settings.policy_base_url,
        bearer_token=token or secrets.token_urlsafe(32),
        service_identity=ServiceIdentity.MODEL_GATEWAY,
    )
    model = (
        providers.get_model_gateway()
        if settings.model_gateway_configured
        else UnavailableModelClient()
    )
    model_service = ModelGatewayInternalService(
        model,
        policy=policy,
        state=state,
        tenant_token_limit=settings.model_tenant_token_limit_per_hour,
        metric_writer=metric_store,
        pricing=settings.model_pricing,
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=(
            *((policy,) if policy is not None else ()),
            *((state,) if state is not None else ()),
            *((metric_store,) if metric_store is not None else ()),
            model,
        ),
    )
    prewarm = getattr(model, "prewarm", None)
    if callable(prewarm):
        app.state.initialize = prewarm
    contract_app = create_contract_app(
        "model-gateway",
        model_routes(model_service),
        stream_routes=model_stream_routes(model_service),
        workload_identities=_configured_identities(
            settings, (ServiceIdentity.AGENT_RUNTIME, ServiceIdentity.POLICY)
        ),
    )
    app.mount("/", contract_app)
    return app
