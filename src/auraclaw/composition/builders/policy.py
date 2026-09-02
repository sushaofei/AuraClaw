from __future__ import annotations

from fastapi import FastAPI

from auraclaw.composition.services import (
    ServiceSpec,
    _base_service_app,
    _configured_identities,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.infrastructure.persistence.postgres_policy_store import PostgresPolicyStateStore
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import policy_routes
from auraclaw.policy.internal_service import PolicyInternalService


def build_policy_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    store = (
        PostgresPolicyStateStore(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=(store,) if store is not None else (),
    )
    contract_app = create_contract_app(
        "policy",
        policy_routes(PolicyInternalService(version="s3-v1", store=store)),
        workload_identities=_configured_identities(
            settings,
            (
                ServiceIdentity.TASK_API,
                ServiceIdentity.ORCHESTRATOR,
                ServiceIdentity.MODEL_GATEWAY,
                ServiceIdentity.ACTION_HANDS,
                ServiceIdentity.DELIVERY_WORKER,
                ServiceIdentity.CREDENTIAL_PROXY,
                ServiceIdentity.ARTIFACT_SERVICE,
            ),
        ),
    )
    app.mount("/", contract_app)
    return app
