from __future__ import annotations

import secrets
from typing import Any

from fastapi import FastAPI

from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.composition import providers
from auraclaw.composition.services import (
    ServiceSpec,
    _base_service_app,
    _configured_identities,
    _worker_post_tick_wait,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.infrastructure.clients.session import (
    RemoteSessionEventStore,
    RemoteSessionOutboxSource,
)
from auraclaw.infrastructure.persistence.postgres_admin_store import PostgresAdminOperationStore
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import admin_routes
from auraclaw.observability.service import ObservabilityProjector, ObservabilityService
from auraclaw.projection.approval.projector import CompositeProjection
from auraclaw.projection.relay import OutboxRelay


def build_projection_app(
    spec: ServiceSpec,
    settings: Settings,
    *,
    worker_interval: float,
) -> FastAPI:
    if not settings.sql_storage_enabled:
        return _base_service_app(spec, settings, worker_interval=worker_interval)

    task_projection = providers.get_task_projection()
    approval_projection = providers.get_approval_projection()
    collaboration_projection = providers.get_collaboration_projection()
    admin_store = PostgresAdminOperationStore(
        settings.resolved_database_url, schema="projection"
    )
    token = settings.workload_token_value(ServiceIdentity.PROJECTION_WORKER.value)
    remote_session = RemoteSessionEventStore(
        settings.session_base_url,
        service_identity=ServiceIdentity.PROJECTION_WORKER,
        bearer_token=token or secrets.token_urlsafe(32),
        timeout=max(10.0, settings.worker_idle_interval + 5.0),
    )
    claim_wait = settings.worker_idle_interval if settings.worker_wake_enabled else 0.0
    source = RemoteSessionOutboxSource(
        remote_session,
        worker_id="projection-worker",
        wait_seconds=claim_wait,
    )
    observability_store = providers.get_observability_store()
    projector = CompositeProjection(
        *providers.session_outbox_projectors(),
        ObservabilityProjector(ObservabilityService(observability_store, remote_session)),
    )
    relay = OutboxRelay(source, projector)
    closeables: tuple[Any, ...] = (
        remote_session,
        task_projection,
        approval_projection,
        collaboration_projection,
        admin_store,
        observability_store,
    )
    app = _base_service_app(
        spec,
        settings,
        tick=relay.relay_once,
        worker_interval=_worker_post_tick_wait(settings, worker_interval),
        closeables=closeables,
    )

    async def status(parameters: dict[str, Any]) -> dict[str, Any]:
        tenant_id = parameters.get("tenant_id")
        count = (
            await task_projection.poison_count(str(tenant_id) if tenant_id else None)
            if isinstance(task_projection, PostgresTaskProjection)
            else 0
        )
        return {"poison_count": count}

    async def redrive(parameters: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(task_projection, PostgresTaskProjection):
            return {"changed": False}
        changed = await task_projection.redrive_poison(
            str(parameters["tenant_id"]), str(parameters["event_id"])
        )
        return {"changed": changed}

    async def rebuild(parameters: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(task_projection, PostgresTaskProjection) or remote_session is None:
            return {"processed": 0}
        tenant = parameters.get("tenant_id")
        tenant_id = str(tenant) if tenant else None
        events = []
        for event_tenant, session_id in await task_projection.session_keys(tenant_id):
            events.extend(await remote_session.load(event_tenant, session_id))
        processed = await task_projection.rebuild(events, tenant_id)
        return {"processed": processed}

    admin_app = create_contract_app(
        "projection-worker",
        admin_routes(
            OwnerAdminService(
                ServiceIdentity.PROJECTION_WORKER,
                {"status": status, "redrive": redrive, "rebuild": rebuild},
                store=admin_store,
            )
        ),
        workload_identities=_configured_identities(settings, (ServiceIdentity.TASK_API,)),
    )
    app.mount("/", admin_app)
    return app
