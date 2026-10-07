from __future__ import annotations

import secrets
from typing import Any

from fastapi import FastAPI

from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.composition import providers
from auraclaw.composition.observability import exporting_observability_store
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
    activity_projection = providers.get_activity_projection()
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
    observability_store = exporting_observability_store(
        settings,
        service_name="projection-worker",
        store=providers.get_observability_store(),
    )
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
        activity_projection,
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
        items = (
            await task_projection.poison_items(
                str(tenant_id) if tenant_id else None,
                limit=min(100, max(1, int(parameters.get("limit", 20)))),
            )
            if isinstance(task_projection, PostgresTaskProjection)
            else []
        )
        return {"poison_count": count, "poison_events": items}

    async def redrive(parameters: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(task_projection, PostgresTaskProjection):
            return {"accepted": False}
        tenant_id = str(parameters["tenant_id"])
        event_id = str(parameters["event_id"])
        if not await task_projection.has_poison(tenant_id, event_id):
            return {"accepted": False, "reason": "poison_event_not_found"}
        accepted = await remote_session.redrive_outbox("projection", event_id)
        return {"accepted": accepted}

    async def rebuild(parameters: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(task_projection, PostgresTaskProjection) or remote_session is None:
            return {"processed": 0}
        tenant = parameters.get("tenant_id")
        if not tenant:
            return {"processed": 0, "accepted": False, "reason": "tenant_id_required"}
        tenant_id = str(tenant)
        events = await remote_session.load_all(tenant_id)
        counts = {
            "task": await task_projection.rebuild(events, tenant_id),
            "approval": await approval_projection.rebuild(events, tenant_id),
            "collaboration": await collaboration_projection.rebuild(events, tenant_id),
            "activity": await activity_projection.rebuild(events, tenant_id),
        }
        return {
            "accepted": True,
            "processed": len(events),
            "projection_counts": counts,
        }

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
