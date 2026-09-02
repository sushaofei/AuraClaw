from __future__ import annotations

from fastapi import FastAPI

from auraclaw.composition import providers
from auraclaw.composition.services import (
    ServiceSpec,
    _base_service_app,
    _configured_identities,
    _fencing_token_ledger,
    _lease_signing_key,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.infrastructure.clients.session import NoOpOutboxRelay
from auraclaw.infrastructure.clients.worker_wake import HttpWorkerWakeClient, OutboxWakeNotifier
from auraclaw.infrastructure.persistence.postgres_fencing_ledger import PostgresFencingTokenLedger
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import (
    collaboration_routes,
    session_routes,
)
from auraclaw.internal.security import LeaseAssertionVerifier
from auraclaw.session.collaboration_internal_service import CollaborationInternalService
from auraclaw.session.collaboration_service import CollaborationService
from auraclaw.session.internal_service import SessionInternalService


def build_session_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    wake: OutboxWakeNotifier | None = None
    closeables: tuple[object, ...] = ()
    if settings.worker_wake_enabled:
        wake = OutboxWakeNotifier(
            {
                "projection": HttpWorkerWakeClient(settings.projection_base_url),
                "control": HttpWorkerWakeClient(settings.control_base_url),
                "delivery": HttpWorkerWakeClient(settings.delivery_base_url),
                "runtime": HttpWorkerWakeClient(settings.runtime_base_url),
            }
        )
        closeables = (wake,)
    fencing_ledger = _fencing_token_ledger(settings, "session")
    if isinstance(fencing_ledger, PostgresFencingTokenLedger):
        closeables += (fencing_ledger,)
    app = _base_service_app(spec, settings, closeables=closeables)
    key = _lease_signing_key(settings)
    verifier = LeaseAssertionVerifier(
        {"development": key},
        ledger=fencing_ledger,
        audience=("session", "runtime"),
    )
    event_store = providers.get_event_store()
    service = SessionInternalService(
        event_store,
        lease_verifier=verifier,
        outbox_wake=None if wake is None else wake.schedule,
    )
    collaboration_service = CollaborationInternalService(
        CollaborationService(
            event_store=event_store,
            relay=NoOpOutboxRelay(),
        ),
        event_store,
        lease_verifier=verifier,
        outbox_wake=None if wake is None else wake.schedule,
    )
    identities = _configured_identities(
        settings,
        (
            ServiceIdentity.TASK_API,
            ServiceIdentity.PROJECTION_WORKER,
            ServiceIdentity.ORCHESTRATOR,
            ServiceIdentity.AGENT_RUNTIME,
            ServiceIdentity.POLICY,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.STREAMING_GATEWAY,
            ServiceIdentity.ACTION_HANDS,
        ),
    )
    contract_app = create_contract_app(
        "session",
        {**session_routes(service), **collaboration_routes(collaboration_service)},
        workload_identities=identities,
    )
    app.mount("/", contract_app)
    return app
