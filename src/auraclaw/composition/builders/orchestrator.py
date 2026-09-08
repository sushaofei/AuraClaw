from __future__ import annotations

import secrets
from datetime import timedelta
from typing import Any

from fastapi import FastAPI

from auraclaw.composition.services import (
    ServiceSpec,
    _base_service_app,
    _configured_identities,
    _fencing_token_ledger,
    _lease_signing_key,
    _worker_post_tick_wait,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.control.orchestrator import ManagedOrchestrator, RegisteredRuntimeProvisioner
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.infrastructure.clients.runtime import RemoteOrchestratorSessionClient
from auraclaw.infrastructure.clients.session import RemoteSessionEventStore
from auraclaw.infrastructure.clients.worker_wake import HttpWorkerWakeClient
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.postgres_control_store import PostgresControlStateStore
from auraclaw.infrastructure.persistence.postgres_fencing_ledger import PostgresFencingTokenLedger
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import control_routes
from auraclaw.internal.security import LeaseAssertionSigner, LeaseAssertionVerifier


def build_orchestrator_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    store = (
        PostgresControlStateStore(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else InMemoryControlStateStore()
    )
    key = _lease_signing_key(settings)
    closeables: tuple[Any, ...] = (store,) if settings.sql_storage_enabled else ()
    fencing_ledger = _fencing_token_ledger(settings, "control")
    if isinstance(fencing_ledger, PostgresFencingTokenLedger):
        closeables += (fencing_ledger,)
    token = settings.workload_token_value(ServiceIdentity.ORCHESTRATOR.value)
    bearer_token = token or secrets.token_urlsafe(32)
    feed_session = RemoteSessionEventStore(
        settings.session_base_url,
        service_identity=ServiceIdentity.ORCHESTRATOR,
        bearer_token=bearer_token,
        timeout=max(10.0, settings.worker_idle_interval + 5.0),
    )
    lifecycle_session = RemoteOrchestratorSessionClient(
        settings.session_base_url,
        bearer_token=bearer_token,
    )
    runtime_wake_client = (
        HttpWorkerWakeClient(settings.runtime_base_url)
        if settings.worker_wake_enabled
        else None
    )
    worker_id = f"orchestrator-{secrets.token_hex(8)}"
    claim_wait = settings.worker_idle_interval if settings.worker_wake_enabled else 0.0
    feed = RunnableFeedConsumer(
        feed_session,
        store,
        worker_id=worker_id,
        wait_seconds=claim_wait,
    )
    orchestrator = ManagedOrchestrator(
        orchestrator_id=worker_id,
        control_store=store,
        session=lifecycle_session,
        provisioner=RegisteredRuntimeProvisioner(store),
        lease_ttl=timedelta(seconds=settings.orchestrator_lease_ttl_seconds),
        runtime_wake=(
            (lambda: runtime_wake_client.wake()) if runtime_wake_client is not None else None
        ),
        register_selected_runtime=False,
    )

    async def schedule_tick() -> int:
        ingested = await feed.run_once()
        scheduled = await orchestrator.schedule_once()
        recovered = 0
        if ingested == 0 and scheduled is None:
            recovered = await orchestrator.recover()
            if recovered:
                scheduled = await orchestrator.schedule_once()
        return ingested + recovered + int(scheduled is not None)

    closeables += (feed_session, lifecycle_session)
    if runtime_wake_client is not None:
        closeables += (runtime_wake_client,)
    app = _base_service_app(
        spec,
        settings,
        tick=schedule_tick,
        worker_interval=_worker_post_tick_wait(settings, settings.orchestrator_worker_interval),
        closeables=closeables,
    )
    service = ControlInternalService(
        store,
        lease_verifier=LeaseAssertionVerifier(
            {"development": key},
            ledger=fencing_ledger,
            audience=("control", "runtime"),
        ),
        lease_signer=LeaseAssertionSigner(key_id="development", signing_key=key),
    )
    contract_app = create_contract_app(
        "orchestrator",
        control_routes(service),
        workload_identities=_configured_identities(
            settings,
            (ServiceIdentity.AGENT_RUNTIME, ServiceIdentity.TASK_API),
        ),
    )
    app.mount("/", contract_app)
    return app
