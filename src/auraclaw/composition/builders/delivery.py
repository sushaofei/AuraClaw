from __future__ import annotations

from datetime import timedelta
from typing import Any

from fastapi import FastAPI

from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.composition.services import (
    ServiceSpec,
    _base_service_app,
    _configured_identities,
    _worker_post_tick_wait,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.delivery.worker import ResultDeliveryWorker
from auraclaw.infrastructure.clients.credential import RemoteCredentialProxy
from auraclaw.infrastructure.clients.policy import RemotePolicyClient
from auraclaw.infrastructure.clients.session import (
    NoOpOutboxRelay,
    RemoteSessionDeliveryOutboxSource,
    RemoteSessionEventStore,
)
from auraclaw.infrastructure.delivery import InMemoryDeliveryJobStore, PostgresDeliveryJobStore
from auraclaw.infrastructure.delivery.remote_sinks import CredentialProxyWebhookSink
from auraclaw.infrastructure.delivery.sinks import ParentSessionResultSink
from auraclaw.infrastructure.observability.stores import PostgresObservabilityStore
from auraclaw.infrastructure.persistence.postgres_admin_store import PostgresAdminOperationStore
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import admin_routes


def build_delivery_worker_app(
    spec: ServiceSpec,
    settings: Settings,
    *,
    worker_interval: float = 1.0,
) -> FastAPI:
    token = settings.workload_token_value(ServiceIdentity.DELIVERY_WORKER.value)
    bearer_token = token or ""
    session = RemoteSessionEventStore(
        settings.session_base_url,
        service_identity=ServiceIdentity.DELIVERY_WORKER,
        bearer_token=bearer_token,
        timeout=max(10.0, settings.worker_idle_interval + 5.0),
    )
    outbox = RemoteSessionDeliveryOutboxSource(
        session,
        worker_id="delivery-worker",
        wait_seconds=(settings.worker_idle_interval if settings.worker_wake_enabled else 0.0),
    )
    store = (
        PostgresDeliveryJobStore(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else InMemoryDeliveryJobStore()
    )
    admin_store = (
        PostgresAdminOperationStore(settings.resolved_database_url, schema="delivery")
        if settings.sql_storage_enabled
        else None
    )
    delivery_metric_store = (
        PostgresObservabilityStore(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    policy = RemotePolicyClient(
        settings.policy_base_url,
        bearer_token=bearer_token,
        service_identity=ServiceIdentity.DELIVERY_WORKER,
    )
    credentials = RemoteCredentialProxy(
        settings.credential_proxy_base_url,
        bearer_token=bearer_token,
        service_identity=ServiceIdentity.DELIVERY_WORKER,
    )
    worker = ResultDeliveryWorker(
        outbox=outbox,
        event_store=session,
        relay=NoOpOutboxRelay(),
        store=store,
        adapters=(
            ParentSessionResultSink(session, NoOpOutboxRelay()),
            CredentialProxyWebhookSink(policy, credentials),
        ),
        circuit_failure_threshold=settings.delivery_circuit_failure_threshold,
        circuit_reset_after=timedelta(seconds=settings.delivery_circuit_reset_seconds),
        circuit_probe_ttl=timedelta(seconds=settings.delivery_circuit_probe_ttl_seconds),
        claim_ttl=timedelta(seconds=settings.delivery_claim_ttl_seconds),
        max_concurrent=settings.delivery_max_concurrent,
        max_concurrent_per_tenant=settings.delivery_max_concurrent_per_tenant,
        metric_writer=delivery_metric_store,
    )
    closeables: tuple[Any, ...] = (session, policy, credentials)
    if settings.sql_storage_enabled:
        closeables += (store, admin_store, delivery_metric_store)
    app = _base_service_app(
        spec,
        settings,
        tick=worker.run_once,
        worker_interval=_worker_post_tick_wait(settings, worker_interval),
        closeables=closeables,
    )
    app.state.session_access = "http"
    app.state.delivery_store_owner = True

    async def delivery_status(parameters: dict[str, Any]) -> dict[str, Any]:
        tenant_id = str(parameters.get("tenant_id", ""))
        session_id = str(parameters.get("session_id", ""))
        sink_id = str(parameters.get("sink_id", ""))
        jobs = await store.list_jobs(tenant_id, session_id) if tenant_id and session_id else []
        circuit = (
            await store.get_sink_circuit(tenant_id, sink_id)
            if tenant_id and sink_id
            else None
        )
        return {
            "jobs": len(jobs),
            "circuit": None
            if circuit is None
            else {
                "state": circuit.state,
                "failure_count": circuit.failure_count,
                "generation": circuit.generation,
                "open_until": circuit.open_until.isoformat()
                if circuit.open_until is not None
                else None,
                "probe_owner": circuit.probe_owner,
            },
        }

    async def delivery_redrive(parameters: dict[str, Any]) -> dict[str, Any]:
        changed = await worker.redeliver(
            str(parameters["tenant_id"]), str(parameters["delivery_id"])
        )
        return {"changed": changed}

    admin_app = create_contract_app(
        "delivery-worker",
        admin_routes(
            OwnerAdminService(
                ServiceIdentity.DELIVERY_WORKER,
                {"status": delivery_status, "redrive": delivery_redrive},
                store=admin_store,
            )
        ),
        workload_identities=_configured_identities(settings, (ServiceIdentity.TASK_API,)),
    )
    app.mount("/", admin_app)
    return app
