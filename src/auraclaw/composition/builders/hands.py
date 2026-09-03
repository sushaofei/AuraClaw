from __future__ import annotations

import asyncio
import logging
import os
import secrets
import socket
from collections.abc import Awaitable, Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from auraclaw.action.capability_catalog import (
    CAPABILITY_LOAD_TOOL_NAME,
    CAPABILITY_SEARCH_TOOL_NAME,
    SKILL_BINDING_STATUS_TOOL_NAME,
    SKILL_RESOLVE_TOOL_NAME,
    CapabilityCatalog,
    CapabilityLoadExecutor,
    CapabilitySearchExecutor,
    RoutedHandsExecutor,
    SkillBindingStatusExecutor,
    SkillResolveExecutor,
    capability_load_tool,
    capability_search_tool,
    skill_binding_status_tool,
    skill_resolve_tool,
)
from auraclaw.action.catalog_reconciler import CapabilityCatalogReconciler
from auraclaw.action.hands import HandsGateway
from auraclaw.action.hands_http import (
    HandsWorkloadAuthenticator,
    SignedLeaseHandsAuthenticator,
    create_hands_http_app,
)
from auraclaw.action.mcp_connection_manager import McpConnectionManager
from auraclaw.action.mcp_internal_service import McpRegistryInternalService
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.resource_gateway import ManagedResourceGateway
from auraclaw.action.skill_admission_maintenance import SkillAdmissionMaintenanceWorker
from auraclaw.action.skill_content_cache import SkillPackageContentCache
from auraclaw.action.skill_internal_service import SkillPublicationInternalService
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore, SkillLifecycleStore
from auraclaw.action.skill_lifecycle_events import (
    BroadcastingSkillStateProjector,
    SkillLifecycleSignalApplier,
    SkillLifecycleSignalRelay,
    SkillTenantRebuilder,
)
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import SkillResolver
from auraclaw.action.skill_publishers import (
    InMemorySkillPublisherStore,
    SkillPublisherService,
    SkillPublisherTrustService,
)
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.action.skill_reliability import SkillPublicationReliabilityWorker
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.composition.services import (
    EmptyApprovalReader,
    ServiceSpec,
    _agent_runtime_token,
    _base_service_app,
    _capability_catalog_store,
    _configured_identities,
    _fencing_token_ledger,
    _lease_signing_key,
    _mcp_registry_service,
    _require_production_security_configuration,
    _service_bearer_token,
    _skill_publication_service,
    _skill_registry_service,
)
from auraclaw.config import Settings
from auraclaw.contracts.capabilities import CapabilityStatus
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.infrastructure.clients.artifact import RemoteArtifactWriter
from auraclaw.infrastructure.clients.artifact_reader import (
    RemoteArtifactReader,
    RemoteSkillArtifactLifecycle,
)
from auraclaw.infrastructure.clients.credential import RemoteCredentialProxy
from auraclaw.infrastructure.clients.mcp_egress import RemoteMcpEgressClient
from auraclaw.infrastructure.clients.policy import RemotePolicyClient
from auraclaw.infrastructure.clients.session import RemoteSkillBindingReferenceReader
from auraclaw.infrastructure.connectors.http.connector import (
    ManagedJavaApiConnector,
    catalog_server_definition,
)
from auraclaw.infrastructure.connectors.mcp.connector import ManagedMcpConnector
from auraclaw.infrastructure.hands.local import LocalHandsService
from auraclaw.infrastructure.kafka.skill_lifecycle_events import (
    KafkaSkillLifecycleSignalConsumer,
    KafkaSkillLifecycleSignalPublisher,
)
from auraclaw.infrastructure.observability.stores import PostgresObservabilityStore
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_fencing_ledger import (
    PostgresFencingTokenLedger,
)
from auraclaw.infrastructure.persistence.postgres_invocation_store import (
    PostgresInvocationStore,
)
from auraclaw.infrastructure.persistence.postgres_mcp_registry import (
    PostgresMcpServerRegistryStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle_events import (
    PostgresSkillLifecycleSignalStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_publishers import (
    PostgresSkillPublisherStore,
)
from auraclaw.infrastructure.persistence.postgres_tool_registry import (
    PostgresToolRegistryStore,
)
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import mcp_registry_routes, skill_publication_routes
from auraclaw.internal.security import LeaseAssertionVerifier

logger = logging.getLogger(__name__)


def build_action_hands_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    _require_production_security_configuration(
        settings,
        spec.name,
        (
            ServiceIdentity.TASK_API,
            ServiceIdentity.CREDENTIAL_PROXY,
            ServiceIdentity.ACTION_HANDS,
        ),
        requires_policy=True,
    )
    fencing_ledger = _fencing_token_ledger(settings, "hands")
    capability_catalog_store = _capability_catalog_store(settings)
    mcp_registry, mcp_registry_store = _mcp_registry_service(settings)
    hands_token = _service_bearer_token(settings, ServiceIdentity.ACTION_HANDS)
    policy = RemotePolicyClient(settings.policy_base_url, bearer_token=hands_token)
    credential_proxy = RemoteCredentialProxy(
        settings.credential_proxy_base_url, bearer_token=hands_token
    )
    mcp_egress_client = RemoteMcpEgressClient(
        settings.credential_proxy_base_url, bearer_token=hands_token
    )
    artifacts = RemoteArtifactWriter(
        settings.artifact_base_url,
        bearer_token=_service_bearer_token(settings, ServiceIdentity.ACTION_HANDS),
    )
    artifact_reader = RemoteArtifactReader(
        settings.artifact_base_url,
        bearer_token=_service_bearer_token(settings, ServiceIdentity.ACTION_HANDS),
        policy=policy,
    )
    skill_artifacts = RemoteSkillArtifactLifecycle(
        settings.artifact_base_url,
        bearer_token=_service_bearer_token(settings, ServiceIdentity.ACTION_HANDS),
        policy=policy,
    )
    skill_binding_references = RemoteSkillBindingReferenceReader(
        settings.session_base_url,
        bearer_token=hands_token,
    )
    invocation_store: PostgresInvocationStore | None = None
    tool_registry_store: PostgresToolRegistryStore | None = None
    hands_metric_store: PostgresObservabilityStore | None = None
    skill_publisher_store: PostgresSkillPublisherStore | InMemorySkillPublisherStore
    if settings.sql_storage_enabled:
        invocation_store = PostgresInvocationStore(settings.resolved_database_url)
        tool_registry_store = PostgresToolRegistryStore(settings.resolved_database_url)
        hands_metric_store = PostgresObservabilityStore(settings.resolved_database_url)
        skill_lifecycle: SkillLifecycleStore = PostgresSkillLifecycleStore(
            settings.resolved_database_url,
            transaction_retry_attempts=settings.skill_transaction_retry_attempts,
            transaction_retry_base_delay=settings.skill_transaction_retry_base_delay_seconds,
            metric_writer=hands_metric_store,
        )
        skill_publisher_store = PostgresSkillPublisherStore(
            settings.resolved_database_url,
            transaction_retry_attempts=settings.skill_transaction_retry_attempts,
            transaction_retry_base_delay=settings.skill_transaction_retry_base_delay_seconds,
            metric_writer=hands_metric_store,
        )
    else:
        skill_lifecycle = InMemorySkillLifecycleStore()
        skill_publisher_store = InMemorySkillPublisherStore()
    skill_publishers = SkillPublisherService(skill_publisher_store)
    publisher_trust = SkillPublisherTrustService(skill_publisher_store)
    closeables: tuple[Any, ...] = (
        policy,
        credential_proxy,
        mcp_egress_client,
        artifacts,
        artifact_reader,
        skill_artifacts,
        skill_binding_references,
        *((invocation_store,) if invocation_store is not None else ()),
        *((tool_registry_store,) if tool_registry_store is not None else ()),
        *((hands_metric_store,) if hands_metric_store is not None else ()),
        *((skill_lifecycle,) if isinstance(skill_lifecycle, PostgresSkillLifecycleStore) else ()),
        *(
            (skill_publisher_store,)
            if isinstance(skill_publisher_store, PostgresSkillPublisherStore)
            else ()
        ),
        *(
            (capability_catalog_store,)
            if isinstance(capability_catalog_store, PostgresCapabilityCatalogStore)
            else ()
        ),
        *(
            (mcp_registry_store,)
            if isinstance(mcp_registry_store, PostgresMcpServerRegistryStore)
            else ()
        ),
        *((fencing_ledger,) if isinstance(fencing_ledger, PostgresFencingTokenLedger) else ()),
    )
    app = _base_service_app(spec, settings, closeables=closeables)
    capability_catalog = CapabilityCatalog(capability_catalog_store)
    skill_registry = _skill_registry_service(settings, artifacts=artifacts)
    skill_publication, _ = _skill_publication_service(
        settings,
        skill_registry,
        skill_lifecycle,
        artifact_reader=artifact_reader,
        artifact_lifecycle=skill_artifacts,
        publisher_trust=publisher_trust,
    )
    skill_content_cache = SkillPackageContentCache(
        artifact_reader,
        max_bytes=settings.skill_content_cache_max_bytes,
        max_entries=settings.skill_content_cache_max_entries,
        ttl_seconds=settings.skill_content_cache_ttl_seconds,
        metric_writer=hands_metric_store,
    )
    skill_rebuilder = SkillStateRebuilder(
        lifecycle=skill_lifecycle,
        artifacts=artifact_reader,
        registry=skill_registry,
        catalog=capability_catalog,
        publisher_trust=publisher_trust,
        content_cache=skill_content_cache,
        metric_writer=hands_metric_store,
    )
    skill_state_projector: SkillTenantRebuilder = skill_rebuilder
    skill_signal_consumer: KafkaSkillLifecycleSignalConsumer | None = None
    skill_signal_relay: SkillLifecycleSignalRelay | None = None
    if settings.sql_storage_enabled and settings.kafka_enabled:
        replica_id = settings.skill_lifecycle_replica_id or (
            f"{os.getenv('HOSTNAME') or socket.gethostname()}-{os.getpid()}"
        )
        skill_signal_store = PostgresSkillLifecycleSignalStore(settings.resolved_database_url)
        skill_signal_publisher = KafkaSkillLifecycleSignalPublisher(
            settings.kafka_bootstrap_servers,
            topic=settings.kafka_skill_lifecycle_topic,
        )
        skill_signal_consumer = KafkaSkillLifecycleSignalConsumer(
            settings.kafka_bootstrap_servers,
            topic=settings.kafka_skill_lifecycle_topic,
            replica_id=replica_id,
            target=SkillLifecycleSignalApplier(
                rebuilder=skill_rebuilder,
                metric_writer=hands_metric_store,
            ),
        )
        skill_state_projector = BroadcastingSkillStateProjector(
            rebuilder=skill_rebuilder,
            signals=skill_signal_store,
            replica_id=replica_id,
        )
        skill_signal_relay = SkillLifecycleSignalRelay(
            signals=skill_signal_store,
            publisher=skill_signal_publisher,
            owner=f"{replica_id}-skill-lifecycle-relay-{secrets.token_hex(4)}",
            claim_ttl=timedelta(seconds=settings.skill_reliability_claim_ttl_seconds),
        )
        app.state.closeables = (
            skill_signal_consumer,
            skill_signal_publisher,
            skill_signal_store,
            *app.state.closeables,
        )
    skill_reliability = SkillPublicationReliabilityWorker(
        lifecycle=skill_lifecycle,
        artifacts=skill_artifacts,
        rebuilder=skill_state_projector,
        owner=f"action-hands-{secrets.token_hex(8)}",
        max_concurrent=settings.skill_reliability_max_concurrent,
        claim_ttl=timedelta(seconds=settings.skill_reliability_claim_ttl_seconds),
        metric_writer=hands_metric_store,
    )
    skill_admission_maintenance = SkillAdmissionMaintenanceWorker(
        skill_lifecycle,
        retention=timedelta(days=settings.skill_admission_retention_days),
        batch_size=settings.skill_admission_cleanup_batch_size,
    )
    skill_management = SkillManagementService(
        lifecycle=skill_lifecycle,
        projector=skill_state_projector,
        artifacts=artifact_reader,
        binding_references=skill_binding_references,
        retired_activator=skill_publication,
    )
    resources = skill_registry.resources or HandsResourceRegistry()
    capability_connectors: dict[str, Any] = {}
    resource_gateway = ManagedResourceGateway(
        resources,
        artifacts=artifacts,
        policy=policy,
        max_concurrent=settings.resource_gateway_max_concurrent,
        max_queued=settings.resource_gateway_max_queued,
        queue_timeout_seconds=settings.resource_gateway_queue_timeout_seconds,
        metric_writer=hands_metric_store,
        catalog_store=capability_catalog_store,
        connectors=capability_connectors,
        miss_loader=lambda tenant_id, _uri: skill_rebuilder.rebuild_tenant(tenant_id),
    )
    capability_catalog.set_availability(resource_gateway)
    skill_resolver = SkillResolver(
        skill_registry,
        capability_catalog_store,
        policy,
        reload_tenant=skill_rebuilder.rebuild_tenant,
    )
    registry = ToolRegistry(
        (
            capability_search_tool(),
            capability_load_tool(),
            skill_resolve_tool(),
            skill_binding_status_tool(),
        )
    )

    async def initialize_registry() -> None:
        if skill_signal_consumer is not None:
            try:
                await skill_signal_consumer.start()
            except Exception as exc:
                logger.warning(
                    "Skill lifecycle Kafka consumer is unavailable; continuing with "
                    "PostgreSQL reconciliation (error=%s)",
                    type(exc).__name__,
                )
        if tool_registry_store is not None:
            await tool_registry_store.load_into(registry)

        def mcp_connector(server: object) -> ManagedMcpConnector:
            from auraclaw.contracts.capabilities import McpServerDefinition

            assert isinstance(server, McpServerDefinition)
            return ManagedMcpConnector(server, credentials=credential_proxy, policy=policy)

        manager = McpConnectionManager(
            registry=mcp_registry,
            connectors=app.state.capability_connectors,
            factory=mcp_connector,  # type: ignore[arg-type]
            catalog=capability_catalog,
            reconciler=app.state.catalog_reconciler,
            egress=mcp_egress_client,
            instance_id=f"action-hands-{secrets.token_hex(8)}",
            max_concurrent=settings.mcp_reconcile_max_concurrent,
            max_concurrent_per_tenant=settings.mcp_reconcile_max_concurrent_per_tenant,
            max_concurrent_per_host=settings.mcp_reconcile_max_concurrent_per_host,
            server_timeout_seconds=settings.mcp_reconcile_server_timeout_seconds,
        )
        mcp_registry.bind_runtime(manager)
        app.state.mcp_connection_manager = manager
        for attempt in range(20):
            try:
                await manager.restore()
                break
            except Exception as exc:
                if attempt >= 19:
                    raise
                logger.warning(
                    "Action Hands startup dependency is unavailable; retrying MCP "
                    "restore (%s/20, error=%s)",
                    attempt + 1,
                    type(exc).__name__,
                )
                await asyncio.sleep(0.5)
        for java_server in settings.java_api_servers:
            await capability_catalog.register_server(catalog_server_definition(java_server))
            app.state.capability_connectors[java_server.server_id] = ManagedJavaApiConnector(
                java_server,
                credentials=credential_proxy,
                policy=policy,
            )
        app.state.skill_rebuild_result = await skill_rebuilder.rebuild_all()
        app.state.skill_installation_drained = await skill_management.reconcile_draining()
        app.state.skill_reliability_result = await skill_reliability.run_once()
        app.state.skill_admission_maintenance_result = await skill_admission_maintenance.run_once()

    app.state.capability_connectors = capability_connectors
    app.state.catalog_reconciler = None
    app.state.initialize = initialize_registry
    routed_hands = RoutedHandsExecutor(
        LocalHandsService(workspace_root=Path.cwd()),
        {
            CAPABILITY_SEARCH_TOOL_NAME: CapabilitySearchExecutor(capability_catalog),
            CAPABILITY_LOAD_TOOL_NAME: CapabilityLoadExecutor(capability_catalog),
            SKILL_RESOLVE_TOOL_NAME: SkillResolveExecutor(skill_resolver),
            SKILL_BINDING_STATUS_TOOL_NAME: SkillBindingStatusExecutor(
                skill_lifecycle,
                publisher_security=skill_publisher_store,
            ),
        },
    )
    gateway = ToolGateway(
        registry=registry,
        policy=policy,
        approvals=EmptyApprovalReader(),
        hands=routed_hands,
        artifacts=artifacts,
        credential_proxy=credential_proxy,
        invocation_store=invocation_store,
        approval_controller=policy,
        instance_id=f"action-hands-{secrets.token_hex(8)}",
        max_concurrent=settings.hands_max_concurrent,
        max_concurrent_per_tenant=settings.hands_max_concurrent_per_tenant,
        max_queued=settings.hands_max_queued,
        max_queued_per_tenant=settings.hands_max_queued_per_tenant,
        queue_timeout=settings.hands_queue_timeout_seconds,
        metric_writer=hands_metric_store,
    )
    token = _agent_runtime_token(settings) or ""
    key = _lease_signing_key(settings)
    authenticator: HandsWorkloadAuthenticator = SignedLeaseHandsAuthenticator(
        {token: "*"} if token else {},
        verifier=LeaseAssertionVerifier(
            {"development": key},
            ledger=fencing_ledger,
            audience="runtime",
        ),
    )
    hands_gateway = HandsGateway(
        registry=registry,
        gateway=gateway,
        resources=resources,
        resource_reader=resource_gateway,
    )
    hands_http_app = create_hands_http_app(
        hands_gateway,
        authenticator=authenticator,
    )
    app.state.capability_catalog = capability_catalog
    app.state.resource_gateway = resource_gateway
    app.state.skill_registry = skill_registry
    app.state.skill_rebuilder = skill_rebuilder
    periodic_jobs: list[tuple[str, float, Callable[[], Awaitable[int | None]]]] = []

    async def rebuild_skill_state() -> int:
        result = await skill_rebuilder.rebuild_all()
        app.state.skill_rebuild_result = result
        return result.publication_count

    periodic_jobs.append(
        ("skill-state", settings.mcp_reconcile_interval_seconds, rebuild_skill_state)
    )

    async def drain_skill_installations() -> int:
        completed = await skill_management.reconcile_draining()
        app.state.skill_installation_drained = completed
        return completed

    periodic_jobs.append(
        (
            "skill-installation-drain",
            settings.mcp_reconcile_interval_seconds,
            drain_skill_installations,
        )
    )

    async def cleanup_skill_admissions() -> int:
        result = await skill_admission_maintenance.run_once()
        app.state.skill_admission_maintenance_result = result
        return result.deleted

    periodic_jobs.append(
        (
            "skill-admission-cleanup",
            settings.skill_admission_cleanup_interval_seconds,
            cleanup_skill_admissions,
        )
    )

    async def reconcile_skill_reliability() -> int:
        result = await skill_reliability.run_once()
        app.state.skill_reliability_result = result
        return result.outbox_completed + result.orphans_deleted

    periodic_jobs.append(
        (
            "skill-reliability",
            settings.mcp_reconcile_interval_seconds,
            reconcile_skill_reliability,
        )
    )
    if skill_signal_relay is not None:
        signal_relay = skill_signal_relay

        async def relay_skill_lifecycle_signals() -> int:
            return await signal_relay.run_once()

        periodic_jobs.append(
            (
                "skill-lifecycle-broadcast",
                min(1.0, settings.mcp_reconcile_interval_seconds),
                relay_skill_lifecycle_signals,
            )
        )
    if skill_signal_consumer is not None:
        signal_consumer = skill_signal_consumer

        async def ensure_skill_lifecycle_consumer() -> int:
            if not signal_consumer.ready:
                await signal_consumer.start()
            return 0

        periodic_jobs.append(
            (
                "skill-lifecycle-consumer",
                min(5.0, settings.mcp_reconcile_interval_seconds),
                ensure_skill_lifecycle_consumer,
            )
        )
    reconciler = CapabilityCatalogReconciler(
        catalog=capability_catalog,
        store=capability_catalog_store,
        connectors=app.state.capability_connectors,
        resource_cache=resource_gateway,
        tool_registry=registry,
        hands_router=routed_hands,
        max_concurrent=settings.mcp_reconcile_max_concurrent,
        max_concurrent_per_tenant=settings.mcp_reconcile_max_concurrent_per_tenant,
        max_concurrent_per_host=settings.mcp_reconcile_max_concurrent_per_host,
        server_timeout_seconds=settings.mcp_reconcile_server_timeout_seconds,
    )
    app.state.catalog_reconciler = reconciler

    async def initialize_remote_catalog() -> None:
        await initialize_registry()
        for connector in app.state.capability_connectors.values():
            setter = getattr(connector, "set_notification_handler", None)
            if setter is not None:
                setter(reconciler.handle_notification)
        await reconciler.reconcile_all()

    async def reconcile_catalog_and_skills() -> int:
        results = await reconciler.reconcile_all_results()
        manager = getattr(app.state, "mcp_connection_manager", None)
        if manager is not None:
            await manager.record_reconcile_results(results)
        return sum(result.status is CapabilityStatus.ACTIVE for result in results)

    async def reconcile_mcp_revisions() -> int:
        manager = getattr(app.state, "mcp_connection_manager", None)
        if manager is None:
            return 0
        return int(await manager.reconcile_loaded())

    app.state.initialize = initialize_remote_catalog
    periodic_jobs.append(
        (
            "capability-catalog",
            settings.mcp_reconcile_interval_seconds,
            reconcile_catalog_and_skills,
        )
    )
    periodic_jobs.append(
        (
            "mcp-revision",
            settings.mcp_revision_reconcile_interval_seconds,
            reconcile_mcp_revisions,
        )
    )
    initialize_periodic_jobs = app.state.initialize
    next_due: dict[str, float] = {}

    async def initialize_and_schedule() -> None:
        await initialize_periodic_jobs()
        now = asyncio.get_running_loop().time()
        next_due.update({name: now + interval for name, interval, _run in periodic_jobs})

    async def reconcile_due_jobs() -> int:
        now = asyncio.get_running_loop().time()
        due = [
            (name, interval, run)
            for name, interval, run in periodic_jobs
            if now >= next_due[name]
        ]
        if not due:
            return 0
        outcomes = await asyncio.gather(
            *(run() for _name, _interval, run in due),
            return_exceptions=True,
        )
        finished_at = asyncio.get_running_loop().time()
        completed = 0
        failures: list[BaseException] = []
        for (name, interval, _run), outcome in zip(due, outcomes, strict=True):
            next_due[name] = finished_at + interval
            if isinstance(outcome, BaseException):
                failures.append(outcome)
            elif outcome is not None:
                completed += outcome
        if failures:
            raise failures[0]
        return completed

    app.state.initialize = initialize_and_schedule
    app.state.tick = reconcile_due_jobs
    app.state.worker_interval = min(interval for _name, interval, _run in periodic_jobs)
    mcp_identities = _configured_identities(
        settings,
        (
            ServiceIdentity.TASK_API,
            ServiceIdentity.CREDENTIAL_PROXY,
            ServiceIdentity.ACTION_HANDS,
        ),
    )
    app.mount(
        "/internal/v1/mcp-registry",
        create_contract_app(
            "action-hands-mcp-registry",
            mcp_registry_routes(McpRegistryInternalService(mcp_registry)),
            workload_identities=mcp_identities,
        ),
    )
    app.mount(
        "/internal/v1/skill-publications",
        create_contract_app(
            "action-hands-skill-publications",
            skill_publication_routes(
                SkillPublicationInternalService(
                    skill_publication,
                    management=skill_management,
                    rebuilder=skill_state_projector,
                    publishers=skill_publishers,
                    admissions=skill_lifecycle,
                    artifacts=artifact_reader,
                    package_cache=skill_content_cache,
                )
            ),
            workload_identities=_configured_identities(
                settings, (ServiceIdentity.TASK_API,)
            ),
        ),
    )
    app.mount("/", hands_http_app)
    return app
