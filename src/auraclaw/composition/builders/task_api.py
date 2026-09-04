from __future__ import annotations

import secrets
from typing import Any

from fastapi import FastAPI

from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_management import InProcessSkillStateProjector, SkillManagementService
from auraclaw.action.skill_packages import SkillDependencyAvailability
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import (
    InMemorySkillPublisherStore,
    SkillPublisherService,
    SkillPublisherTrustService,
)
from auraclaw.api.dependencies import (
    get_collaboration_projection,
    get_observability_service,
    get_sync_invocation_gateway,
    get_task_command_gateway,
    get_task_projection,
    get_task_query_service,
    get_task_result_waiter,
)
from auraclaw.api.routes.admin_mcp import create_mcp_admin_router
from auraclaw.api.routes.admin_skills import create_skill_admin_router
from auraclaw.composition.api import create_app
from auraclaw.composition.services import (
    ServiceSpec,
    _capability_catalog_store,
    _mcp_registry_service,
    _service_bearer_token,
    _skill_publication_service,
    _skill_registry_service,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.gateways.query.waiter import TaskResultWaiter
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.gateways.task.invocations import SyncInvocationGateway
from auraclaw.infrastructure.clients.artifact import RemoteSkillPackageUploadClient
from auraclaw.infrastructure.clients.mcp_registry import RemoteMcpRegistryClient
from auraclaw.infrastructure.clients.policy import RemotePolicyClient, RemoteTaskAdmissionController
from auraclaw.infrastructure.clients.session import NoOpOutboxRelay, RemoteSessionEventStore
from auraclaw.infrastructure.clients.skill_publication import RemoteSkillPublicationClient
from auraclaw.infrastructure.observability.stores import PostgresObservabilityStore
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_mcp_registry import (
    PostgresMcpServerRegistryStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)
from auraclaw.infrastructure.projection.postgres_approval_store import (
    PostgresApprovalProjection,
)
from auraclaw.infrastructure.projection.postgres_collaboration_store import (
    PostgresCollaborationProjection,
)
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.observability.service import ObservabilityService
from auraclaw.session.task_service import TaskService


def build_task_api_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    del spec
    app = create_app(profile="task-api")
    token = settings.workload_token_value(ServiceIdentity.TASK_API.value)
    config_ready = bool(
        token
        and settings.sql_storage_enabled
        and (settings.signed_identity_configured or settings.insecure_identity_headers_enabled)
    )
    remote_session = RemoteSessionEventStore(
        settings.session_base_url,
        service_identity=ServiceIdentity.TASK_API,
        bearer_token=token or secrets.token_urlsafe(32),
    )
    policy = RemotePolicyClient(
        settings.policy_base_url,
        bearer_token=token or secrets.token_urlsafe(32),
        service_identity=ServiceIdentity.TASK_API,
    )
    if not settings.sql_storage_enabled:
        raise ValueError(
            "task-api requires SQL storage; use `auraclaw serve` with .env.dev "
            "configured for PostgreSQL or Kingbase"
        )
    task_projection = PostgresTaskProjection(settings.resolved_database_url)
    approval_projection = PostgresApprovalProjection(settings.resolved_database_url)
    collaboration_projection = PostgresCollaborationProjection(settings.resolved_database_url)
    task_service = TaskService(
        runtime_budget=settings.runtime_budget_snapshot(),
        event_store=remote_session,
        relay=NoOpOutboxRelay(),
        reader=task_projection,
        admission=RemoteTaskAdmissionController(policy),
        approvals=approval_projection,
        approval_notifier=policy,
    )
    gateway = TaskCommandGateway(task_service)
    query = TaskQueryService(task_projection, collaboration_projection, remote_session)
    waiter = TaskResultWaiter(
        query,
        poll_interval=settings.sync_invoke_poll_interval_seconds,
        max_concurrent=settings.sync_invoke_max_concurrent,
        default_timeout_seconds=settings.sync_invoke_default_timeout_seconds,
        max_timeout_seconds=settings.sync_invoke_max_timeout_seconds,
    )
    invocations = SyncInvocationGateway(gateway, waiter)
    observability_store = PostgresObservabilityStore(settings.resolved_database_url)
    observability = ObservabilityService(observability_store, remote_session)
    app.dependency_overrides[get_task_command_gateway] = lambda: gateway
    app.dependency_overrides[get_task_projection] = lambda: task_projection
    app.dependency_overrides[get_task_query_service] = lambda: query
    app.dependency_overrides[get_task_result_waiter] = lambda: waiter
    app.dependency_overrides[get_sync_invocation_gateway] = lambda: invocations
    app.dependency_overrides[get_collaboration_projection] = lambda: collaboration_projection
    app.dependency_overrides[get_observability_service] = lambda: observability
    app.state.observability_service = observability
    identity_closeables = (
        (app.state.identity_verifier,) if hasattr(app.state.identity_verifier, "close") else ()
    )
    app.state.closeables = (
        *identity_closeables,
        remote_session,
        policy,
        task_projection,
        approval_projection,
        collaboration_projection,
        observability_store,
    )
    mcp_registry, mcp_store = _mcp_registry_service(settings)
    mcp_lifecycle = RemoteMcpRegistryClient(
        settings.hands_url,
        bearer_token=_service_bearer_token(settings, ServiceIdentity.TASK_API),
    )
    capability_catalog_store = _capability_catalog_store(settings)
    app.include_router(
        create_mcp_admin_router(
            mcp_registry,
            lifecycle=mcp_lifecycle,
            catalog=CapabilityCatalog(capability_catalog_store),
        )
    )
    skill_registry = _skill_registry_service(settings)
    skill_lifecycle: SkillLifecycleStore | None = None
    skill_publication: SkillPublicationService | RemoteSkillPublicationClient
    skill_management: SkillManagementService | RemoteSkillPublicationClient
    skill_uploads: RemoteSkillPackageUploadClient | None = None
    publisher_management: SkillPublisherService | RemoteSkillPublicationClient
    if settings.sql_storage_enabled:
        skill_publication = RemoteSkillPublicationClient(
            settings.hands_url,
            bearer_token=_service_bearer_token(settings, ServiceIdentity.TASK_API),
        )
        skill_management = skill_publication
        publisher_management = skill_publication
        skill_uploads = RemoteSkillPackageUploadClient(
            settings.artifact_base_url,
            bearer_token=_service_bearer_token(settings, ServiceIdentity.TASK_API),
        )
    else:
        publisher_store = InMemorySkillPublisherStore()
        publisher_management = SkillPublisherService(publisher_store)
        skill_publication, skill_lifecycle = _skill_publication_service(
            settings,
            skill_registry,
            publisher_trust=SkillPublisherTrustService(publisher_store),
        )
        skill_management = SkillManagementService(
            lifecycle=skill_lifecycle,
            projector=InProcessSkillStateProjector(
                lifecycle=skill_lifecycle,
                registry=skill_registry,
            ),
            retired_activator=skill_publication,
        )
    app.include_router(
        create_skill_admin_router(
            skill_registry,
            publication_service=skill_publication,
            management_service=skill_management,
            content_reader=(
                skill_publication
                if isinstance(skill_publication, RemoteSkillPublicationClient)
                else None
            ),
            upload_service=skill_uploads,
            publisher_service=publisher_management,
            admission_reader=skill_publication if skill_lifecycle is None else skill_lifecycle,
            capability_availability=SkillDependencyAvailability(
                capability_catalog_store
            ),
            admission_metrics_window_hours=settings.skill_admission_metrics_window_hours,
            admission_quarantine_alert_ratio=(settings.skill_admission_quarantine_alert_ratio),
            admission_quarantine_alert_min_samples=(
                settings.skill_admission_quarantine_alert_min_samples
            ),
        )
    )
    extra_closeables: list[Any] = [mcp_lifecycle]
    if isinstance(mcp_store, PostgresMcpServerRegistryStore):
        extra_closeables.append(mcp_store)
    if isinstance(capability_catalog_store, PostgresCapabilityCatalogStore):
        extra_closeables.append(capability_catalog_store)
    if isinstance(skill_lifecycle, PostgresSkillLifecycleStore):
        extra_closeables.append(skill_lifecycle)
    if isinstance(skill_publication, RemoteSkillPublicationClient):
        extra_closeables.append(skill_publication)
    if skill_uploads is not None:
        extra_closeables.append(skill_uploads)
    app.state.closeables = (*app.state.closeables, *extra_closeables)
    app.state.config_ready = config_ready
    app.state.storage_label = "projection-read-only"
    app.state.session_access = "http"
    return app
