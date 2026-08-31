from __future__ import annotations

import asyncio
import logging
import os
import secrets
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from auraclaw import __version__
from auraclaw.action.capability_catalog import (
    CAPABILITY_LOAD_TOOL_NAME,
    CAPABILITY_SEARCH_TOOL_NAME,
    SKILL_BINDING_STATUS_TOOL_NAME,
    SKILL_RESOLVE_TOOL_NAME,
    CapabilityCatalog,
    CapabilityLoadExecutor,
    CapabilitySearchExecutor,
    InMemoryCapabilityCatalogStore,
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
from auraclaw.action.mcp_registry import (
    InMemoryMcpServerRegistryStore,
    McpServerRegistryService,
)
from auraclaw.action.ports import (
    ArtifactContentReader,
    ArtifactWriter,
    SkillArtifactLifecycle,
)
from auraclaw.action.resource_gateway import ManagedResourceGateway
from auraclaw.action.skill_admission_maintenance import SkillAdmissionMaintenanceWorker
from auraclaw.action.skill_internal_service import SkillPublicationInternalService
from auraclaw.action.skill_lifecycle import (
    InMemorySkillLifecycleStore,
    SkillLifecycleStore,
)
from auraclaw.action.skill_management import (
    InProcessSkillStateProjector,
    SkillManagementService,
)
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackageRegistry,
    SkillResolver,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import (
    InMemorySkillPublisherStore,
    SkillPublisherService,
    SkillPublisherTrustService,
)
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.action.skill_reconciler import SkillPackageReconciler
from auraclaw.action.skill_reliability import SkillPublicationReliabilityWorker
from auraclaw.action.skill_sources import SkillSourceService
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.api.dependencies import (
    get_collaboration_projection,
    get_observability_service,
    get_streaming_gateway,
    get_sync_invocation_gateway,
    get_task_command_gateway,
    get_task_projection,
    get_task_query_service,
    get_task_result_waiter,
)
from auraclaw.api.routes.admin_mcp import create_mcp_admin_router
from auraclaw.api.routes.admin_skills import create_skill_admin_router
from auraclaw.artifact.internal_service import ArtifactInternalService
from auraclaw.composition import providers
from auraclaw.composition.api import create_app
from auraclaw.composition.object_storage import (
    build_object_storage,
    object_storage_closeables,
)
from auraclaw.composition.worker_wake import WorkerWakeGate
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.capabilities import CapabilityStatus
from auraclaw.contracts.errors import FencingTokenError, LeaseConflictError
from auraclaw.contracts.internal import (
    InternalRequestContext,
    McpRegistrySnapshotRequest,
    McpRegistrySnapshotResponse,
    ServiceIdentity,
)
from auraclaw.contracts.mcp_registry import McpActiveSnapshotEntry
from auraclaw.contracts.skills import (
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import CredentialReference
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.control.orchestrator import (
    ManagedOrchestrator,
    RegisteredRuntimeProvisioner,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.credential_proxy.internal_service import CredentialProxyInternalService
from auraclaw.delivery.worker import ResultDeliveryWorker
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.gateways.query.waiter import TaskResultWaiter
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.gateways.task.invocations import SyncInvocationGateway
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.clients.artifact import (
    RemoteArtifactWriter,
    RemoteSkillPackageUploadClient,
)
from auraclaw.infrastructure.clients.artifact_reader import (
    RemoteArtifactReader,
    RemoteSkillArtifactLifecycle,
)
from auraclaw.infrastructure.clients.credential import RemoteCredentialProxy
from auraclaw.infrastructure.clients.mcp_egress import RemoteMcpEgressClient
from auraclaw.infrastructure.clients.mcp_registry import RemoteMcpRegistryClient
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.infrastructure.clients.policy import (
    RemotePolicyClient,
    RemoteTaskAdmissionController,
)
from auraclaw.infrastructure.clients.runtime import (
    RemoteCollaborationClient,
    RemoteOrchestratorSessionClient,
    RemoteRuntimeControlClient,
    RemoteRuntimeSessionClient,
)
from auraclaw.infrastructure.clients.session import (
    NoOpOutboxRelay,
    RemoteSessionDeliveryOutboxSource,
    RemoteSessionEventStore,
    RemoteSessionOutboxSource,
    RemoteSkillBindingReferenceReader,
)
from auraclaw.infrastructure.clients.skill_publication import (
    RemoteSkillPublicationClient,
)
from auraclaw.infrastructure.clients.worker_wake import (
    HttpWorkerWakeClient,
    OutboxWakeNotifier,
)
from auraclaw.infrastructure.connectors.http.connector import (
    ManagedJavaApiConnector,
    catalog_server_definition,
)
from auraclaw.infrastructure.connectors.http.egress import ManagedJavaApiEgressAdapter
from auraclaw.infrastructure.connectors.mcp.connector import ManagedMcpConnector
from auraclaw.infrastructure.credentials.mcp_egress_manager import McpEgressManager
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault
from auraclaw.infrastructure.credentials.vault import HashiCorpVault
from auraclaw.infrastructure.credentials.webhook import ManagedWebhookCredentialAdapter
from auraclaw.infrastructure.delivery import (
    InMemoryDeliveryJobStore,
    PostgresDeliveryJobStore,
)
from auraclaw.infrastructure.delivery.remote_sinks import CredentialProxyWebhookSink
from auraclaw.infrastructure.delivery.sinks import ParentSessionResultSink
from auraclaw.infrastructure.hands.local import LocalHandsService
from auraclaw.infrastructure.observability.stores import PostgresObservabilityStore
from auraclaw.infrastructure.persistence.memory_control_store import (
    InMemoryControlStateStore,
)
from auraclaw.infrastructure.persistence.postgres_admin_store import (
    PostgresAdminOperationStore,
)
from auraclaw.infrastructure.persistence.postgres_artifact_repository import (
    PostgresArtifactRepository,
)
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_control_store import (
    PostgresControlStateStore,
)
from auraclaw.infrastructure.persistence.postgres_credential_registry import (
    PostgresCredentialRegistry,
)
from auraclaw.infrastructure.persistence.postgres_fencing_ledger import (
    FencingLedgerOwner,
    PostgresFencingTokenLedger,
)
from auraclaw.infrastructure.persistence.postgres_invocation_store import (
    PostgresInvocationStore,
)
from auraclaw.infrastructure.persistence.postgres_mcp_registry import (
    PostgresMcpServerRegistryStore,
)
from auraclaw.infrastructure.persistence.postgres_model_store import (
    PostgresModelStateStore,
)
from auraclaw.infrastructure.persistence.postgres_policy_store import (
    PostgresPolicyStateStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_publishers import (
    PostgresSkillPublisherStore,
)
from auraclaw.infrastructure.persistence.postgres_tool_registry import (
    PostgresToolRegistryStore,
)
from auraclaw.infrastructure.projection.postgres_approval_store import (
    PostgresApprovalProjection,
)
from auraclaw.infrastructure.projection.postgres_collaboration_store import (
    PostgresCollaborationProjection,
)
from auraclaw.infrastructure.projection.postgres_task_store import (
    PostgresTaskProjection,
)
from auraclaw.internal.http import HttpContractClient, create_contract_app
from auraclaw.internal.routes import (
    admin_routes,
    artifact_routes,
    collaboration_routes,
    control_routes,
    credential_routes,
    mcp_registry_routes,
    model_routes,
    model_stream_routes,
    policy_routes,
    session_routes,
    skill_publication_routes,
)
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
    LeaseAssertionSigner,
    LeaseAssertionVerifier,
)
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.observability.service import ObservabilityService
from auraclaw.policy.internal_service import PolicyInternalService
from auraclaw.projection.approval.projector import CompositeProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.collaboration_controller import RuntimeCollaborationController
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.harness import AgentHarness
from auraclaw.session.collaboration_internal_service import CollaborationInternalService
from auraclaw.session.collaboration_service import CollaborationService
from auraclaw.session.internal_service import SessionInternalService
from auraclaw.session.task_service import TaskService

logger = logging.getLogger(__name__)
_SKILL_PACKAGE_REGISTRY: SkillPackageRegistry | None = None


def _mcp_registry_service(
    settings: Settings,
) -> tuple[
    McpServerRegistryService,
    InMemoryMcpServerRegistryStore | PostgresMcpServerRegistryStore,
]:
    store: InMemoryMcpServerRegistryStore | PostgresMcpServerRegistryStore
    if settings.sql_storage_enabled:
        store = PostgresMcpServerRegistryStore(settings.resolved_database_url)
    else:
        store = InMemoryMcpServerRegistryStore()
    allow_private_none = (
        settings.mcp_allow_private_auth_none
        if settings.mcp_allow_private_auth_none is not None
        else settings.deployment_profile == "development"
    )
    return (
        McpServerRegistryService(store, allow_private_auth_none=allow_private_none),
        store,
    )


def _capability_catalog_store(
    settings: Settings,
) -> InMemoryCapabilityCatalogStore | PostgresCapabilityCatalogStore:
    if settings.sql_storage_enabled:
        return PostgresCapabilityCatalogStore(settings.resolved_database_url)
    return InMemoryCapabilityCatalogStore()


def _skill_registry_service(
    settings: Settings,
    *,
    artifacts: ArtifactWriter | None = None,
) -> SkillPackageRegistry:
    global _SKILL_PACKAGE_REGISTRY
    if artifacts is None and _SKILL_PACKAGE_REGISTRY is not None:
        return _SKILL_PACKAGE_REGISTRY
    configured_signing_key = (
        settings.skill_signing_key.get_secret_value().encode()
        if settings.skill_signing_key is not None
        else None
    )
    signing_key = configured_signing_key or b"auraclaw-development-platform-skill-key"
    registry = SkillPackageRegistry(
        artifacts=(artifacts or ArtifactStore(InMemoryObjectStorage(), signing_key=signing_key)),
        signature_verifier=HmacSkillSignatureVerifier({"platform": signing_key}),
        resources=HandsResourceRegistry(),
    )
    if artifacts is None:
        _SKILL_PACKAGE_REGISTRY = registry
    return registry


def _skill_publication_service(
    settings: Settings,
    registry: SkillPackageRegistry,
    lifecycle: SkillLifecycleStore | None = None,
    artifact_reader: ArtifactContentReader | None = None,
    artifact_lifecycle: SkillArtifactLifecycle | None = None,
    publisher_trust: SkillPublisherTrustService | None = None,
) -> tuple[SkillPublicationService, SkillLifecycleStore]:
    selected_lifecycle = lifecycle
    if selected_lifecycle is None:
        if settings.sql_storage_enabled:
            selected_lifecycle = PostgresSkillLifecycleStore(settings.resolved_database_url)
        else:
            selected_lifecycle = InMemorySkillLifecycleStore()
    now = datetime.now(UTC)
    admin_upload = SkillSourceRecord(
        source_id="sks_admin_upload",
        tenant_id="*",
        kind=SkillSourceKind.ADMIN_UPLOAD,
        desired_state=SkillSourceDesiredState.ENABLED,
        publisher_allowlist=("platform",),
        created_by="system",
        updated_by="system",
        created_at=now,
        updated_at=now,
    )
    return (
        SkillPublicationService(
            registry=registry,
            lifecycle=selected_lifecycle,
            artifacts=artifact_reader,
            artifact_lifecycle=artifact_lifecycle,
            publisher_trust=publisher_trust,
            bootstrap_sources=(admin_upload,),
        ),
        selected_lifecycle,
    )


async def _hands_mcp_snapshot(
    settings: Settings,
) -> tuple[McpActiveSnapshotEntry, ...] | None:
    token = settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
    if not token:
        return None
    client = httpx.AsyncClient(base_url=settings.hands_url)
    contract = HttpContractClient(client, bearer_token=token)
    try:
        response = await contract.call(
            "/internal/v1/mcp-registry/snapshot",
            McpRegistrySnapshotRequest(
                context=InternalRequestContext(
                    tenant_id="platform",
                    service_identity=ServiceIdentity.CREDENTIAL_PROXY,
                    request_id=secrets.token_hex(12),
                    correlation_id="mcp-egress-restore",
                    causation_id="mcp-egress-restore",
                )
            ),
            McpRegistrySnapshotResponse,
        )
    except Exception:
        logger.warning("MCP registry snapshot is unavailable; retrying on the next tick")
        return None
    finally:
        await client.aclose()
    return tuple(McpActiveSnapshotEntry.model_validate(item) for item in response.servers)


def _worker_idle_interval(settings: Settings, configured: float) -> float:
    if settings.worker_wake_enabled:
        return settings.worker_idle_interval
    return configured


def _worker_post_tick_wait(settings: Settings, configured: float) -> float:
    """Idle sleep after a tick that already long-polled the Session outbox."""
    if settings.worker_wake_enabled:
        return 0.05
    return configured


SERVICE_BY_COMMAND = {
    "api": "task-api",
    "session": "session",
    "projection": "projection-worker",
    "orchestrator": "orchestrator",
    "runtime": "agent-runtime",
    "model-gateway": "model-gateway",
    "hands": "action-hands",
    "policy": "policy",
    "credential-proxy": "credential-proxy",
    "artifact": "artifact-service",
    "streaming": "streaming-gateway",
    "delivery": "delivery-worker",
}

WORKER_SERVICES = {
    "projection-worker",
    "orchestrator",
    "agent-runtime",
    "delivery-worker",
    "action-hands",
}

DATABASE_SERVICES = {
    "session",
    "projection-worker",
    "orchestrator",
    "action-hands",
    "policy",
    "credential-proxy",
    "artifact-service",
    "model-gateway",
    "streaming-gateway",
    "delivery-worker",
}


@dataclass(frozen=True)
class ServiceSpec:
    command: str
    name: str
    port: int
    worker: bool


class EmptyApprovalReader:
    async def get(self, tenant_id: str, approval_id: str) -> None:
        del tenant_id, approval_id
        return None

    async def find_approved(
        self,
        tenant_id: str,
        session_id: str,
        digest: str,
        policy_version: str,
    ) -> None:
        del tenant_id, session_id, digest, policy_version
        return None


class UnavailableModelClient:
    async def generate(self, request: Any) -> Any:
        del request
        raise RuntimeError("model provider is not configured")

    async def aclose(self) -> None:
        return None


class RemoteRuntimeWorker:
    def __init__(
        self,
        control: RemoteRuntimeControlClient,
        harness: AgentHarness,
        *,
        heartbeat_interval: timedelta | None = None,
    ) -> None:
        self._control = control
        self._harness = harness
        self._registered = False
        # Must stay below recover_expired's stale-heartbeat window (30s), or a
        # healthy long model call can be mistaken for a dead runtime.
        self._heartbeat_interval = heartbeat_interval or timedelta(seconds=10)
        self._last_heartbeat_at = 0.0

    async def tick(self) -> int:
        now = time.monotonic()
        if not self._registered:
            await self._control.register()
            self._registered = True
            self._last_heartbeat_at = now
        elif now - self._last_heartbeat_at >= self._heartbeat_interval.total_seconds():
            await self._control.heartbeat()
            self._last_heartbeat_at = time.monotonic()
        assignments = await self._control.claim(
            limit=max(1, int(getattr(self._control, "capacity", 1)))
        )
        if assignments:
            await asyncio.gather(
                *(self._run_assignment(assignment) for assignment in assignments)
            )
        return len(assignments)

    async def _run_assignment(self, assignment: RuntimeAssignment) -> None:
        task_id = f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
        try:
            await self._execute_with_heartbeat(assignment)
        except (FencingTokenError, LeaseConflictError):
            logger.warning(
                "runtime lost lease for %s; abandoning stale assignment",
                task_id,
                exc_info=True,
            )
            try:
                await self._control.abandon_assignment(
                    task_id,
                    runtime_id=assignment.runtime_id,
                    lease_id=assignment.lease_id,
                    fencing_token=assignment.fencing_token,
                )
            except Exception:
                logger.exception("failed to abandon stale assignment %s", task_id)
            return
        except Exception as exc:
            try:
                await self._harness.record_failure(assignment, exc)
            except Exception:
                logger.exception(
                    "failed to record run failure for %s; leaving assignment running",
                    task_id,
                )
                raise
            try:
                await self._control.finish_assignment(task_id, "failed")
            except Exception:
                logger.exception(
                    "failed to disposition assignment %s after recorded failure", task_id
                )
            raise

    async def _execute_with_heartbeat(self, assignment: RuntimeAssignment) -> None:
        stop = asyncio.Event()
        execute = asyncio.create_task(
            self._harness.execute(assignment),
            name=f"runtime-harness-{assignment.run_id}",
        )

        async def keep_alive() -> None:
            failures = 0
            while not stop.is_set():
                delay = self._heartbeat_interval.total_seconds()
                try:
                    renew = getattr(self._control, "renew_assignment", None)
                    if callable(renew):
                        await renew(assignment)
                    else:
                        await self._control.heartbeat()
                    self._last_heartbeat_at = time.monotonic()
                    failures = 0
                except Exception as exc:
                    failures += 1
                    retry_base = min(
                        self._heartbeat_interval.total_seconds(), 1.0
                    )
                    delay = min(
                        self._heartbeat_interval.total_seconds(),
                        retry_base * (2 ** (failures - 1)),
                    )
                    # Transient control-plane blips must not stop keep-alive;
                    # exiting here leaves last_heartbeat_at stale and
                    # recover_expired can reclaim a still-running long call.
                    logger.warning(
                        "runtime heartbeat failed during execute for session=%s run=%s; will retry",
                        assignment.session_id,
                        assignment.run_id,
                        exc_info=True,
                    )
                    expires_at = assignment.execution_claim_expires_at
                    if expires_at is not None and expires_at <= datetime.now(UTC):
                        execute.cancel()
                        raise LeaseConflictError(
                            "execution claim renewal safety window elapsed"
                        ) from exc
                try:
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=delay,
                    )
                    return
                except TimeoutError:
                    continue

        heartbeats = asyncio.create_task(keep_alive(), name="remote-runtime-heartbeat")
        try:
            done, _ = await asyncio.wait(
                {execute, heartbeats}, return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeats in done:
                await heartbeats
            await execute
        finally:
            stop.set()
            if not heartbeats.done():
                await heartbeats


def _runtime_instance_identity(settings: Settings) -> tuple[str, str]:
    hostname = (
        os.getenv("AURACLAW_RUNTIME_INSTANCE_UID")
        or os.getenv("POD_UID")
        or socket.gethostname()
    )
    if settings.deployment_profile == "production":
        runtime_id = settings.runtime_id or f"runtime-{hostname}"
        node_id = (
            hostname
            if settings.runtime_node_id in {None, "local"}
            else settings.runtime_node_id or hostname
        )
        return runtime_id, node_id
    return settings.runtime_id or "runtime-local-1", settings.runtime_node_id or "local"


def _configured_identities(
    settings: Settings, identities: tuple[ServiceIdentity, ...]
) -> dict[str, ServiceIdentity]:
    configured: dict[str, ServiceIdentity] = {}
    for identity in identities:
        token = settings.workload_token_value(identity.value)
        if token:
            configured[token] = identity
    return configured


def _agent_runtime_token(settings: Settings) -> str | None:
    return settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value)


def _service_bearer_token(settings: Settings, identity: ServiceIdentity) -> str:
    return settings.workload_token_value(identity.value) or secrets.token_urlsafe(32)


def _lease_signing_key(settings: Settings) -> bytes:
    if settings.lease_signing_key is not None:
        value = settings.lease_signing_key.get_secret_value().encode()
        if len(value) >= 32:
            return value
    return secrets.token_bytes(32)


def _has_workload_tokens(settings: Settings, identities: tuple[ServiceIdentity, ...]) -> bool:
    return all(settings.workload_token_value(identity.value) for identity in identities)


def _require_production_security_configuration(
    settings: Settings,
    service_name: str,
    identities: tuple[ServiceIdentity, ...],
    *,
    requires_policy: bool = False,
) -> None:
    if settings.deployment_profile != "production":
        return
    missing = [
        identity.value
        for identity in identities
        if not settings.workload_token_value(identity.value)
    ]
    if requires_policy and not settings.policy_base_url.strip():
        missing.append("policy-base-url")
    if missing:
        raise ValueError(
            f"{service_name} production security configuration is missing: "
            + ", ".join(missing)
        )


def _lease_key_configured(settings: Settings) -> bool:
    return (
        settings.lease_signing_key is not None
        and len(settings.lease_signing_key.get_secret_value()) >= 32
    )


def _fencing_token_ledger(
    settings: Settings,
    owner: FencingLedgerOwner,
) -> InMemoryFencingTokenLedger | PostgresFencingTokenLedger:
    if settings.sql_storage_enabled:
        return PostgresFencingTokenLedger(
            settings.resolved_database_url,
            owner=owner,
        )
    if settings.deployment_profile == "production":
        raise ValueError(
            f"{owner} production composition requires a persistent fencing token ledger"
        )
    return InMemoryFencingTokenLedger()


async def _seed_managed_connector_credentials(
    proxy: CredentialProxy,
    settings: Settings,
) -> int:
    expires_at = datetime.now(UTC) + timedelta(days=365)
    debug_tenants = (
        ("local", "development", "1") if settings.deployment_profile == "development" else ()
    )
    seeded = 0
    for java_server in settings.java_api_servers:
        if java_server.credential_ref is None:
            continue
        reference = CredentialReference(
            credential_ref=java_server.credential_ref,
            provider=java_server.server_id,
            account_scope=java_server.base_url,
            allowed_operations=("http.invoke",),
            expires_at=expires_at,
        )
        tenants = {java_server.tenant_id or "platform", *debug_tenants}
        for tenant_id in tenants:
            seeded += int(await proxy.seed_reference(tenant_id, reference))
    return seeded


def _task_api_app(settings: Settings) -> FastAPI:
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
        skill_source_management = skill_publication
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
        skill_source_management = SkillSourceService(
            skill_lifecycle,
            projector=InProcessSkillStateProjector(
                lifecycle=skill_lifecycle,
                registry=skill_registry,
            ),
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
            source_service=skill_source_management,
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


def _streaming_app(settings: Settings) -> FastAPI:
    if not settings.sql_storage_enabled:
        raise ValueError(
            "streaming-gateway requires SQL storage; use `auraclaw serve` with .env.dev "
            "configured for PostgreSQL or Kingbase"
        )
    app = create_app(profile="streaming-gateway")
    projection = PostgresTaskProjection(settings.resolved_database_url)
    gateway = StreamingGateway(
        reader=projection,
        bus=providers.get_runtime_replay_bus(),
        delta_min_interval=settings.stream_delta_min_interval_seconds,
    )
    app.dependency_overrides[get_streaming_gateway] = lambda: gateway
    app.state.closeables = (projection,)
    app.state.storage_label = "projection-read-only"
    app.state.session_access = "database"
    return app


def service_spec(command: str, settings: Settings | None = None) -> ServiceSpec:
    selected = settings or get_settings()
    try:
        name = SERVICE_BY_COMMAND[command]
    except KeyError as exc:
        raise ValueError(f"unknown service command: {command}") from exc
    return ServiceSpec(
        command=command,
        name=name,
        port=selected.service_port(name),
        worker=name in WORKER_SERVICES,
    )


def _readiness(name: str, settings: Settings) -> tuple[bool, dict[str, str]]:
    dependencies: dict[str, str] = {}
    ready = True
    if name in DATABASE_SERVICES:
        dependencies["database"] = (
            settings.storage_label if settings.sql_storage_enabled else "missing"
        )
        ready = ready and settings.sql_storage_enabled
    if name == "task-api":
        identity_ready = (
            settings.insecure_identity_headers_enabled or settings.signed_identity_configured
        )
        dependencies["chaintower_identity"] = (
            "insecure-headers"
            if settings.insecure_identity_headers_enabled
            else "ready"
            if identity_ready
            else "missing"
        )
        ready = ready and identity_ready
    if name == "model-gateway":
        dependencies["model_provider"] = "ready" if settings.model_gateway_configured else "missing"
        ready = ready and settings.model_gateway_configured
        identity_ready = bool(settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value))
        dependencies["runtime_workload_identity"] = "ready" if identity_ready else "missing"
        ready = ready and identity_ready
    if name == "session":
        lease_ready = _lease_key_configured(settings)
        dependencies["lease_signing_key"] = "ready" if lease_ready else "missing"
        ready = ready and lease_ready
        required_identities = (
            ServiceIdentity.TASK_API,
            ServiceIdentity.PROJECTION_WORKER,
            ServiceIdentity.ORCHESTRATOR,
            ServiceIdentity.AGENT_RUNTIME,
            ServiceIdentity.POLICY,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.STREAMING_GATEWAY,
        )
        identity_ready = _has_workload_tokens(settings, required_identities)
        dependencies["workload_identities"] = "ready" if identity_ready else "missing"
        ready = ready and identity_ready
    if name == "orchestrator":
        lease_ready = _lease_key_configured(settings)
        identity_ready = _has_workload_tokens(
            settings,
            (ServiceIdentity.AGENT_RUNTIME, ServiceIdentity.TASK_API),
        )
        dependencies["lease_signer"] = "ready" if lease_ready else "missing"
        dependencies["control_workload_identities"] = "ready" if identity_ready else "missing"
        ready = ready and lease_ready and identity_ready
    if name == "artifact-service":
        storage_ready = settings.object_storage_enabled or (
            settings.deployment_profile == "development"
            and settings.resolved_artifact_backend == "local"
        )
        dependencies["object_storage"] = (
            settings.resolved_artifact_backend
            if settings.object_storage_enabled
            else "local"
            if settings.resolved_artifact_backend == "local"
            else "missing"
        )
        policy_identity_ready = bool(
            settings.workload_token_value(ServiceIdentity.ARTIFACT_SERVICE.value)
        )
        dependencies["policy_workload_identity"] = "ready" if policy_identity_ready else "missing"
        ready = ready and storage_ready and policy_identity_ready
    if name == "projection-worker":
        token_ready = bool(settings.workload_token_value(ServiceIdentity.PROJECTION_WORKER.value))
        dependencies["session_workload_identity"] = "ready" if token_ready else "missing"
        ready = ready and token_ready
    if name == "agent-runtime":
        token_ready = bool(settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value))
        provider_secret_absent = settings.model_api_key is None
        dependencies["workload_identity"] = "ready" if token_ready else "missing"
        dependencies["provider_secret_isolation"] = (
            "ready" if provider_secret_absent else "forbidden"
        )
        ready = ready and token_ready and provider_secret_absent
    if name == "action-hands":
        identity_ready = settings.runtime_workload_token is not None and bool(
            settings.runtime_workload_token.get_secret_value()
        )
        dependencies["workload_identity"] = "ready" if identity_ready else "missing"
        lease_ready = _lease_key_configured(settings)
        dependencies["lease_verifier"] = "ready" if lease_ready else "missing"
        downstream_identity_ready = bool(
            settings.workload_token_value(ServiceIdentity.ACTION_HANDS.value)
        )
        dependencies["downstream_workload_identity"] = (
            "ready" if downstream_identity_ready else "missing"
        )
        publish_identity_ready = bool(settings.workload_token_value(ServiceIdentity.TASK_API.value))
        dependencies["skill_publish_workload_identity"] = (
            "ready" if publish_identity_ready else "missing"
        )
        ready = (
            ready
            and identity_ready
            and lease_ready
            and downstream_identity_ready
            and publish_identity_ready
        )
    if name == "policy":
        identities = (
            ServiceIdentity.TASK_API,
            ServiceIdentity.ORCHESTRATOR,
            ServiceIdentity.MODEL_GATEWAY,
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.CREDENTIAL_PROXY,
            ServiceIdentity.ARTIFACT_SERVICE,
        )
        identity_ready = _has_workload_tokens(settings, identities)
        dependencies["enforcement_identities"] = "ready" if identity_ready else "missing"
        ready = ready and identity_ready
    if name == "credential-proxy":
        identity_ready = _has_workload_tokens(
            settings,
            (
                ServiceIdentity.TASK_API,
                ServiceIdentity.ACTION_HANDS,
                ServiceIdentity.DELIVERY_WORKER,
            ),
        )
        vault_configured = bool(settings.credential_vault_addr) and (
            settings.credential_vault_token is not None
        )
        vault_ready = vault_configured or (
            settings.deployment_profile == "development"
            and not settings.credential_vault_addr
        )
        dependencies["caller_identities"] = "ready" if identity_ready else "missing"
        dependencies["vault"] = (
            "ready" if vault_configured else ("memory" if vault_ready else "missing")
        )
        policy_identity_ready = bool(
            settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
        )
        dependencies["policy_workload_identity"] = "ready" if policy_identity_ready else "missing"
        ready = ready and identity_ready and vault_ready and policy_identity_ready
    if name == "delivery-worker":
        identity_ready = bool(settings.workload_token_value(ServiceIdentity.DELIVERY_WORKER.value))
        dependencies["delivery_workload_identity"] = "ready" if identity_ready else "missing"
        ready = ready and identity_ready
    return ready, dependencies


@asynccontextmanager
async def _service_lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.started_at = datetime.now(UTC)
    app.state.stopping = False
    app.state.worker_iterations = 0
    stop = asyncio.Event()
    worker_task: asyncio.Task[None] | None = None
    log_level = str(getattr(app.state, "log_level", "INFO") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
        force=True,
    )
    if getattr(app.state, "worker_wake", None) is None and bool(app.state.worker):
        app.state.worker_wake = WorkerWakeGate()
    initialize = getattr(app.state, "initialize", None)
    if initialize is not None:
        await initialize()

    async def worker_loop() -> None:
        while not stop.is_set():
            processed = 0
            tick = getattr(app.state, "tick", None)
            if tick is not None:
                try:
                    result = await tick()
                    processed = int(result or 0)
                    app.state.worker_error = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    app.state.worker_error = type(exc).__name__
            app.state.worker_iterations += 1
            if stop.is_set():
                break
            if processed > 0:
                # Drain remaining work immediately after a productive tick.
                continue
            wake = getattr(app.state, "worker_wake", None)
            if isinstance(wake, WorkerWakeGate):
                woke = await wake.wait(app.state.worker_interval)
                if stop.is_set():
                    break
                if woke:
                    continue
            else:
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=app.state.worker_interval)

    if bool(app.state.worker):
        worker_task = asyncio.create_task(worker_loop(), name=f"{app.state.service_name}-worker")
    try:
        yield
    finally:
        app.state.stopping = True
        stop.set()
        wake = getattr(app.state, "worker_wake", None)
        if isinstance(wake, WorkerWakeGate):
            wake.signal()
        if worker_task is not None:
            with suppress(asyncio.CancelledError):
                await worker_task
        for closeable in getattr(app.state, "closeables", ()):
            close = getattr(closeable, "aclose", None)
            if close is None:
                close = closeable.close
            await close()


def _base_service_app(
    spec: ServiceSpec,
    settings: Settings,
    *,
    tick: Callable[[], Awaitable[int | None]] | None = None,
    worker_interval: float = 1.0,
    closeables: tuple[Any, ...] = (),
    readiness_probe: Callable[[], Awaitable[tuple[bool, str]]] | None = None,
) -> FastAPI:
    app = FastAPI(
        title=f"AuraClaw {spec.name}",
        version=__version__,
        lifespan=_service_lifespan,
    )
    app.state.service_name = spec.name
    app.state.worker = spec.worker
    app.state.tick = tick
    app.state.worker_interval = worker_interval
    app.state.closeables = closeables
    app.state.worker_error = None
    app.state.worker_wake = WorkerWakeGate() if spec.worker else None
    app.state.log_level = settings.log_level
    ready, dependencies = _readiness(spec.name, settings)
    app.state.config_ready = ready
    app.state.dependencies = dependencies
    app.state.readiness_probe = readiness_probe

    @app.get("/health/live")
    async def live(request: Request) -> JSONResponse:
        stopping = bool(getattr(request.app.state, "stopping", False))
        return JSONResponse(
            status_code=503 if stopping else 200,
            content={"status": "stopping" if stopping else "ok", "service": spec.name},
        )

    @app.get("/health/ready")
    async def ready_status(request: Request) -> JSONResponse:
        configured = bool(request.app.state.config_ready)
        probe = request.app.state.readiness_probe
        if configured and probe is not None:
            try:
                probe_ready, probe_detail = await probe()
            except Exception as exc:
                probe_ready, probe_detail = False, type(exc).__name__
            request.app.state.dependencies["connectivity"] = (
                "ready" if probe_ready else f"unavailable:{probe_detail}"
            )
            configured = configured and probe_ready
        stopping = bool(getattr(request.app.state, "stopping", False))
        worker_error = getattr(request.app.state, "worker_error", None)
        is_ready = configured and not stopping and worker_error is None
        return JSONResponse(
            status_code=200 if is_ready else 503,
            content={
                "status": "ready" if is_ready else "degraded",
                "service": spec.name,
                "dependencies": dict(request.app.state.dependencies),
                "worker_error": worker_error,
            },
        )

    @app.get("/internal/info")
    async def info(request: Request) -> dict[str, Any]:
        return {
            "service": spec.name,
            "worker": spec.worker,
            "worker_iterations": int(request.app.state.worker_iterations),
            "deployment_profile": settings.deployment_profile,
        }

    if spec.worker:

        @app.post("/internal/v1/worker/wake")
        async def wake_worker(request: Request) -> Response:
            wake = getattr(request.app.state, "worker_wake", None)
            if isinstance(wake, WorkerWakeGate):
                wake.signal()
            return Response(status_code=204)

    return app


def _session_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    wake: OutboxWakeNotifier | None = None
    closeables: tuple[Any, ...] = ()
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


def _orchestrator_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
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
        HttpWorkerWakeClient(settings.runtime_base_url) if settings.worker_wake_enabled else None
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
        # Keep recover off the create→schedule hot path; run it when idle.
        if ingested == 0 and scheduled is None:
            recovered = await orchestrator.recover()
            if recovered:
                scheduled = await orchestrator.schedule_once()
        return ingested + recovered + int(scheduled is not None)

    tick = schedule_tick
    closeables += (feed_session, lifecycle_session)
    if runtime_wake_client is not None:
        closeables += (runtime_wake_client,)
    app = _base_service_app(
        spec,
        settings,
        tick=tick,
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


def _hands_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
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
    closeables: tuple[Any, ...] = ()
    fencing_ledger = _fencing_token_ledger(settings, "hands")
    policy: RemotePolicyClient
    credential_proxy: RemoteCredentialProxy | None = None
    artifacts: RemoteArtifactWriter
    invocation_store: PostgresInvocationStore | None = None
    tool_registry_store: PostgresToolRegistryStore | None = None
    capability_catalog_store = _capability_catalog_store(settings)
    mcp_registry, mcp_registry_store = _mcp_registry_service(settings)
    remote_clients: list[Any] = []
    hands_token = _service_bearer_token(settings, ServiceIdentity.ACTION_HANDS)
    policy = RemotePolicyClient(settings.policy_base_url, bearer_token=hands_token)
    credential_proxy = RemoteCredentialProxy(
        settings.credential_proxy_base_url, bearer_token=hands_token
    )
    mcp_egress_client = RemoteMcpEgressClient(
        settings.credential_proxy_base_url, bearer_token=hands_token
    )
    remote_clients.extend((policy, credential_proxy, mcp_egress_client))
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
    skill_publisher_store: PostgresSkillPublisherStore | InMemorySkillPublisherStore
    if settings.sql_storage_enabled:
        invocation_store = PostgresInvocationStore(settings.resolved_database_url)
        tool_registry_store = PostgresToolRegistryStore(settings.resolved_database_url)
        skill_lifecycle: SkillLifecycleStore = PostgresSkillLifecycleStore(
            settings.resolved_database_url
        )
        skill_publisher_store = PostgresSkillPublisherStore(settings.resolved_database_url)
    else:
        skill_lifecycle = InMemorySkillLifecycleStore()
        skill_publisher_store = InMemorySkillPublisherStore()
    skill_publishers = SkillPublisherService(skill_publisher_store)
    publisher_trust = SkillPublisherTrustService(skill_publisher_store)
    closeables = (
        *remote_clients,
        artifacts,
        artifact_reader,
        skill_artifacts,
        skill_binding_references,
        *((invocation_store,) if invocation_store is not None else ()),
        *((tool_registry_store,) if tool_registry_store is not None else ()),
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
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
    )
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
    skill_rebuilder = SkillStateRebuilder(
        lifecycle=skill_lifecycle,
        artifacts=artifact_reader,
        registry=skill_registry,
        catalog=capability_catalog,
        publisher_trust=publisher_trust,
    )
    skill_reliability = SkillPublicationReliabilityWorker(
        lifecycle=skill_lifecycle,
        artifacts=skill_artifacts,
        rebuilder=skill_rebuilder,
        owner=f"action-hands-{secrets.token_hex(8)}",
    )
    skill_admission_maintenance = SkillAdmissionMaintenanceWorker(
        skill_lifecycle,
        retention=timedelta(days=settings.skill_admission_retention_days),
        batch_size=settings.skill_admission_cleanup_batch_size,
    )
    skill_management = SkillManagementService(
        lifecycle=skill_lifecycle,
        projector=skill_rebuilder,
        artifacts=artifact_reader,
        binding_references=skill_binding_references,
        retired_activator=skill_publication,
    )
    resources = skill_registry.resources or HandsResourceRegistry()
    resource_gateway = ManagedResourceGateway(
        resources,
        artifacts=artifacts,
        policy=policy if isinstance(policy, RemotePolicyClient) else None,
    )
    skill_resolver = SkillResolver(
        skill_registry,
        capability_catalog_store,
        policy if isinstance(policy, RemotePolicyClient) else None,
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
        if tool_registry_store is not None:
            await tool_registry_store.load_into(registry)
        if credential_proxy is not None and isinstance(policy, RemotePolicyClient):

            def _mcp_connector(server: object) -> ManagedMcpConnector:
                from auraclaw.contracts.capabilities import McpServerDefinition

                assert isinstance(server, McpServerDefinition)
                return ManagedMcpConnector(
                    server,
                    credentials=credential_proxy,
                    policy=policy,
                )

            manager = McpConnectionManager(
                registry=mcp_registry,
                connectors=app.state.capability_connectors,
                factory=_mcp_connector,  # type: ignore[arg-type]
                catalog=capability_catalog,
                reconciler=app.state.catalog_reconciler,
                egress=mcp_egress_client,
                instance_id=f"action-hands-{secrets.token_hex(8)}",
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
                        "Action Hands startup dependency is unavailable; "
                        "retrying MCP restore (%s/20, error=%s)",
                        attempt + 1,
                        type(exc).__name__,
                    )
                    await asyncio.sleep(0.5)
        for java_server in settings.java_api_servers:
            catalog_server = catalog_server_definition(java_server)
            await capability_catalog.register_server(catalog_server)
            if credential_proxy is not None and isinstance(policy, RemotePolicyClient):
                app.state.capability_connectors[java_server.server_id] = ManagedJavaApiConnector(
                    java_server,
                    credentials=credential_proxy,
                    policy=policy,
                )
        app.state.skill_rebuild_result = await skill_rebuilder.rebuild_all()
        app.state.skill_installation_drained = await skill_management.reconcile_draining()
        app.state.skill_reliability_result = await skill_reliability.run_once()
        app.state.skill_admission_maintenance_result = await skill_admission_maintenance.run_once()

    app.state.capability_connectors = {}
    app.state.catalog_reconciler = None
    app.state.initialize = initialize_registry
    routed_hands = RoutedHandsExecutor(
        LocalHandsService(workspace_root=Path.cwd()),
        {
            CAPABILITY_SEARCH_TOOL_NAME: CapabilitySearchExecutor(
                capability_catalog,
            ),
            CAPABILITY_LOAD_TOOL_NAME: CapabilityLoadExecutor(
                capability_catalog,
            ),
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
        approval_controller=policy if isinstance(policy, RemotePolicyClient) else None,
        instance_id=f"action-hands-{secrets.token_hex(8)}",
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
        (
            "skill-state",
            settings.mcp_reconcile_interval_seconds,
            rebuild_skill_state,
        )
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
    if credential_proxy is not None and isinstance(policy, RemotePolicyClient):
        reconciler = CapabilityCatalogReconciler(
            catalog=capability_catalog,
            store=capability_catalog_store,
            connectors=app.state.capability_connectors,
            resource_cache=resource_gateway,
            tool_registry=registry,
            hands_router=routed_hands,
            trust_remote_tool_annotations=settings.mcp_trust_remote_tool_annotations,
        )
        skill_reconciler = SkillPackageReconciler(
            store=capability_catalog_store,
            connectors=app.state.capability_connectors,
            lifecycle=skill_lifecycle,
            publication=skill_publication,
            rebuilder=skill_rebuilder,
            snapshot_provider=reconciler.snapshot_for,
        )
        app.state.catalog_reconciler = reconciler
        app.state.skill_reconciler = skill_reconciler

        async def initialize_remote_catalog() -> None:
            await initialize_registry()
            for connector in app.state.capability_connectors.values():
                setter = getattr(connector, "set_notification_handler", None)
                if setter is not None:
                    setter(reconciler.handle_notification)
            await reconciler.reconcile_all()
            await skill_reconciler.reconcile_all()

        async def reconcile_catalog_and_skills() -> int:
            results = await reconciler.reconcile_all_results()
            manager = getattr(app.state, "mcp_connection_manager", None)
            if manager is not None:
                await manager.record_reconcile_results(results)
            await skill_reconciler.reconcile_all()
            return sum(
                result.status is CapabilityStatus.ACTIVE for result in results
            )

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
    if periodic_jobs:
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
            for (name, interval, _run), outcome in zip(
                due,
                outcomes,
                strict=True,
            ):
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
                    rebuilder=skill_rebuilder,
                    publishers=skill_publishers,
                    admissions=skill_lifecycle,
                    sources=SkillSourceService(
                        skill_lifecycle,
                        synchronizer=getattr(app.state, "skill_reconciler", None),
                        projector=skill_rebuilder,
                    ),
                    artifacts=artifact_reader,
                )
            ),
            workload_identities=_configured_identities(settings, (ServiceIdentity.TASK_API,)),
        ),
    )
    app.mount("/", hands_http_app)
    return app


def _policy_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
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


def _credential_proxy_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    _require_production_security_configuration(
        settings,
        spec.name,
        (
            ServiceIdentity.TASK_API,
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.CREDENTIAL_PROXY,
        ),
        requires_policy=True,
    )
    if settings.deployment_profile == "production":
        if settings.debug_vault_secrets:
            raise ValueError("credential-proxy production forbids debug Vault secrets")
        if not settings.credential_vault_addr or settings.credential_vault_token is None:
            raise ValueError("credential-proxy production requires an external Vault")
    registry = (
        PostgresCredentialRegistry(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    vault: InMemoryVault | HashiCorpVault
    if settings.credential_vault_addr and settings.credential_vault_token is not None:
        vault = HashiCorpVault(
            settings.credential_vault_addr,
            token=settings.credential_vault_token.get_secret_value(),
            mount=settings.credential_vault_mount,
        )
    else:
        vault = InMemoryVault(settings.debug_vault_secrets)
    policy: RemotePolicyClient | None = None
    token = settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
    if token:
        policy = RemotePolicyClient(
            settings.policy_base_url,
            bearer_token=token,
            service_identity=ServiceIdentity.CREDENTIAL_PROXY,
        )
    java_api_adapters = {
        f"java-api:{server.server_id}": ManagedJavaApiEgressAdapter(server)
        for server in settings.java_api_servers
    }
    closeables: tuple[Any, ...] = (
        *((registry,) if registry is not None else ()),
        *((vault,) if isinstance(vault, HashiCorpVault) else ()),
        *((policy,) if policy is not None else ()),
        *java_api_adapters.values(),
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
        readiness_probe=(vault.readiness if isinstance(vault, HashiCorpVault) else None),
    )
    proxy = CredentialProxy(vault, registry=registry)
    adapters: dict[str, Any] = {
        "webhook": ManagedWebhookCredentialAdapter(
            allowed_hosts=settings.allowed_credential_egress_hosts
        ),
        **java_api_adapters,
    }
    mcp_egress = McpEgressManager(adapters=adapters, proxy=proxy)
    service = CredentialProxyInternalService(
        proxy,
        adapters=adapters,
        policy=policy,
        mcp_egress=mcp_egress,
    )
    async def restore_mcp_egress() -> None:
        snapshot = await _hands_mcp_snapshot(settings)
        if snapshot is None:
            return
        await mcp_egress.restore(snapshot)

    async def initialize() -> None:
        await _seed_managed_connector_credentials(proxy, settings)
        await restore_mcp_egress()

    async def reconcile_mcp_egress() -> int:
        snapshot = await _hands_mcp_snapshot(settings)
        if snapshot is None:
            return 0
        return await mcp_egress.reconcile(snapshot)

    app.state.initialize = initialize
    app.state.tick = reconcile_mcp_egress
    app.state.worker = True
    app.state.worker_interval = settings.mcp_revision_reconcile_interval_seconds
    contract_app = create_contract_app(
        "credential-proxy",
        credential_routes(service),
        workload_identities=_configured_identities(
            settings,
            (
                ServiceIdentity.TASK_API,
                ServiceIdentity.ACTION_HANDS,
                ServiceIdentity.DELIVERY_WORKER,
            ),
        ),
    )
    app.mount("/", contract_app)
    return app


def _artifact_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    _require_production_security_configuration(
        settings,
        spec.name,
        (
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.TASK_API,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.ARTIFACT_SERVICE,
        ),
        requires_policy=True,
    )
    storage = build_object_storage(settings)
    repository = (
        PostgresArtifactRepository(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    admin_store = (
        PostgresAdminOperationStore(settings.resolved_database_url, schema="artifact")
        if settings.sql_storage_enabled
        else None
    )
    policy: RemotePolicyClient | None = None
    token = settings.workload_token_value(ServiceIdentity.ARTIFACT_SERVICE.value)
    if token:
        policy = RemotePolicyClient(
            settings.policy_base_url,
            bearer_token=token,
            service_identity=ServiceIdentity.ARTIFACT_SERVICE,
        )
    closeables: tuple[Any, ...] = (
        *((repository,) if repository is not None else ()),
        *((admin_store,) if admin_store is not None else ()),
        *object_storage_closeables(storage),
        *((policy,) if policy is not None else ()),
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
        readiness_probe=(storage.verifier.readiness if storage.verifier is not None else None),
    )
    service = ArtifactInternalService(
        storage.presigner,
        repository=repository,
        object_verifier=storage.verifier,
        policy=policy,
        multipart=storage.multipart,
        multipart_threshold=settings.artifact_multipart_threshold,
        multipart_part_size=settings.artifact_multipart_part_size,
    )

    async def artifact_status(parameters: dict[str, Any]) -> dict[str, Any]:
        del parameters
        return {"storage": storage.backend, "metadata_owner": "artifact-service"}

    async def artifact_retention(parameters: dict[str, Any]) -> dict[str, Any]:
        del parameters
        deleted = await service.cleanup_expired()
        return {"expired_uploads_deleted": deleted}

    routes = artifact_routes(service)
    routes.update(
        admin_routes(
            OwnerAdminService(
                ServiceIdentity.ARTIFACT_SERVICE,
                {"status": artifact_status, "retention": artifact_retention},
                store=admin_store,
            )
        )
    )
    contract_app = create_contract_app(
        "artifact-service",
        routes,
        workload_identities=_configured_identities(
            settings,
            (
                ServiceIdentity.ACTION_HANDS,
                ServiceIdentity.TASK_API,
                ServiceIdentity.DELIVERY_WORKER,
            ),
        ),
    )
    app.mount("/", contract_app)
    return app


def _delivery_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    token = settings.workload_token_value(ServiceIdentity.DELIVERY_WORKER.value)
    bearer_token = token or secrets.token_urlsafe(32)
    claim_wait = settings.worker_idle_interval if settings.worker_wake_enabled else 0.0
    session = RemoteSessionEventStore(
        settings.session_base_url,
        service_identity=ServiceIdentity.DELIVERY_WORKER,
        bearer_token=bearer_token,
        timeout=max(10.0, settings.worker_idle_interval + 5.0),
    )
    outbox = RemoteSessionDeliveryOutboxSource(
        session,
        worker_id="delivery-worker",
        wait_seconds=claim_wait,
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
    )
    closeables: tuple[Any, ...] = (session, policy, credentials)
    if settings.sql_storage_enabled:
        closeables += (store, admin_store)
    app = _base_service_app(
        spec,
        settings,
        tick=worker.run_once,
        worker_interval=_worker_post_tick_wait(settings, 1.0),
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


def _projection_app(
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
    projector = CompositeProjection(*providers.session_outbox_projectors())
    admin_store = PostgresAdminOperationStore(settings.resolved_database_url, schema="projection")
    closeables: tuple[Any, ...] = ()
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
    relay = OutboxRelay(source, projector)
    closeables = (
        remote_session,
        task_projection,
        approval_projection,
        collaboration_projection,
        admin_store,
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


def _model_gateway_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    policy: RemotePolicyClient | None = None
    state = (
        PostgresModelStateStore(settings.resolved_database_url)
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
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=(
            *((policy,) if policy is not None else ()),
            *((state,) if state is not None else ()),
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
        workload_identities=_configured_identities(settings, (ServiceIdentity.AGENT_RUNTIME,)),
    )
    app.mount("/", contract_app)
    return app


def _runtime_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
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
        capability_controller=RuntimeCapabilityController(hands),
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


def create_service_app(
    command: str,
    settings: Settings | None = None,
    *,
    worker_interval: float = 1.0,
) -> FastAPI:
    selected = settings or get_settings()
    spec = service_spec(command, selected)
    if spec.name == "task-api":
        return _task_api_app(selected)
    if spec.name == "streaming-gateway":
        return _streaming_app(selected)
    if spec.name == "session":
        return _session_app(spec, selected)
    if spec.name == "action-hands":
        return _hands_app(spec, selected)
    if spec.name == "model-gateway":
        return _model_gateway_app(spec, selected)
    if spec.name == "policy":
        return _policy_app(spec, selected)
    if spec.name == "credential-proxy":
        return _credential_proxy_app(spec, selected)
    if spec.name == "artifact-service":
        return _artifact_app(spec, selected)
    if spec.name == "delivery-worker":
        return _delivery_app(spec, selected)
    if spec.name == "orchestrator":
        return _orchestrator_app(spec, selected)
    if spec.name == "agent-runtime":
        return _runtime_app(spec, selected)
    if spec.name == "projection-worker":
        return _projection_app(spec, selected, worker_interval=worker_interval)
    return _base_service_app(spec, selected)
