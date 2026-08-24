from __future__ import annotations

import asyncio
import logging
import secrets
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
    SKILL_RESOLVE_TOOL_NAME,
    CapabilityCatalog,
    CapabilityLoadExecutor,
    CapabilitySearchExecutor,
    InMemoryCapabilityCatalogStore,
    RoutedHandsExecutor,
    SkillResolveExecutor,
    capability_load_tool,
    capability_search_tool,
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
from auraclaw.action.resource_gateway import ManagedResourceGateway
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackageRegistry,
    SkillResolver,
)
from auraclaw.action.skill_reconciler import SkillPackageReconciler
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.api.dependencies import (
    get_collaboration_projection,
    get_observability_service,
    get_streaming_gateway,
    get_task_command_gateway,
    get_task_projection,
    get_task_query_service,
)
from auraclaw.api.routes.admin_mcp import create_mcp_admin_router
from auraclaw.artifact.internal_service import (
    ArtifactInternalService,
    SeaweedFSObjectVerifier,
)
from auraclaw.composition import providers
from auraclaw.composition.api import create_app
from auraclaw.composition.worker_wake import WorkerWakeGate
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.internal import (
    InternalRequestContext,
    McpRegistrySnapshotRequest,
    McpRegistrySnapshotResponse,
    ServiceIdentity,
)
from auraclaw.contracts.mcp_registry import McpActiveSnapshotEntry
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
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.infrastructure.artifacts.seaweedfs import (
    SeaweedFSMultipartClient,
    SeaweedFSS3Presigner,
)
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.clients.artifact import RemoteArtifactWriter
from auraclaw.infrastructure.clients.credential import RemoteCredentialProxy
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.infrastructure.clients.policy import (
    RemotePolicyClient,
    RemoteTaskAdmissionController,
)
from auraclaw.infrastructure.clients.runtime import (
    RemoteOrchestratorSessionClient,
    RemoteRuntimeControlClient,
    RemoteRuntimeSessionClient,
)
from auraclaw.infrastructure.clients.session import (
    NoOpOutboxRelay,
    RemoteSessionDeliveryOutboxSource,
    RemoteSessionEventStore,
    RemoteSessionOutboxSource,
    RemoteTaskProjection,
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
from auraclaw.infrastructure.observability.stores import InMemoryObservabilityStore
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
    control_routes,
    credential_routes,
    mcp_registry_routes,
    model_routes,
    model_stream_routes,
    policy_routes,
    session_routes,
)
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
    LeaseAssertionSigner,
    LeaseAssertionVerifier,
)
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.observability.service import ObservabilityService
from auraclaw.policy.internal_service import PolicyInternalService
from auraclaw.projection.approval.projector import InMemoryApprovalProjection
from auraclaw.projection.collaboration.projector import InMemoryCollaborationProjection
from auraclaw.projection.ports import (
    ApprovalViewReader,
    CollaborationReader,
    TaskReader,
)
from auraclaw.projection.relay import OutboxRelay
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.harness import AgentHarness
from auraclaw.session.internal_service import SessionInternalService
from auraclaw.session.task_service import TaskService

logger = logging.getLogger(__name__)


def _mcp_registry_service(settings: Settings) -> tuple[
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
        McpServerRegistryService(
            store, allow_private_auth_none=allow_private_none
        ),
        store,
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
    return tuple(
        McpActiveSnapshotEntry.model_validate(item) for item in response.servers
    )


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
        assignments = await self._control.claim(limit=1)
        for assignment in assignments:
            task_id = (
                f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
            )
            try:
                await self._execute_with_heartbeat(assignment)
            except Exception as exc:
                try:
                    await self._harness.record_failure(assignment, exc)
                except Exception:
                    # Keep assignment running so reclaim/lease recovery can retry.
                    # Finishing here without a terminal Session event leaves the
                    # Session stuck in runnable forever.
                    logger.exception(
                        "failed to record run failure for %s; leaving assignment running",
                        task_id,
                    )
                    raise
                try:
                    await self._control.finish_assignment(task_id, "failed")
                except Exception:
                    logger.exception(
                        "failed to disposition assignment %s after recorded failure",
                        task_id,
                    )
                raise
        return len(assignments)

    async def _execute_with_heartbeat(self, assignment: RuntimeAssignment) -> None:
        stop = asyncio.Event()

        async def keep_alive() -> None:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=self._heartbeat_interval.total_seconds(),
                    )
                    return
                except TimeoutError:
                    try:
                        await self._control.heartbeat()
                        self._last_heartbeat_at = time.monotonic()
                    except Exception:
                        # Transient control-plane blips must not stop keep-alive;
                        # exiting here leaves last_heartbeat_at stale and
                        # recover_expired can reclaim a still-running long call.
                        logger.warning(
                            "runtime heartbeat failed during execute "
                            "for session=%s run=%s; will retry",
                            assignment.session_id,
                            assignment.run_id,
                            exc_info=True,
                        )

        heartbeats = asyncio.create_task(keep_alive(), name="remote-runtime-heartbeat")
        try:
            await self._harness.execute(assignment)
        finally:
            stop.set()
            await heartbeats


def _runtime_instance_identity(settings: Settings) -> tuple[str, str]:
    return settings.runtime_id, settings.runtime_node_id


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


def _has_workload_tokens(
    settings: Settings, identities: tuple[ServiceIdentity, ...]
) -> bool:
    return all(settings.workload_token_value(identity.value) for identity in identities)


def _lease_key_configured(settings: Settings) -> bool:
    return (
        settings.lease_signing_key is not None
        and len(settings.lease_signing_key.get_secret_value()) >= 32
    )


def _seed_managed_connector_credentials(
    proxy: CredentialProxy,
    settings: Settings,
) -> None:
    expires_at = datetime.now(UTC) + timedelta(days=365)
    debug_tenants = (
        ("local", "development", "1")
        if settings.deployment_profile == "development"
        else ()
    )
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
            proxy.register_reference(tenant_id, reference)


def _task_api_app(settings: Settings) -> FastAPI:
    app = create_app(profile="task-api")
    token = settings.workload_token_value(ServiceIdentity.TASK_API.value)
    config_ready = bool(
        token
        and (settings.sql_storage_enabled or settings.storage_backend == "memory")
        and (
            settings.signed_identity_configured
            or settings.insecure_identity_headers_enabled
        )
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
    task_projection: TaskReader
    approval_projection: ApprovalViewReader
    collaboration_projection: CollaborationReader
    if settings.sql_storage_enabled:
        task_projection = PostgresTaskProjection(settings.resolved_database_url)
        approval_projection = PostgresApprovalProjection(settings.resolved_database_url)
        collaboration_projection = PostgresCollaborationProjection(
            settings.resolved_database_url
        )
    else:
        # Session owns the memory store in the multi-process debug topology.
        task_projection = RemoteTaskProjection(remote_session)
        approval_projection = InMemoryApprovalProjection()
        collaboration_projection = InMemoryCollaborationProjection()
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
    observability = ObservabilityService(InMemoryObservabilityStore(), remote_session)
    app.dependency_overrides[get_task_command_gateway] = lambda: gateway
    app.dependency_overrides[get_task_projection] = lambda: task_projection
    app.dependency_overrides[get_task_query_service] = lambda: query
    app.dependency_overrides[get_collaboration_projection] = lambda: (
        collaboration_projection
    )
    app.dependency_overrides[get_observability_service] = lambda: observability
    app.state.observability_service = observability
    identity_closeables = (
        (app.state.identity_verifier,)
        if hasattr(app.state.identity_verifier, "close")
        else ()
    )
    app.state.closeables = (
        *identity_closeables,
        remote_session,
        policy,
        *(
            (task_projection, approval_projection, collaboration_projection)
            if settings.sql_storage_enabled
            else ()
        ),
    )
    mcp_registry, mcp_store = _mcp_registry_service(settings)
    app.include_router(create_mcp_admin_router(mcp_registry))
    if isinstance(mcp_store, PostgresMcpServerRegistryStore):
        app.state.closeables = (*app.state.closeables, mcp_store)
    app.state.config_ready = config_ready
    app.state.storage_label = "projection-read-only"
    app.state.session_access = "http"
    return app


def _streaming_app(settings: Settings) -> FastAPI:
    app = create_app(profile="streaming-gateway")
    token = settings.workload_token_value(ServiceIdentity.STREAMING_GATEWAY.value)
    remote_session: RemoteSessionEventStore | None = None
    projection: TaskReader
    if settings.sql_storage_enabled:
        projection = PostgresTaskProjection(settings.resolved_database_url)
    else:
        # Session owns the memory store in the multi-process debug topology.
        remote_session = RemoteSessionEventStore(
            settings.session_base_url,
            service_identity=ServiceIdentity.STREAMING_GATEWAY,
            bearer_token=token or secrets.token_urlsafe(32),
        )
        projection = RemoteTaskProjection(remote_session)
    gateway = StreamingGateway(
        reader=projection,
        bus=providers.get_runtime_replay_bus(),
    )
    app.dependency_overrides[get_streaming_gateway] = lambda: gateway
    app.state.closeables = (
        *((projection,) if settings.sql_storage_enabled else ()),
        *((remote_session,) if remote_session is not None else ()),
    )
    app.state.storage_label = "projection-read-only"
    app.state.session_access = "http" if remote_session is not None else "database"
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
        database_ready = (
            settings.sql_storage_enabled or settings.storage_backend == "memory"
        )
        dependencies["postgres"] = (
            "ready"
            if settings.sql_storage_enabled
            else "memory"
            if settings.storage_backend == "memory"
            else "missing"
        )
        ready = ready and database_ready
    if name == "task-api":
        identity_ready = (
            settings.insecure_identity_headers_enabled
            or settings.signed_identity_configured
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
        dependencies["model_provider"] = (
            "ready" if settings.model_gateway_configured else "missing"
        )
        ready = ready and settings.model_gateway_configured
        identity_ready = bool(
            settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value)
        )
        dependencies["runtime_workload_identity"] = (
            "ready" if identity_ready else "missing"
        )
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
        dependencies["control_workload_identities"] = (
            "ready" if identity_ready else "missing"
        )
        ready = ready and lease_ready and identity_ready
    if name == "artifact-service":
        storage_ready = (
            settings.seaweedfs_enabled or settings.artifact_backend == "local"
        )
        dependencies["object_storage"] = (
            "seaweedfs"
            if settings.seaweedfs_enabled
            else "local"
            if settings.artifact_backend == "local"
            else "missing"
        )
        policy_identity_ready = bool(
            settings.workload_token_value(ServiceIdentity.ARTIFACT_SERVICE.value)
        )
        dependencies["policy_workload_identity"] = (
            "ready" if policy_identity_ready else "missing"
        )
        ready = ready and storage_ready and policy_identity_ready
    if name == "projection-worker":
        token_ready = bool(
            settings.workload_token_value(ServiceIdentity.PROJECTION_WORKER.value)
        )
        dependencies["session_workload_identity"] = (
            "ready" if token_ready else "missing"
        )
        ready = ready and token_ready
    if name == "agent-runtime":
        token_ready = bool(
            settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value)
        )
        provider_secret_absent = settings.model_api_key is None
        dependencies["workload_identity"] = "ready" if token_ready else "missing"
        dependencies["provider_secret_isolation"] = (
            "ready" if provider_secret_absent else "forbidden"
        )
        ready = ready and token_ready and provider_secret_absent
    if name == "action-hands":
        identity_ready = (
            settings.runtime_workload_token is not None
            and bool(settings.runtime_workload_token.get_secret_value())
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
        ready = ready and identity_ready and lease_ready and downstream_identity_ready
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
        dependencies["enforcement_identities"] = (
            "ready" if identity_ready else "missing"
        )
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
        vault_ready = vault_configured or not settings.credential_vault_addr
        dependencies["caller_identities"] = "ready" if identity_ready else "missing"
        dependencies["vault"] = "ready" if vault_configured else (
            "memory" if vault_ready else "missing"
        )
        policy_identity_ready = bool(
            settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
        )
        dependencies["policy_workload_identity"] = (
            "ready" if policy_identity_ready else "missing"
        )
        ready = ready and identity_ready and vault_ready and policy_identity_ready
    if name == "delivery-worker":
        identity_ready = bool(
            settings.workload_token_value(ServiceIdentity.DELIVERY_WORKER.value)
        )
        dependencies["delivery_workload_identity"] = (
            "ready" if identity_ready else "missing"
        )
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
        worker_task = asyncio.create_task(
            worker_loop(), name=f"{app.state.service_name}-worker"
        )
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
    app = _base_service_app(spec, settings, closeables=closeables)
    key = _lease_signing_key(settings)
    verifier = LeaseAssertionVerifier(
        {"development": key},
        ledger=InMemoryFencingTokenLedger(),
        audience=("session", "runtime"),
    )
    service = SessionInternalService(
        providers.get_event_store(),
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
        ),
    )
    contract_app = create_contract_app(
        "session",
        session_routes(service),
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
    claim_wait = (
        settings.worker_idle_interval if settings.worker_wake_enabled else 0.0
    )
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
            (lambda: runtime_wake_client.wake())
            if runtime_wake_client is not None
            else None
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
        worker_interval=_worker_post_tick_wait(
            settings, settings.orchestrator_worker_interval
        ),
        closeables=closeables,
    )
    service = ControlInternalService(
        store,
        lease_verifier=LeaseAssertionVerifier(
            {"development": key},
            ledger=InMemoryFencingTokenLedger(),
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
    closeables: tuple[Any, ...] = ()
    policy: RemotePolicyClient
    credential_proxy: RemoteCredentialProxy | None = None
    artifacts: RemoteArtifactWriter
    invocation_store: PostgresInvocationStore | None = None
    tool_registry_store: PostgresToolRegistryStore | None = None
    capability_catalog_store: (
        InMemoryCapabilityCatalogStore | PostgresCapabilityCatalogStore
    ) = InMemoryCapabilityCatalogStore()
    mcp_registry, mcp_registry_store = _mcp_registry_service(settings)
    remote_clients: list[Any] = []
    hands_token = _service_bearer_token(settings, ServiceIdentity.ACTION_HANDS)
    policy = RemotePolicyClient(settings.policy_base_url, bearer_token=hands_token)
    credential_proxy = RemoteCredentialProxy(
        settings.credential_proxy_base_url, bearer_token=hands_token
    )
    remote_clients.extend((policy, credential_proxy))
    artifacts = RemoteArtifactWriter(
        settings.artifact_base_url,
        bearer_token=_service_bearer_token(settings, ServiceIdentity.ACTION_HANDS),
    )
    if settings.sql_storage_enabled:
        invocation_store = PostgresInvocationStore(settings.resolved_database_url)
        tool_registry_store = PostgresToolRegistryStore(settings.resolved_database_url)
        capability_catalog_store = PostgresCapabilityCatalogStore(
            settings.resolved_database_url
        )
    closeables = (
        *remote_clients,
        artifacts,
        *((invocation_store,) if invocation_store is not None else ()),
        *((tool_registry_store,) if tool_registry_store is not None else ()),
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
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
    )
    capability_catalog = CapabilityCatalog(capability_catalog_store)
    resources = HandsResourceRegistry()
    resource_gateway = ManagedResourceGateway(
        resources,
        artifacts=artifacts,
        policy=policy if isinstance(policy, RemotePolicyClient) else None,
    )
    configured_signing_key = (
        settings.skill_signing_key.get_secret_value().encode()
        if settings.skill_signing_key is not None
        else None
    )
    skill_signer = HmacSkillSignatureVerifier(
        {
            "ct-model": (
                configured_signing_key or b"auraclaw-development-model-skill-key"
            ),
            "platform": (
                configured_signing_key or b"auraclaw-development-platform-skill-key"
            ),
        }
    )
    skill_artifacts: ArtifactStore | RemoteArtifactWriter = artifacts
    if settings.deployment_profile == "development":
        skill_artifacts = ArtifactStore(
            InMemoryObjectStorage(),
            signing_key=(
                configured_signing_key or b"auraclaw-development-platform-skill-key"
            ),
        )
    skill_registry = SkillPackageRegistry(
        artifacts=skill_artifacts,
        signature_verifier=skill_signer,
        resources=resources,
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
            )
            mcp_registry.bind_runtime(manager)
            app.state.mcp_connection_manager = manager
            await manager.restore()
        for java_server in settings.java_api_servers:
            catalog_server = catalog_server_definition(java_server)
            await capability_catalog.register_server(catalog_server)
            if credential_proxy is not None and isinstance(policy, RemotePolicyClient):
                app.state.capability_connectors[java_server.server_id] = (
                    ManagedJavaApiConnector(
                        java_server,
                        credentials=credential_proxy,
                        policy=policy,
                    )
                )

    app.state.capability_connectors = {}
    app.state.catalog_reconciler = None
    app.state.initialize = initialize_registry
    routed_hands = RoutedHandsExecutor(
        LocalHandsService(workspace_root=Path.cwd()),
        {
            CAPABILITY_SEARCH_TOOL_NAME: CapabilitySearchExecutor(
                capability_catalog,
                skills=skill_registry,
            ),
            CAPABILITY_LOAD_TOOL_NAME: CapabilityLoadExecutor(
                capability_catalog,
                skills=skill_registry,
            ),
            SKILL_RESOLVE_TOOL_NAME: SkillResolveExecutor(skill_resolver),
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
    )
    token = (
        _agent_runtime_token(settings) or ""
    )
    key = _lease_signing_key(settings)
    authenticator: HandsWorkloadAuthenticator = SignedLeaseHandsAuthenticator(
        {token: "*"} if token else {},
        verifier=LeaseAssertionVerifier(
            {"development": key},
            ledger=InMemoryFencingTokenLedger(),
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
    periodic_jobs: list[tuple[str, float, Callable[[], Awaitable[int | None]]]] = []
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
            registry=skill_registry,
        )
        app.state.catalog_reconciler = reconciler
        app.state.skill_reconciler = skill_reconciler

        async def initialize_remote_catalog() -> None:
            await initialize_registry()
            for connector in app.state.capability_connectors.values():
                setter = getattr(connector, "set_notification_handler", None)
                if setter is not None:
                    setter(reconciler.handle_notification)
            expected = len(app.state.capability_connectors)
            for attempt in range(20):
                active = await reconciler.reconcile_all()
                await skill_reconciler.reconcile_all()
                if expected == 0 or active >= expected:
                    break
                if attempt < 19:
                    await asyncio.sleep(0.5)

        async def reconcile_catalog_and_skills() -> int:
            active = await reconciler.reconcile_all()
            await skill_reconciler.reconcile_all()
            return active

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
            next_due.update(
                {name: now + interval for name, interval, _run in periodic_jobs}
            )

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
        app.state.worker_interval = min(
            interval for _name, interval, _run in periodic_jobs
        )
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
            workload_identities=mcp_identities or None,
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
        readiness_probe=(
            vault.readiness if isinstance(vault, HashiCorpVault) else None
        ),
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
    )
    _seed_managed_connector_credentials(proxy, settings)

    async def restore_mcp_egress() -> None:
        snapshot = await _hands_mcp_snapshot(settings)
        if snapshot is None:
            return
        await mcp_egress.restore(snapshot)

    async def reconcile_mcp_egress() -> int:
        snapshot = await _hands_mcp_snapshot(settings)
        if snapshot is None:
            return 0
        return await mcp_egress.reconcile(snapshot)

    app.state.initialize = restore_mcp_egress
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
    access_key = (
        settings.seaweedfs_access_key.get_secret_value()
        if settings.seaweedfs_access_key is not None
        else "development-access-key"
    )
    secret_key = (
        settings.seaweedfs_secret_key.get_secret_value()
        if settings.seaweedfs_secret_key is not None
        else "development-secret-key"
    )
    presigner = SeaweedFSS3Presigner(
        settings.seaweedfs_s3_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=settings.seaweedfs_bucket,
        region=settings.seaweedfs_region,
        path_style=settings.seaweedfs_path_style,
    )
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
    verifier = SeaweedFSObjectVerifier(presigner) if settings.seaweedfs_enabled else None
    multipart = SeaweedFSMultipartClient(presigner) if settings.seaweedfs_enabled else None
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
        *((verifier,) if verifier is not None else ()),
        *((multipart,) if multipart is not None else ()),
        *((policy,) if policy is not None else ()),
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
        readiness_probe=verifier.readiness if verifier is not None else None,
    )
    service = ArtifactInternalService(
        presigner,
        repository=repository,
        object_verifier=verifier,
        policy=policy,
        multipart=multipart,
        multipart_threshold=settings.artifact_multipart_threshold,
        multipart_part_size=settings.artifact_multipart_part_size,
    )

    async def artifact_status(parameters: dict[str, Any]) -> dict[str, Any]:
        del parameters
        return {"storage": "seaweedfs", "metadata_owner": "artifact-service"}

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
    claim_wait = (
        settings.worker_idle_interval if settings.worker_wake_enabled else 0.0
    )
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
        jobs = (
            await store.list_jobs(tenant_id, session_id)
            if tenant_id and session_id
            else []
        )
        return {"jobs": len(jobs)}

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
        workload_identities=_configured_identities(
            settings, (ServiceIdentity.TASK_API,)
        ),
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
    projector = providers.get_task_projection()
    admin_store = PostgresAdminOperationStore(
        settings.resolved_database_url, schema="projection"
    )
    closeables: tuple[Any, ...] = ()
    token = settings.workload_token_value(ServiceIdentity.PROJECTION_WORKER.value)
    remote_session = RemoteSessionEventStore(
        settings.session_base_url,
        service_identity=ServiceIdentity.PROJECTION_WORKER,
        bearer_token=token or secrets.token_urlsafe(32),
        timeout=max(10.0, settings.worker_idle_interval + 5.0),
    )
    claim_wait = (
        settings.worker_idle_interval if settings.worker_wake_enabled else 0.0
    )
    source = RemoteSessionOutboxSource(
        remote_session,
        worker_id="projection-worker",
        wait_seconds=claim_wait,
    )
    relay = OutboxRelay(source, projector)
    closeables = (remote_session, projector, admin_store)
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
            await projector.poison_count(str(tenant_id) if tenant_id else None)
            if isinstance(projector, PostgresTaskProjection)
            else 0
        )
        return {"poison_count": count}

    async def redrive(parameters: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(projector, PostgresTaskProjection):
            return {"changed": False}
        changed = await projector.redrive_poison(
            str(parameters["tenant_id"]), str(parameters["event_id"])
        )
        return {"changed": changed}

    async def rebuild(parameters: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(projector, PostgresTaskProjection) or remote_session is None:
            return {"processed": 0}
        tenant = parameters.get("tenant_id")
        tenant_id = str(tenant) if tenant else None
        events = []
        for event_tenant, session_id in await projector.session_keys(tenant_id):
            events.extend(await remote_session.load(event_tenant, session_id))
        processed = await projector.rebuild(events, tenant_id)
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
        workload_identities=_configured_identities(
            settings, (ServiceIdentity.TASK_API,)
        ),
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
        workload_identities=_configured_identities(
            settings, (ServiceIdentity.AGENT_RUNTIME,)
        ),
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
    )
    worker = RemoteRuntimeWorker(control, harness)
    app = _base_service_app(
        spec,
        settings,
        tick=worker.tick,
        worker_interval=settings.runtime_poll_interval,
        closeables=(control, session, model, hands_http),
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
