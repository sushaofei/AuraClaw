from __future__ import annotations

import asyncio
import secrets
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from auraclaw import __version__
from auraclaw.action.capability_catalog import (
    CAPABILITY_SEARCH_TOOL_NAME,
    CapabilityCatalog,
    CapabilitySearchExecutor,
    InMemoryCapabilityCatalogStore,
    RoutedHandsExecutor,
    capability_search_tool,
)
from auraclaw.action.catalog_reconciler import McpCatalogReconciler
from auraclaw.action.mcp import HandsMcpServer
from auraclaw.action.mcp_http import (
    SignedLeaseWorkloadAuthenticator,
    StaticWorkloadAuthenticator,
    WorkloadAuthenticator,
    create_hands_mcp_app,
)
from auraclaw.action.mcp_primitives import McpResourceRegistry
from auraclaw.action.model_skill_compiler import (
    ModelSkillCompiler,
    ModelSkillPublisher,
)
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.remote_mcp import ManagedRemoteMcpTransport
from auraclaw.action.resource_gateway import ManagedResourceGateway
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackageRegistry,
)
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.api.dependencies import (
    get_collaboration_projection,
    get_observability_service,
    get_task_command_gateway,
    get_task_projection,
    get_task_query_service,
)
from auraclaw.artifact.internal_service import (
    ArtifactInternalService,
    SeaweedFSObjectVerifier,
)
from auraclaw.composition import providers
from auraclaw.composition.api import create_app
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.contracts.mcp import McpTrustedContext
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.control.orchestrator import ManagedOrchestrator, RegisteredRuntimeProvisioner
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.credential_proxy.internal_service import CredentialProxyInternalService
from auraclaw.delivery.worker import ResultDeliveryWorker
from auraclaw.gateways.query.reader import TaskQueryService
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
)
from auraclaw.infrastructure.credentials.mcp_egress import ManagedMcpEgressAdapter
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
from auraclaw.infrastructure.model_sources.mysql import MySqlModelSkillSource
from auraclaw.infrastructure.observability.stores import InMemoryObservabilityStore
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.postgres_admin_store import (
    PostgresAdminOperationStore,
)
from auraclaw.infrastructure.persistence.postgres_artifact_repository import (
    PostgresArtifactRepository,
)
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_control_store import PostgresControlStateStore
from auraclaw.infrastructure.persistence.postgres_credential_registry import (
    PostgresCredentialRegistry,
)
from auraclaw.infrastructure.persistence.postgres_invocation_store import (
    PostgresInvocationStore,
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
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import (
    admin_routes,
    artifact_routes,
    control_routes,
    credential_routes,
    model_routes,
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
from auraclaw.projection.ports import ApprovalViewReader, CollaborationReader, TaskReader
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.mcp_client import HandsMcpClient, HttpMcpTransport
from auraclaw.session.internal_service import SessionInternalService
from auraclaw.session.task_service import TaskService

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


class RemoteRuntimeWorker:
    def __init__(
        self,
        control: RemoteRuntimeControlClient,
        harness: AgentHarness,
    ) -> None:
        self._control = control
        self._harness = harness
        self._registered = False

    async def tick(self) -> int:
        if not self._registered:
            await self._control.register()
            self._registered = True
        else:
            await self._control.heartbeat()
        assignments = await self._control.claim(limit=1)
        for assignment in assignments:
            try:
                await self._harness.execute(assignment)
            except Exception as exc:
                task_id = (
                    f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
                )
                await self._harness.record_failure(assignment, exc)
                await self._control.finish_assignment(task_id, "failed")
                raise
        return len(assignments)


def _runtime_instance_identity(settings: Settings) -> tuple[str, str]:
    """Give every production replica a stable pod/host-scoped identity by default."""
    if settings.deployment_profile != "production":
        return settings.runtime_id, settings.runtime_node_id
    hostname = socket.gethostname()
    runtime_id = (
        f"runtime-{hostname}" if settings.runtime_id == "runtime-local-1" else settings.runtime_id
    )
    node_id = hostname if settings.runtime_node_id == "local" else settings.runtime_node_id
    return runtime_id, node_id


def _configured_identities(
    settings: Settings, identities: tuple[ServiceIdentity, ...]
) -> dict[str, ServiceIdentity]:
    if settings.deployment_profile == "development":
        return {
            f"development-{identity.value}": identity
            for identity in identities
        }
    configured: dict[str, ServiceIdentity] = {}
    for identity in identities:
        token = settings.workload_token_value(identity.value)
        if token:
            configured[token] = identity
    return configured


def _task_api_app(settings: Settings) -> FastAPI:
    app = create_app(profile="task-api")
    if settings.deployment_profile == "development":
        return app
    token = settings.workload_token_value(ServiceIdentity.TASK_API.value)
    config_ready = bool(token and settings.postgres_enabled)
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
    if settings.postgres_enabled:
        task_projection = PostgresTaskProjection(settings.resolved_database_url)
        approval_projection = PostgresApprovalProjection(settings.resolved_database_url)
        collaboration_projection = PostgresCollaborationProjection(
            settings.resolved_database_url
        )
    else:
        task_projection = InMemoryTaskProjection()
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
    app.dependency_overrides[get_collaboration_projection] = (
        lambda: collaboration_projection
    )
    app.dependency_overrides[get_observability_service] = lambda: observability
    app.state.observability_service = observability
    app.state.closeables = (
        remote_session,
        policy,
        *(
            (task_projection, approval_projection, collaboration_projection)
            if settings.postgres_enabled
            else ()
        ),
    )
    app.state.config_ready = config_ready
    app.state.storage_label = "projection-read-only"
    app.state.session_access = "http"
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
        database_ready = settings.postgres_enabled or settings.deployment_profile == "development"
        dependencies["postgres"] = (
            "ready"
            if settings.postgres_enabled
            else "development-memory"
            if settings.deployment_profile == "development"
            else "missing"
        )
        ready = ready and database_ready
    if name == "model-gateway":
        dependencies["model_provider"] = (
            "ready" if settings.model_gateway_configured else "missing"
        )
        ready = ready and settings.model_gateway_configured
        identity_ready = (
            settings.deployment_profile == "development"
            or bool(
                settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value)
            )
        )
        dependencies["runtime_workload_identity"] = (
            "ready" if identity_ready else "missing"
        )
        ready = ready and identity_ready
    if name == "session":
        lease_ready = (
            settings.lease_signing_key is not None
            and len(settings.lease_signing_key.get_secret_value()) >= 32
            or settings.deployment_profile == "development"
        )
        dependencies["lease_signing_key"] = "ready" if lease_ready else "missing"
        ready = ready and lease_ready
        required_identities = (
            ServiceIdentity.TASK_API,
            ServiceIdentity.PROJECTION_WORKER,
            ServiceIdentity.ORCHESTRATOR,
            ServiceIdentity.AGENT_RUNTIME,
            ServiceIdentity.POLICY,
            ServiceIdentity.DELIVERY_WORKER,
        )
        identity_ready = (
            settings.deployment_profile == "development"
            or all(
                settings.workload_token_value(identity.value)
                for identity in required_identities
            )
        )
        dependencies["workload_identities"] = (
            "ready" if identity_ready else "missing"
        )
        ready = ready and identity_ready
    if name == "orchestrator":
        lease_ready = (
            settings.deployment_profile == "development"
            or settings.lease_signing_key is not None
            and len(settings.lease_signing_key.get_secret_value()) >= 32
        )
        identity_ready = (
            settings.deployment_profile == "development"
            or bool(settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value))
            and bool(settings.workload_token_value(ServiceIdentity.TASK_API.value))
        )
        dependencies["lease_signer"] = "ready" if lease_ready else "missing"
        dependencies["control_workload_identities"] = (
            "ready" if identity_ready else "missing"
        )
        ready = ready and lease_ready and identity_ready
    if name == "artifact-service":
        storage_ready = settings.seaweedfs_enabled or settings.deployment_profile == "development"
        dependencies["seaweedfs"] = "ready" if storage_ready else "missing"
        policy_identity_ready = (
            settings.deployment_profile == "development"
            or bool(
                settings.workload_token_value(ServiceIdentity.ARTIFACT_SERVICE.value)
            )
        )
        dependencies["policy_workload_identity"] = (
            "ready" if policy_identity_ready else "missing"
        )
        ready = ready and storage_ready and policy_identity_ready
    if name == "projection-worker":
        token_ready = (
            settings.deployment_profile == "development"
            or bool(
                settings.workload_token_value(
                    ServiceIdentity.PROJECTION_WORKER.value
                )
            )
        )
        dependencies["session_workload_identity"] = (
            "ready" if token_ready else "missing"
        )
        ready = ready and token_ready
    if name == "agent-runtime":
        token_ready = (
            settings.deployment_profile == "development"
            or bool(
                settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value)
            )
        )
        provider_secret_absent = (
            settings.deployment_profile == "development"
            or settings.model_api_key is None
        )
        dependencies["workload_identity"] = (
            "ready" if token_ready else "missing"
        )
        dependencies["provider_secret_isolation"] = (
            "ready" if provider_secret_absent else "forbidden"
        )
        ready = ready and token_ready and provider_secret_absent
    if name == "action-hands":
        identity_ready = (
            settings.runtime_workload_token is not None
            and bool(settings.runtime_workload_token.get_secret_value())
            or settings.deployment_profile == "development"
        )
        dependencies["workload_identity"] = "ready" if identity_ready else "missing"
        lease_ready = (
            settings.deployment_profile == "development"
            or settings.lease_signing_key is not None
            and len(settings.lease_signing_key.get_secret_value()) >= 32
        )
        dependencies["lease_verifier"] = "ready" if lease_ready else "missing"
        downstream_identity_ready = (
            settings.deployment_profile == "development"
            or bool(
                settings.workload_token_value(ServiceIdentity.ACTION_HANDS.value)
            )
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
        identity_ready = (
            settings.deployment_profile == "development"
            or all(settings.workload_token_value(item.value) for item in identities)
        )
        dependencies["enforcement_identities"] = (
            "ready" if identity_ready else "missing"
        )
        ready = ready and identity_ready
    if name == "credential-proxy":
        identity_ready = (
            settings.deployment_profile == "development"
            or all(
                settings.workload_token_value(item.value)
                for item in (
                    ServiceIdentity.TASK_API,
                    ServiceIdentity.ACTION_HANDS,
                    ServiceIdentity.DELIVERY_WORKER,
                )
            )
        )
        vault_ready = (
            settings.deployment_profile == "development"
            or bool(settings.credential_vault_addr)
            and settings.credential_vault_token is not None
        )
        dependencies["caller_identities"] = (
            "ready" if identity_ready else "missing"
        )
        dependencies["vault"] = "ready" if vault_ready else "missing"
        policy_identity_ready = (
            settings.deployment_profile == "development"
            or bool(
                settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
            )
        )
        dependencies["policy_workload_identity"] = (
            "ready" if policy_identity_ready else "missing"
        )
        ready = ready and identity_ready and vault_ready and policy_identity_ready
    if name == "delivery-worker":
        identity_ready = (
            settings.deployment_profile == "development"
            or bool(settings.workload_token_value(ServiceIdentity.DELIVERY_WORKER.value))
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
    initialize = getattr(app.state, "initialize", None)
    if initialize is not None:
        await initialize()

    async def worker_loop() -> None:
        while not stop.is_set():
            tick = getattr(app.state, "tick", None)
            if tick is not None:
                try:
                    await tick()
                    app.state.worker_error = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    app.state.worker_error = type(exc).__name__
            app.state.worker_iterations += 1
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

    return app


def _development_lease_key(settings: Settings) -> bytes:
    if settings.lease_signing_key is not None:
        value = settings.lease_signing_key.get_secret_value().encode()
        if len(value) >= 32:
            return value
    if settings.deployment_profile == "development":
        return b"auraclaw-development-lease-key-0001"
    return secrets.token_bytes(32)


def _session_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    app = _base_service_app(spec, settings)
    key = _development_lease_key(settings)
    verifier = LeaseAssertionVerifier(
        {"development": key},
        ledger=InMemoryFencingTokenLedger(),
        audience=("session", "runtime"),
    )
    service = SessionInternalService(providers.get_event_store(), lease_verifier=verifier)
    identities = _configured_identities(
        settings,
        (
            ServiceIdentity.TASK_API,
            ServiceIdentity.PROJECTION_WORKER,
            ServiceIdentity.ORCHESTRATOR,
            ServiceIdentity.AGENT_RUNTIME,
            ServiceIdentity.POLICY,
            ServiceIdentity.DELIVERY_WORKER,
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
        if settings.postgres_enabled
        else InMemoryControlStateStore()
    )
    key = _development_lease_key(settings)
    closeables: tuple[Any, ...] = (store,) if settings.postgres_enabled else ()
    tick: Callable[[], Awaitable[int | None]] = store.recover_expired
    if settings.deployment_profile == "production":
        token = settings.workload_token_value(ServiceIdentity.ORCHESTRATOR.value)
        bearer_token = token or secrets.token_urlsafe(32)
        feed_session = RemoteSessionEventStore(
            settings.session_base_url,
            service_identity=ServiceIdentity.ORCHESTRATOR,
            bearer_token=bearer_token,
        )
        lifecycle_session = RemoteOrchestratorSessionClient(
            settings.session_base_url,
            bearer_token=bearer_token,
        )
        worker_id = f"orchestrator-{secrets.token_hex(8)}"
        feed = RunnableFeedConsumer(feed_session, store, worker_id=worker_id)
        orchestrator = ManagedOrchestrator(
            orchestrator_id=worker_id,
            control_store=store,
            session=lifecycle_session,
            provisioner=RegisteredRuntimeProvisioner(store),
            lease_ttl=timedelta(seconds=settings.orchestrator_lease_ttl_seconds),
        )

        async def production_tick() -> int:
            ingested = await feed.run_once()
            recovered = await orchestrator.recover()
            scheduled = await orchestrator.schedule_once()
            return ingested + recovered + int(scheduled is not None)

        tick = production_tick
        closeables += (feed_session, lifecycle_session)
    app = _base_service_app(
        spec,
        settings,
        tick=tick,
        closeables=closeables,
    )
    service = ControlInternalService(
        store,
        lease_verifier=LeaseAssertionVerifier(
            {"development": key},
            ledger=InMemoryFencingTokenLedger(),
            audience=("control", "runtime"),
        ),
        lease_signer=LeaseAssertionSigner(
            key_id="development", signing_key=key
        ),
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
    policy: PolicyEngine | RemotePolicyClient
    credential_proxy: RemoteCredentialProxy | None = None
    artifacts: ArtifactStore | RemoteArtifactWriter
    invocation_store: PostgresInvocationStore | None = None
    tool_registry_store: PostgresToolRegistryStore | None = None
    capability_catalog_store: (
        InMemoryCapabilityCatalogStore | PostgresCapabilityCatalogStore
    ) = InMemoryCapabilityCatalogStore()
    if settings.deployment_profile == "production":
        hands_token = settings.workload_token_value(
            ServiceIdentity.ACTION_HANDS.value
        ) or secrets.token_urlsafe(32)
        policy = RemotePolicyClient(
            settings.policy_base_url, bearer_token=hands_token
        )
        credential_proxy = RemoteCredentialProxy(
            settings.credential_proxy_base_url, bearer_token=hands_token
        )
        artifacts = RemoteArtifactWriter(
            settings.artifact_base_url, bearer_token=hands_token
        )
        if settings.postgres_enabled:
            invocation_store = PostgresInvocationStore(settings.resolved_database_url)
            tool_registry_store = PostgresToolRegistryStore(
                settings.resolved_database_url
            )
            capability_catalog_store = PostgresCapabilityCatalogStore(
                settings.resolved_database_url
            )
        closeables = (
            policy,
            credential_proxy,
            artifacts,
            *((invocation_store,) if invocation_store is not None else ()),
            *((tool_registry_store,) if tool_registry_store is not None else ()),
            *(
                (capability_catalog_store,)
                if isinstance(
                    capability_catalog_store, PostgresCapabilityCatalogStore
                )
                else ()
            ),
        )
    else:
        policy = PolicyEngine(version="s3-v1")
        artifacts = ArtifactStore(
            InMemoryObjectStorage(), signing_key=b"auraclaw-s3-artifact-key"
        )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
    )
    capability_catalog = CapabilityCatalog(capability_catalog_store)
    resources = McpResourceRegistry()
    resource_gateway = ManagedResourceGateway(
        resources,
        artifacts=artifacts,
        policy=policy if isinstance(policy, RemotePolicyClient) else None,
    )
    configured_signing_key = (
        settings.model_skill_signing_key.get_secret_value().encode()
        if settings.model_skill_signing_key is not None
        else None
    )
    if (
        settings.deployment_profile == "production"
        and settings.model_skill_source_configured
        and configured_signing_key is None
    ):
        raise ValueError(
            "Production Model Skill source requires AURACLAW_MODEL_SKILL_SIGNING_KEY"
        )
    model_skill_signer = HmacSkillSignatureVerifier(
        {
            "ct-model": (
                configured_signing_key
                or b"auraclaw-development-model-skill-key"
            )
        }
    )
    skill_registry = SkillPackageRegistry(
        artifacts=artifacts,
        signature_verifier=model_skill_signer,
        resources=resources,
    )
    model_skill_publisher: ModelSkillPublisher | None = None
    if settings.model_skill_source_configured:
        mysql_password = settings.model_skill_mysql_password
        if (
            settings.model_skill_mysql_host is None
            or settings.model_skill_mysql_user is None
            or mysql_password is None
            or settings.model_skill_mysql_database is None
        ):
            raise ValueError("Model Skill MySQL source configuration is incomplete")
        model_skill_publisher = ModelSkillPublisher(
            MySqlModelSkillSource(
                host=settings.model_skill_mysql_host,
                port=settings.model_skill_mysql_port,
                user=settings.model_skill_mysql_user,
                password=mysql_password.get_secret_value(),
                database=settings.model_skill_mysql_database,
                tenant_id=settings.model_skill_source_tenant_id,
                include_drafts=settings.model_skill_include_drafts,
            ),
            ModelSkillCompiler(model_skill_signer),
            skill_registry,
            target_tenant_id=settings.model_skill_target_tenant_id,
        )
    registry = ToolRegistry((capability_search_tool(),))

    async def initialize_registry() -> None:
        if tool_registry_store is not None:
            await tool_registry_store.load_into(registry)
        for server in settings.mcp_egress_servers:
            await capability_catalog.register_server(server)
            if credential_proxy is not None and isinstance(policy, RemotePolicyClient):
                app.state.remote_mcp_transports[server.server_id] = (
                    ManagedRemoteMcpTransport(
                        server,
                        credentials=credential_proxy,
                        policy=policy,
                    )
                )
        if model_skill_publisher is not None:
            await model_skill_publisher.reconcile()

    app.state.remote_mcp_transports = {}
    app.state.catalog_reconciler = None
    app.state.initialize = initialize_registry
    routed_hands = RoutedHandsExecutor(
        LocalHandsService(workspace_root=Path.cwd()),
        {
            CAPABILITY_SEARCH_TOOL_NAME: CapabilitySearchExecutor(
                capability_catalog
            )
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
        settings.runtime_workload_token.get_secret_value()
        if settings.runtime_workload_token is not None
        else (
            "development-runtime-token"
            if settings.deployment_profile == "development"
            else secrets.token_urlsafe(32)
        )
    )
    authenticator: WorkloadAuthenticator
    if settings.deployment_profile == "development":
        authenticator = StaticWorkloadAuthenticator(
            {
                token: McpTrustedContext(
                    tenant_id="development",
                    root_session_id="development",
                    session_id="development",
                    run_id="development",
                    runtime_id="development-runtime",
                    lease_id="development-lease",
                    fencing_token=1,
                    deadline=datetime.now(UTC) + timedelta(hours=24),
                )
            }
        )
    else:
        key = _development_lease_key(settings)
        authenticator = SignedLeaseWorkloadAuthenticator(
            {token: "*"},
            verifier=LeaseAssertionVerifier(
                {"development": key},
                ledger=InMemoryFencingTokenLedger(),
                audience="runtime",
            ),
        )
    mcp_app = create_hands_mcp_app(
        HandsMcpServer(
            registry=registry,
            gateway=gateway,
            resources=resources,
            resource_reader=resource_gateway,
        ),
        authenticator=authenticator,
    )
    app.state.capability_catalog = capability_catalog
    app.state.resource_gateway = resource_gateway
    app.state.skill_registry = skill_registry
    app.state.model_skill_publisher = model_skill_publisher
    periodic_jobs: list[
        tuple[str, float, Callable[[], Awaitable[int | None]]]
    ] = []
    if model_skill_publisher is not None:

        async def reconcile_model_skills() -> int:
            return len(await model_skill_publisher.reconcile())

        periodic_jobs.append(
            (
                "model-skills",
                settings.model_skill_reconcile_interval_seconds,
                reconcile_model_skills,
            )
        )
    if credential_proxy is not None and isinstance(policy, RemotePolicyClient):
        reconciler = McpCatalogReconciler(
            catalog=capability_catalog,
            store=capability_catalog_store,
            transports=app.state.remote_mcp_transports,
            resource_cache=resource_gateway,
            tool_registry=registry,
            hands_router=routed_hands,
        )
        app.state.catalog_reconciler = reconciler

        async def initialize_remote_catalog() -> None:
            await initialize_registry()
            for transport in app.state.remote_mcp_transports.values():
                transport.set_notification_handler(
                    reconciler.handle_notification
                )
            await reconciler.reconcile_all()

        app.state.initialize = initialize_remote_catalog
        periodic_jobs.append(
            (
                "mcp-catalog",
                settings.mcp_reconcile_interval_seconds,
                reconciler.reconcile_all,
            )
        )
    if periodic_jobs:
        initialize_periodic_jobs = app.state.initialize
        next_due: dict[str, float] = {}

        async def initialize_and_schedule() -> None:
            await initialize_periodic_jobs()
            now = asyncio.get_running_loop().time()
            next_due.update(
                {
                    name: now + interval
                    for name, interval, _run in periodic_jobs
                }
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
    app.mount("/", mcp_app)
    return app


def _policy_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    store = (
        PostgresPolicyStateStore(settings.resolved_database_url)
        if settings.postgres_enabled
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
        if settings.postgres_enabled
        else None
    )
    vault: InMemoryVault | HashiCorpVault
    if (
        settings.deployment_profile == "production"
        and settings.credential_vault_addr
        and settings.credential_vault_token is not None
    ):
        vault = HashiCorpVault(
            settings.credential_vault_addr,
            token=settings.credential_vault_token.get_secret_value(),
            mount=settings.credential_vault_mount,
        )
    else:
        vault = InMemoryVault({})
    policy: RemotePolicyClient | None = None
    if settings.deployment_profile == "production":
        token = settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
        policy = RemotePolicyClient(
            settings.policy_base_url,
            bearer_token=token or secrets.token_urlsafe(32),
            service_identity=ServiceIdentity.CREDENTIAL_PROXY,
        )
    mcp_adapters = {
        f"mcp:{server.server_id}": ManagedMcpEgressAdapter(server)
        for server in settings.mcp_egress_servers
    }
    closeables: tuple[Any, ...] = (
        *((registry,) if registry is not None else ()),
        *((vault,) if isinstance(vault, HashiCorpVault) else ()),
        *((policy,) if policy is not None else ()),
        *mcp_adapters.values(),
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
        readiness_probe=(
            vault.readiness if isinstance(vault, HashiCorpVault) else None
        ),
    )
    service = CredentialProxyInternalService(
        CredentialProxy(vault, registry=registry),
        adapters={
            "webhook": ManagedWebhookCredentialAdapter(
                allowed_hosts=settings.allowed_credential_egress_hosts
            ),
            **mcp_adapters,
        },
        policy=policy,
    )
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
        if settings.postgres_enabled
        else None
    )
    admin_store = (
        PostgresAdminOperationStore(
            settings.resolved_database_url, schema="artifact"
        )
        if settings.postgres_enabled
        else None
    )
    verifier = (
        SeaweedFSObjectVerifier(presigner)
        if settings.deployment_profile == "production"
        else None
    )
    multipart = (
        SeaweedFSMultipartClient(presigner)
        if settings.deployment_profile == "production"
        else None
    )
    policy: RemotePolicyClient | None = None
    if settings.deployment_profile == "production":
        token = settings.workload_token_value(ServiceIdentity.ARTIFACT_SERVICE.value)
        policy = RemotePolicyClient(
            settings.policy_base_url,
            bearer_token=token or secrets.token_urlsafe(32),
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
    session = RemoteSessionEventStore(
        settings.session_base_url,
        service_identity=ServiceIdentity.DELIVERY_WORKER,
        bearer_token=bearer_token,
    )
    outbox = RemoteSessionDeliveryOutboxSource(
        session, worker_id="delivery-worker"
    )
    store = (
        PostgresDeliveryJobStore(settings.resolved_database_url)
        if settings.postgres_enabled
        else InMemoryDeliveryJobStore()
    )
    admin_store = (
        PostgresAdminOperationStore(
            settings.resolved_database_url, schema="delivery"
        )
        if settings.postgres_enabled
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
    if settings.postgres_enabled:
        closeables += (store, admin_store)
    app = _base_service_app(
        spec,
        settings,
        tick=worker.run_once,
        closeables=closeables,
    )
    app.state.session_access = "http"
    app.state.delivery_store_owner = True
    async def delivery_status(parameters: dict[str, Any]) -> dict[str, Any]:
        tenant_id = str(parameters.get("tenant_id", ""))
        session_id = str(parameters.get("session_id", ""))
        jobs = await store.list_jobs(tenant_id, session_id) if tenant_id and session_id else []
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
    if not settings.postgres_enabled:
        return _base_service_app(spec, settings, worker_interval=worker_interval)
    projector = providers.get_task_projection()
    admin_store = PostgresAdminOperationStore(
        settings.resolved_database_url, schema="projection"
    )
    closeables: tuple[Any, ...] = ()
    remote_session: RemoteSessionEventStore | None = None
    if settings.deployment_profile == "production":
        token = settings.workload_token_value(ServiceIdentity.PROJECTION_WORKER.value)
        remote_session = RemoteSessionEventStore(
            settings.session_base_url,
            service_identity=ServiceIdentity.PROJECTION_WORKER,
            bearer_token=token or secrets.token_urlsafe(32),
        )
        source = RemoteSessionOutboxSource(
            remote_session, worker_id="projection-worker"
        )
        relay = OutboxRelay(source, projector)
        closeables = (remote_session, projector, admin_store)
    else:
        relay = OutboxRelay(providers.get_event_store(), projector)
        closeables = (projector, admin_store)
    app = _base_service_app(
        spec,
        settings,
        tick=relay.relay_once,
        worker_interval=worker_interval,
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
        if settings.postgres_enabled
        else None
    )
    if settings.deployment_profile == "production":
        token = settings.workload_token_value(ServiceIdentity.MODEL_GATEWAY.value)
        policy = RemotePolicyClient(
            settings.policy_base_url,
            bearer_token=token or secrets.token_urlsafe(32),
            service_identity=ServiceIdentity.MODEL_GATEWAY,
        )
    app = _base_service_app(
        spec,
        settings,
        closeables=(
            *((policy,) if policy is not None else ()),
            *((state,) if state is not None else ()),
        ),
    )
    model = (
        providers.get_model_gateway()
        if settings.model_gateway_configured
        else UnavailableModelClient()
    )
    contract_app = create_contract_app(
        "model-gateway",
        model_routes(
            ModelGatewayInternalService(
                model,
                policy=policy,
                state=state,
                tenant_token_limit=settings.model_tenant_token_limit_per_hour,
            )
        ),
        workload_identities=_configured_identities(
            settings, (ServiceIdentity.AGENT_RUNTIME,)
        ),
    )
    app.mount("/", contract_app)
    return app


def _runtime_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    token = settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value)
    bearer_token = token or "development-runtime-token"
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
        base_url=settings.hands_mcp_url.removesuffix("/mcp"),
        timeout=settings.model_timeout_seconds,
    )
    hands = HandsMcpClient(
        HttpMcpTransport(
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
        return create_app(profile="streaming-gateway")
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
