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
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from auraclaw import __version__
from auraclaw.action.capability_catalog import (
    InMemoryCapabilityCatalogStore,
)
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
from auraclaw.action.skill_lifecycle import (
    InMemorySkillLifecycleStore,
    SkillLifecycleStore,
)
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import (
    SkillPublisherTrustService,
)
from auraclaw.composition.worker_wake import WorkerWakeGate
from auraclaw.config import Settings, get_settings
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
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.clients.runtime import (
    RemoteRuntimeControlClient,
)
from auraclaw.infrastructure.credentials.proxy import CredentialProxy
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_fencing_ledger import (
    FencingLedgerOwner,
    PostgresFencingTokenLedger,
)
from auraclaw.infrastructure.persistence.postgres_mcp_registry import (
    PostgresMcpServerRegistryStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)
from auraclaw.internal.http import HttpContractClient
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
)
from auraclaw.runtime.harness import AgentHarness

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
            selected_lifecycle = PostgresSkillLifecycleStore(
                settings.resolved_database_url,
                transaction_retry_attempts=settings.skill_transaction_retry_attempts,
                transaction_retry_base_delay=(
                    settings.skill_transaction_retry_base_delay_seconds
                ),
            )
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


ServiceBuilder = Callable[[ServiceSpec, Settings, float], FastAPI]


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
            ServiceIdentity.ACTION_HANDS,
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


def _task_api_builder(spec: ServiceSpec, settings: Settings, _worker_interval: float) -> FastAPI:
    from auraclaw.composition.builders.task_api import build_task_api_app

    return build_task_api_app(spec, settings)


def _streaming_gateway_builder(
    spec: ServiceSpec, settings: Settings, _worker_interval: float
) -> FastAPI:
    from auraclaw.composition.builders.streaming import build_streaming_gateway_app

    return build_streaming_gateway_app(spec, settings)


def _session_builder(spec: ServiceSpec, settings: Settings, _worker_interval: float) -> FastAPI:
    from auraclaw.composition.builders.session import build_session_app

    return build_session_app(spec, settings)


def _orchestrator_builder(
    spec: ServiceSpec, settings: Settings, _worker_interval: float
) -> FastAPI:
    from auraclaw.composition.builders.orchestrator import build_orchestrator_app

    return build_orchestrator_app(spec, settings)


def _runtime_builder(spec: ServiceSpec, settings: Settings, _worker_interval: float) -> FastAPI:
    from auraclaw.composition.builders.runtime import build_agent_runtime_app

    return build_agent_runtime_app(spec, settings)


def _model_gateway_builder(
    spec: ServiceSpec, settings: Settings, _worker_interval: float
) -> FastAPI:
    from auraclaw.composition.builders.model_gateway import build_model_gateway_app

    return build_model_gateway_app(spec, settings)


def _hands_builder(spec: ServiceSpec, settings: Settings, _worker_interval: float) -> FastAPI:
    from auraclaw.composition.builders.hands import build_action_hands_app

    return build_action_hands_app(spec, settings)


def _policy_builder(spec: ServiceSpec, settings: Settings, _worker_interval: float) -> FastAPI:
    from auraclaw.composition.builders.policy import build_policy_app

    return build_policy_app(spec, settings)


def _credential_proxy_builder(
    spec: ServiceSpec, settings: Settings, _worker_interval: float
) -> FastAPI:
    from auraclaw.composition.builders.credential_proxy import build_credential_proxy_app

    return build_credential_proxy_app(spec, settings)


def _artifact_service_builder(
    spec: ServiceSpec, settings: Settings, _worker_interval: float
) -> FastAPI:
    from auraclaw.composition.builders.artifact import build_artifact_service_app

    return build_artifact_service_app(spec, settings)


def _delivery_builder(spec: ServiceSpec, settings: Settings, _worker_interval: float) -> FastAPI:
    from auraclaw.composition.builders.delivery import build_delivery_worker_app

    return build_delivery_worker_app(
        spec,
        settings,
        worker_interval=_worker_interval,
    )


def _projection_builder(
    spec: ServiceSpec, settings: Settings, worker_interval: float
) -> FastAPI:
    from auraclaw.composition.builders.projection import build_projection_app

    return build_projection_app(spec, settings, worker_interval=worker_interval)


def _base_builder(spec: ServiceSpec, settings: Settings, worker_interval: float) -> FastAPI:
    return _base_service_app(spec, settings, worker_interval=worker_interval)


SERVICE_BUILDERS: dict[str, ServiceBuilder] = {
    "task-api": _task_api_builder,
    "streaming-gateway": _streaming_gateway_builder,
    "session": _session_builder,
    "action-hands": _hands_builder,
    "model-gateway": _model_gateway_builder,
    "policy": _policy_builder,
    "credential-proxy": _credential_proxy_builder,
    "artifact-service": _artifact_service_builder,
    "delivery-worker": _delivery_builder,
    "orchestrator": _orchestrator_builder,
    "agent-runtime": _runtime_builder,
    "projection-worker": _projection_builder,
    "default": _base_builder,
}


def create_service_app(
    command: str,
    settings: Settings | None = None,
    *,
    worker_interval: float = 1.0,
) -> FastAPI:
    selected = settings or get_settings()
    spec = service_spec(command, selected)
    builder = SERVICE_BUILDERS.get(spec.name, SERVICE_BUILDERS["default"])
    return builder(spec, selected, worker_interval)
