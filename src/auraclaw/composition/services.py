from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from auraclaw import __version__
from auraclaw.action.mcp import HandsMcpServer
from auraclaw.action.mcp_http import StaticWorkloadAuthenticator, create_hands_mcp_app
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.composition import providers
from auraclaw.composition.api import create_app
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.contracts.mcp import McpTrustedContext
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.hands.local import LocalHandsService
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import session_routes
from auraclaw.internal.security import InMemoryFencingTokenLedger, LeaseAssertionVerifier
from auraclaw.projection.relay import OutboxRelay
from auraclaw.session.internal_service import SessionInternalService

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
}

DATABASE_SERVICES = {
    "session",
    "projection-worker",
    "orchestrator",
    "action-hands",
    "policy",
    "credential-proxy",
    "artifact-service",
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
    if name == "session":
        lease_ready = (
            settings.lease_signing_key is not None
            and len(settings.lease_signing_key.get_secret_value()) >= 32
            or settings.deployment_profile == "development"
        )
        dependencies["lease_signing_key"] = "ready" if lease_ready else "missing"
        ready = ready and lease_ready
    if name == "artifact-service":
        storage_ready = settings.seaweedfs_enabled or settings.deployment_profile == "development"
        dependencies["seaweedfs"] = "ready" if storage_ready else "missing"
        ready = ready and storage_ready
    if name == "action-hands":
        identity_ready = (
            settings.runtime_workload_token is not None
            and bool(settings.runtime_workload_token.get_secret_value())
            or settings.deployment_profile == "development"
        )
        dependencies["workload_identity"] = "ready" if identity_ready else "missing"
        ready = ready and identity_ready
    return ready, dependencies


@asynccontextmanager
async def _service_lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.started_at = datetime.now(UTC)
    app.state.stopping = False
    app.state.worker_iterations = 0
    stop = asyncio.Event()
    worker_task: asyncio.Task[None] | None = None

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


def _base_service_app(
    spec: ServiceSpec,
    settings: Settings,
    *,
    tick: Callable[[], Awaitable[int | None]] | None = None,
    worker_interval: float = 1.0,
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
    app.state.worker_error = None
    ready, dependencies = _readiness(spec.name, settings)
    app.state.config_ready = ready
    app.state.dependencies = dependencies

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
    )
    service = SessionInternalService(providers.get_event_store(), lease_verifier=verifier)
    identities = None
    if settings.deployment_profile == "development":
        identities = {
            f"development-{identity.value}": identity
            for identity in ServiceIdentity
        }
    contract_app = create_contract_app(
        "session",
        session_routes(service),
        workload_identities=identities,
    )
    app.mount("/", contract_app)
    return app


def _hands_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    app = _base_service_app(spec, settings)
    registry = ToolRegistry()
    gateway = ToolGateway(
        registry=registry,
        policy=PolicyEngine(version="s2-v1"),
        approvals=EmptyApprovalReader(),
        hands=LocalHandsService(workspace_root=Path.cwd()),
        artifacts=ArtifactStore(
            InMemoryObjectStorage(), signing_key=b"auraclaw-s2-artifact-key"
        ),
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
    trusted = McpTrustedContext(
        tenant_id="development",
        root_session_id="development",
        session_id="development",
        run_id="development",
        runtime_id="development-runtime",
        lease_id="development-lease",
        fencing_token=1,
        deadline=datetime.now(UTC) + timedelta(hours=24),
    )
    mcp_app = create_hands_mcp_app(
        HandsMcpServer(registry=registry, gateway=gateway),
        authenticator=StaticWorkloadAuthenticator({token: trusted}),
    )
    app.mount("/", mcp_app)
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
        return create_app(profile="task-api")
    if spec.name == "streaming-gateway":
        return create_app(profile="streaming-gateway")
    if spec.name == "session":
        return _session_app(spec, selected)
    if spec.name == "action-hands":
        return _hands_app(spec, selected)
    if spec.name == "projection-worker":
        if not selected.postgres_enabled:
            return _base_service_app(
                spec, selected, worker_interval=worker_interval
            )
        relay = OutboxRelay(providers.get_event_store(), providers.get_task_projection())
        return _base_service_app(
            spec,
            selected,
            tick=relay.relay_once,
            worker_interval=worker_interval,
        )
    if spec.name == "orchestrator":
        if not selected.postgres_enabled:
            return _base_service_app(spec, selected)
        return _base_service_app(
            spec,
            selected,
            tick=providers.get_control_store().recover_expired,
        )
    return _base_service_app(spec, selected)
