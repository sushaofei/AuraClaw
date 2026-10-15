"""Common service health endpoints and worker startup, wake and shutdown lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from auraclaw import __version__
from auraclaw.composition.security import has_workload_tokens, lease_key_configured
from auraclaw.composition.service_spec import DATABASE_SERVICES, ServiceSpec
from auraclaw.composition.worker_wake import WorkerWakeGate
from auraclaw.config import Settings
from auraclaw.contracts.internal import (
    ServiceIdentity,
)
from auraclaw.infrastructure.observability import configure_json_logging


def worker_idle_interval(settings: Settings, configured: float) -> float:
    if settings.worker_wake_enabled:
        return settings.worker_idle_interval
    return configured


def worker_post_tick_wait(settings: Settings, configured: float) -> float:
    """Idle sleep after a tick that already long-polled the Session outbox."""
    if settings.worker_wake_enabled:
        return 0.05
    return configured


def readiness(name: str, settings: Settings) -> tuple[bool, dict[str, str]]:
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
        dependencies["upstream_identity"] = (
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
        lease_ready = lease_key_configured(settings)
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
        identity_ready = has_workload_tokens(settings, required_identities)
        dependencies["workload_identities"] = "ready" if identity_ready else "missing"
        ready = ready and identity_ready
    if name == "orchestrator":
        lease_ready = lease_key_configured(settings)
        identity_ready = has_workload_tokens(
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
        lease_ready = lease_key_configured(settings)
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
        identity_ready = has_workload_tokens(settings, identities)
        dependencies["enforcement_identities"] = "ready" if identity_ready else "missing"
        ready = ready and identity_ready
    if name == "credential-proxy":
        identity_ready = has_workload_tokens(
            settings,
            (
                ServiceIdentity.TASK_API,
                ServiceIdentity.ACTION_HANDS,
                ServiceIdentity.DELIVERY_WORKER,
            ),
        )
        vault_auth_configured = settings.credential_vault_token is not None or (
            bool(settings.credential_vault_approle_role_id)
            and settings.credential_vault_approle_secret_id is not None
        )
        vault_configured = bool(settings.credential_vault_addr) and vault_auth_configured
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
    configure_json_logging(level=log_level, service=app.state.service_name)
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


def base_service_app(
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
    ready, dependencies = readiness(spec.name, settings)
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
