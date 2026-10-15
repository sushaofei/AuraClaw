"""Public service entrypoint and dispatch to independently importable builders."""

from __future__ import annotations

from fastapi import FastAPI

from auraclaw.composition.service_host import base_service_app
from auraclaw.composition.service_spec import DATABASE_SERVICES as DATABASE_SERVICES
from auraclaw.composition.service_spec import KAFKA_SERVICES, ServiceBuilder
from auraclaw.composition.service_spec import SERVICE_BY_COMMAND as SERVICE_BY_COMMAND
from auraclaw.composition.service_spec import ServiceSpec as ServiceSpec
from auraclaw.composition.service_spec import service_spec as service_spec
from auraclaw.config import Settings, get_settings


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
    return base_service_app(spec, settings, worker_interval=worker_interval)


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
    if selected.deployment_profile == "production":
        if spec.name in DATABASE_SERVICES and not selected.sql_storage_enabled:
            raise ValueError(f"{spec.name} production composition requires SQL storage")
        if spec.name in KAFKA_SERVICES and not selected.kafka_enabled:
            raise ValueError(f"{spec.name} production composition requires Kafka")
    builder = SERVICE_BUILDERS.get(spec.name, SERVICE_BUILDERS["default"])
    return builder(spec, selected, worker_interval)
