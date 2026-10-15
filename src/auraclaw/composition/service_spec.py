"""Service topology and entrypoint specifications shared by the CLI and builders."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from fastapi import FastAPI

from auraclaw.config import Settings, get_settings

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
    "task-api",
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


KAFKA_SERVICES = {
    "agent-runtime",
    "action-hands",
    "streaming-gateway",
}


@dataclass(frozen=True)
class ServiceSpec:
    command: str
    name: str
    port: int
    worker: bool


ServiceBuilder = Callable[[ServiceSpec, Settings, float], FastAPI]


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
