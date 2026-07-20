from dataclasses import dataclass
from uuid import uuid4

from fastapi import Header

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.observability.service import ObservabilityService
from auraclaw.projection.ports import CollaborationReader, TaskReader


@dataclass(frozen=True)
class RequestIdentity:
    tenant_id: str
    actor: Actor
    correlation_id: str


async def request_identity(
    tenant_id: str = Header(default="local", alias="X-Tenant-ID", min_length=1),
    actor_id: str = Header(default="local-user", alias="X-Actor-ID", min_length=1),
    correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
) -> RequestIdentity:
    return RequestIdentity(
        tenant_id=tenant_id,
        actor=Actor(type="user", id=actor_id),
        correlation_id=correlation_id or f"corr_{uuid4().hex}",
    )


def command_context(
    *,
    identity: RequestIdentity,
    command_id: str,
    expected_version: int,
    operation: str,
) -> CommandContext:
    return CommandContext(
        command_id=command_id,
        tenant_id=identity.tenant_id,
        actor=identity.actor,
        correlation_id=identity.correlation_id,
        expected_version=expected_version,
        operation=operation,
    )


def get_task_command_gateway() -> TaskCommandGateway:
    raise RuntimeError("TaskCommandGateway dependency was not configured by composition")


def get_task_projection() -> TaskReader:
    raise RuntimeError("TaskReader dependency was not configured by composition")


def get_task_query_service() -> TaskQueryService:
    raise RuntimeError("TaskQueryService dependency was not configured by composition")


def get_collaboration_projection() -> CollaborationReader:
    raise RuntimeError("CollaborationReader dependency was not configured by composition")


def get_streaming_gateway() -> StreamingGateway:
    raise RuntimeError("StreamingGateway dependency was not configured by composition")


def get_observability_service() -> ObservabilityService:
    raise RuntimeError("ObservabilityService dependency was not configured by composition")
