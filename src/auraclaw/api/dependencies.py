from dataclasses import dataclass
from functools import lru_cache
from uuid import uuid4

from fastapi import Header

from auraclaw.application.tasks import TaskService
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.infrastructure.memory import InMemoryEventStore
from auraclaw.projections.tasks import InMemoryTaskProjection


@dataclass(frozen=True)
class RequestIdentity:
    tenant_id: str
    actor: Actor
    correlation_id: str


@lru_cache
def get_event_store() -> InMemoryEventStore:
    return InMemoryEventStore()


@lru_cache
def get_task_projection() -> InMemoryTaskProjection:
    return InMemoryTaskProjection()


@lru_cache
def get_task_service() -> TaskService:
    projection = get_task_projection()
    return TaskService(event_store=get_event_store(), projector=projection, reader=projection)


async def request_identity(
    tenant_id: str = Header(default="local", alias="X-Tenant-ID"),
    actor_id: str = Header(default="local-user", alias="X-Actor-ID"),
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
) -> CommandContext:
    return CommandContext(
        command_id=command_id,
        tenant_id=identity.tenant_id,
        actor=identity.actor,
        correlation_id=identity.correlation_id,
        expected_version=expected_version,
    )
