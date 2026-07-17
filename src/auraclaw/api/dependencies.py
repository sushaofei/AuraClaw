from dataclasses import dataclass
from functools import lru_cache
from uuid import uuid4

from fastapi import Header

from auraclaw.application.tasks import AllowAllAdmissionController, TaskService
from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.infrastructure.memory import InMemoryEventStore
from auraclaw.infrastructure.postgres import PostgresEventStore, PostgresTaskProjection
from auraclaw.projections.relay import OutboxRelay
from auraclaw.projections.tasks import InMemoryTaskProjection


@dataclass(frozen=True)
class RequestIdentity:
    tenant_id: str
    actor: Actor
    correlation_id: str


Store = InMemoryEventStore | PostgresEventStore
Projection = InMemoryTaskProjection | PostgresTaskProjection


@lru_cache
def get_event_store() -> Store:
    settings = get_settings()
    if settings.postgres_enabled:
        return PostgresEventStore(settings.resolved_database_url)
    return InMemoryEventStore()


@lru_cache
def get_task_projection() -> Projection:
    settings = get_settings()
    if settings.postgres_enabled:
        return PostgresTaskProjection(settings.resolved_database_url)
    return InMemoryTaskProjection()


@lru_cache
def get_task_service() -> TaskService:
    projection = get_task_projection()
    event_store = get_event_store()
    relay = OutboxRelay(event_store, projection)
    return TaskService(
        event_store=event_store,
        relay=relay,
        reader=projection,
        admission=AllowAllAdmissionController(),
    )


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
