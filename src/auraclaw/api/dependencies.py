from dataclasses import dataclass
from functools import lru_cache
from uuid import uuid4

from fastapi import Header

from auraclaw.application.streaming import StreamingGateway
from auraclaw.application.tasks import AllowAllAdmissionController, TaskService
from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.infrastructure.memory import InMemoryEventStore
from auraclaw.infrastructure.postgres import (
    PostgresApprovalProjection,
    PostgresCollaborationProjection,
    PostgresEventStore,
    PostgresTaskProjection,
)
from auraclaw.infrastructure.runtime_events import (
    KafkaRuntimeEventProducer,
    KafkaStreamingIngestor,
    ReplayRuntimeEventBus,
)
from auraclaw.projections.approvals import CompositeProjection, InMemoryApprovalProjection
from auraclaw.projections.collaboration import InMemoryCollaborationProjection
from auraclaw.projections.relay import OutboxRelay
from auraclaw.projections.tasks import InMemoryTaskProjection


@dataclass(frozen=True)
class RequestIdentity:
    tenant_id: str
    actor: Actor
    correlation_id: str


Store = InMemoryEventStore | PostgresEventStore
Projection = InMemoryTaskProjection | PostgresTaskProjection
ApprovalProjection = InMemoryApprovalProjection | PostgresApprovalProjection
CollaborationProjection = InMemoryCollaborationProjection | PostgresCollaborationProjection


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
def get_approval_projection() -> ApprovalProjection:
    settings = get_settings()
    if settings.postgres_enabled:
        return PostgresApprovalProjection(settings.resolved_database_url)
    return InMemoryApprovalProjection()


@lru_cache
def get_collaboration_projection() -> CollaborationProjection:
    settings = get_settings()
    if settings.postgres_enabled:
        return PostgresCollaborationProjection(settings.resolved_database_url)
    return InMemoryCollaborationProjection()


@lru_cache
def get_task_service() -> TaskService:
    projection = get_task_projection()
    approvals = get_approval_projection()
    collaboration = get_collaboration_projection()
    event_store = get_event_store()
    relay = OutboxRelay(
        event_store,
        CompositeProjection(projection, approvals, collaboration),
    )
    return TaskService(
        event_store=event_store,
        relay=relay,
        reader=projection,
        admission=AllowAllAdmissionController(),
        approvals=approvals,
    )


@lru_cache
def get_runtime_replay_bus() -> ReplayRuntimeEventBus:
    settings = get_settings()
    return ReplayRuntimeEventBus(
        retention_events=settings.runtime_event_retention_events,
        connection_queue_size=settings.stream_connection_queue_size,
    )


@lru_cache
def get_runtime_event_producer() -> KafkaRuntimeEventProducer | ReplayRuntimeEventBus:
    settings = get_settings()
    if settings.kafka_enabled:
        return KafkaRuntimeEventProducer(
            settings.kafka_bootstrap_servers,
            topic=settings.kafka_runtime_topic,
        )
    return get_runtime_replay_bus()


@lru_cache
def get_streaming_ingestor() -> KafkaStreamingIngestor | None:
    settings = get_settings()
    if not settings.kafka_enabled:
        return None
    return KafkaStreamingIngestor(
        settings.kafka_bootstrap_servers,
        topic=settings.kafka_runtime_topic,
        group_id=settings.kafka_streaming_group,
        target=get_runtime_replay_bus(),
    )


@lru_cache
def get_streaming_gateway() -> StreamingGateway:
    return StreamingGateway(reader=get_task_projection(), bus=get_runtime_replay_bus())


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
