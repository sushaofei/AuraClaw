from functools import lru_cache

from auraclaw.composition.adapters.development_worker import (
    DelayedRuntimeEventPublisher,
    DevelopmentModelClient,
    DevelopmentRuntimeWorker,
)
from auraclaw.config import get_settings
from auraclaw.control.orchestrator import LocalRuntimeProvisioner, ManagedOrchestrator
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.infrastructure.kafka.runtime_events import (
    KafkaRuntimeEventProducer,
    KafkaStreamingIngestor,
    ReplayRuntimeEventBus,
)
from auraclaw.infrastructure.observability.stores import (
    InMemoryObservabilityStore,
    PostgresObservabilityStore,
)
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.projection.postgres_approval_store import (
    PostgresApprovalProjection,
)
from auraclaw.infrastructure.projection.postgres_collaboration_store import (
    PostgresCollaborationProjection,
)
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.observability.service import ObservabilityProjector, ObservabilityService
from auraclaw.projection.approval.projector import CompositeProjection, InMemoryApprovalProjection
from auraclaw.projection.collaboration.projector import InMemoryCollaborationProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.clients import FencedSessionClient, FencedToolClient, IdempotentToolClient
from auraclaw.runtime.harness import AgentHarness
from auraclaw.session.task_service import TaskService

Store = InMemoryEventStore | PostgresEventStore
Projection = InMemoryTaskProjection | PostgresTaskProjection
ApprovalProjection = InMemoryApprovalProjection | PostgresApprovalProjection
CollaborationProjection = InMemoryCollaborationProjection | PostgresCollaborationProjection
ObservabilityStore = InMemoryObservabilityStore | PostgresObservabilityStore


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
        CompositeProjection(
            projection,
            approvals,
            collaboration,
            ObservabilityProjector(get_observability_service()),
        ),
    )
    return TaskService(
        event_store=event_store,
        relay=relay,
        reader=projection,
        admission=AllowAllAdmissionController(),
        approvals=approvals,
    )


def get_task_command_gateway() -> TaskCommandGateway:
    return TaskCommandGateway(get_task_service())


def get_task_query_service() -> TaskQueryService:
    return TaskQueryService(get_task_projection(), get_collaboration_projection())


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


def build_development_runtime_worker() -> DevelopmentRuntimeWorker:
    settings = get_settings()
    event_store = get_event_store()
    projection = get_task_projection()
    control = InMemoryControlStateStore()
    session = FencedSessionClient(event_store, control)
    publisher = DelayedRuntimeEventPublisher(
        # The worker and Streaming Gateway share this process. Publishing its
        # deterministic test deltas directly keeps the test surface available
        # even when a developer's .env selects Kafka but Kafka is unavailable.
        get_runtime_replay_bus().publish,
        delta_delay=settings.development_stream_delay,
    )
    relay = OutboxRelay(
        event_store,
        CompositeProjection(
            projection,
            get_approval_projection(),
            get_collaboration_projection(),
            ObservabilityProjector(get_observability_service()),
        ),
    )
    orchestrator = ManagedOrchestrator(
        orchestrator_id="development-orchestrator",
        control_store=control,
        session=session,
        provisioner=LocalRuntimeProvisioner("development"),
    )
    harness = AgentHarness(
        control_store=control,
        session=session,
        model=DevelopmentModelClient(),
        tools=FencedToolClient(IdempotentToolClient(), control),
        runtime_events=publisher,
    )
    return DevelopmentRuntimeWorker(
        event_store=event_store,
        reader=projection,
        relay=relay,
        orchestrator=orchestrator,
        harness=harness,
        poll_interval=settings.development_runtime_poll_interval,
    )


@lru_cache
def get_observability_store() -> ObservabilityStore:
    settings = get_settings()
    if settings.postgres_enabled:
        return PostgresObservabilityStore(settings.resolved_database_url)
    return InMemoryObservabilityStore()


@lru_cache
def get_observability_service() -> ObservabilityService:
    return ObservabilityService(get_observability_store(), get_event_store())
