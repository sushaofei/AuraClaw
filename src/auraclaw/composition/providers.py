from functools import lru_cache

from auraclaw.config import get_settings
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.gateways.query.waiter import TaskResultWaiter
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.gateways.task.invocations import SyncInvocationGateway
from auraclaw.infrastructure.kafka.runtime_events import (
    KafkaRuntimeEventProducer,
    KafkaStreamingIngestor,
    PostgresRuntimeEventStore,
    ReplayRuntimeEventBus,
    RuntimeEventProducerSDK,
    SDKRuntimeEventPublisher,
)
from auraclaw.infrastructure.model import OpenAICompatibleProvider
from auraclaw.infrastructure.observability.stores import (
    InMemoryObservabilityStore,
    PostgresObservabilityStore,
)
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
from auraclaw.projection.ports import ProjectionWriter
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.model_gateway import ModelGateway, StaticCredentialResolver
from auraclaw.runtime.ports import ModelClient
from auraclaw.session.task_service import TaskService

Store = InMemoryEventStore | PostgresEventStore
Projection = InMemoryTaskProjection | PostgresTaskProjection
ApprovalProjection = InMemoryApprovalProjection | PostgresApprovalProjection
CollaborationProjection = InMemoryCollaborationProjection | PostgresCollaborationProjection
ObservabilityStore = InMemoryObservabilityStore | PostgresObservabilityStore
RuntimeReplayStore = ReplayRuntimeEventBus | PostgresRuntimeEventStore


@lru_cache
def get_event_store() -> Store:
    settings = get_settings()
    if settings.sql_storage_enabled:
        return PostgresEventStore(settings.resolved_database_url)
    return InMemoryEventStore()


@lru_cache
def get_task_projection() -> Projection:
    settings = get_settings()
    if settings.sql_storage_enabled:
        return PostgresTaskProjection(settings.resolved_database_url)
    return InMemoryTaskProjection()


@lru_cache
def get_approval_projection() -> ApprovalProjection:
    settings = get_settings()
    if settings.sql_storage_enabled:
        return PostgresApprovalProjection(settings.resolved_database_url)
    return InMemoryApprovalProjection()


@lru_cache
def get_collaboration_projection() -> CollaborationProjection:
    settings = get_settings()
    if settings.sql_storage_enabled:
        return PostgresCollaborationProjection(settings.resolved_database_url)
    return InMemoryCollaborationProjection()


def session_outbox_projectors() -> tuple[ProjectionWriter, ...]:
    """Projectors that must consume Session outbox in every topology."""
    return (
        get_task_projection(),
        get_approval_projection(),
        get_collaboration_projection(),
    )


@lru_cache
def get_task_service() -> TaskService:
    projection = get_task_projection()
    approvals = get_approval_projection()
    event_store = get_event_store()
    relay = OutboxRelay(
        event_store,
        CompositeProjection(
            *session_outbox_projectors(),
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
    return TaskQueryService(
        get_task_projection(),
        get_collaboration_projection(),
        get_event_store(),
    )


@lru_cache
def get_task_result_waiter() -> TaskResultWaiter:
    settings = get_settings()
    return TaskResultWaiter(
        get_task_query_service(),
        poll_interval=settings.sync_invoke_poll_interval_seconds,
        max_concurrent=settings.sync_invoke_max_concurrent,
        default_timeout_seconds=settings.sync_invoke_default_timeout_seconds,
        max_timeout_seconds=settings.sync_invoke_max_timeout_seconds,
    )


@lru_cache
def get_sync_invocation_gateway() -> SyncInvocationGateway:
    return SyncInvocationGateway(get_task_command_gateway(), get_task_result_waiter())


@lru_cache
def get_runtime_replay_bus() -> RuntimeReplayStore:
    settings = get_settings()
    if settings.sql_storage_enabled:
        return PostgresRuntimeEventStore(
            settings.resolved_database_url,
            retention_events=settings.runtime_event_retention_events,
            connection_queue_size=settings.stream_connection_queue_size,
        )
    return ReplayRuntimeEventBus(
        retention_events=settings.runtime_event_retention_events,
        connection_queue_size=settings.stream_connection_queue_size,
    )


@lru_cache
def get_runtime_event_producer() -> KafkaRuntimeEventProducer | RuntimeReplayStore:
    settings = get_settings()
    if settings.kafka_enabled:
        return KafkaRuntimeEventProducer(
            settings.kafka_bootstrap_servers,
            topic=settings.kafka_runtime_topic,
        )
    return get_runtime_replay_bus()


@lru_cache
def get_runtime_event_publisher() -> SDKRuntimeEventPublisher:
    # Provider streams already arrive as meaningful chunks. A one-byte threshold
    # preserves those chunks while retaining SDK validation, redaction and sequencing.
    settings = get_settings()
    producer = get_runtime_event_producer()
    replay = get_runtime_replay_bus()
    allocator = (
        replay
        if not settings.kafka_enabled and isinstance(replay, PostgresRuntimeEventStore)
        else None
    )
    sdk = RuntimeEventProducerSDK(
        producer,
        sequence_allocator=allocator,
        delta_flush_bytes=1,
    )
    return SDKRuntimeEventPublisher(sdk)


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
    settings = get_settings()
    return StreamingGateway(
        reader=get_task_projection(),
        bus=get_runtime_replay_bus(),
        delta_min_interval=settings.stream_delta_min_interval_seconds,
    )


@lru_cache
def get_model_gateway() -> ModelClient:
    settings = get_settings()
    if not settings.model_gateway_configured:
        raise RuntimeError("AURACLAW_MODEL_API_KEY, BASE_URL and NAME must be configured")
    assert settings.model_api_key is not None
    assert settings.model_base_url is not None
    assert settings.model_name is not None
    adapter = OpenAICompatibleProvider(
        base_url=settings.model_base_url,
        model=settings.model_name,
        name=settings.model_provider,
        timeout_seconds=settings.model_timeout_seconds,
        thinking_enabled=settings.model_thinking_enabled,
    )
    return ModelGateway(
        (adapter,),
        StaticCredentialResolver({settings.model_provider: settings.model_api_key}),
        default_provider=settings.model_provider,
    )


@lru_cache
def get_observability_store() -> ObservabilityStore:
    settings = get_settings()
    if settings.sql_storage_enabled:
        return PostgresObservabilityStore(settings.resolved_database_url)
    return InMemoryObservabilityStore()


@lru_cache
def get_observability_service() -> ObservabilityService:
    return ObservabilityService(get_observability_store(), get_event_store())
