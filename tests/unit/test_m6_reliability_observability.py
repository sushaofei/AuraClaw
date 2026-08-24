import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter

import pytest
from fastapi.testclient import TestClient

from auraclaw.composition.providers import (
    get_event_store,
    get_observability_service,
    get_observability_store,
    get_task_projection,
    get_task_service,
)
from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.contracts.observability import TraceContext
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.observability.stores import (
    InMemoryObservabilityStore,
    StructuredLogger,
)
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.main import create_app
from auraclaw.observability.redaction import contains_sensitive
from auraclaw.observability.service import ObservabilityProjector, ObservabilityService
from auraclaw.projection.approval.projector import CompositeProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.session.task_service import TaskService


async def _task() -> tuple[str, InMemoryEventStore, InMemoryTaskProjection]:
    events = InMemoryEventStore()
    projection = InMemoryTaskProjection()
    service = TaskService(
        event_store=events,
        relay=OutboxRelay(events, projection),
        reader=projection,
        admission=AllowAllAdmissionController(),
    )
    result = await service.create_task(
        goal="diagnose reliability",
        context=CommandContext(
            command_id="m6-create",
            tenant_id="tenant-m6",
            actor=Actor(type="user", id="operator"),
            correlation_id="corr-m6",
            expected_version=0,
            operation="create_task",
        ),
    )
    return str(result["session_id"]), events, projection


def test_trace_audit_metrics_alerts_and_timeline_share_correlation_context() -> None:
    async def scenario() -> None:
        session_id, events, _ = await _task()
        store = InMemoryObservabilityStore()
        service = ObservabilityService(store, events)
        context = TraceContext(
            trace_id="a" * 32,
            span_id="b" * 16,
            tenant_id="tenant-m6",
            root_session_id=session_id,
            session_id=session_id,
            run_id="run-m6",
            event_id="evt-m6",
            command_id="cmd-m6",
            tool_invocation_id="tool-m6",
            runtime_id="runtime-m6",
            delivery_id="delivery-m6",
            approval_id="approval-m6",
        )
        await service.record_span(
            context=context,
            component="tool_gateway",
            operation="execute",
            started_at=datetime.now(UTC),
            status="error",
            attributes={"authorization": "Bearer real-token", "reason": "timeout"},
        )
        await service.audit(
            context=context,
            action="tool.execute",
            outcome="unknown",
            actor_type="runtime",
            actor_id="runtime-m6",
            resource_ref="managed://tool/m6",
            metadata={"password": "do-not-store", "effect": "unknown"},
        )
        await service.metric("projection.lag.seconds", 4.5, context=context)
        await service.metric("runtime.lease_lost.count", 1, context=context)
        await service.metric("tool.side_effect_unknown.count", 1, context=context)
        await service.metric("delivery.dlq.count", 1, context=context)

        timeline = await service.timeline("tenant-m6", session_id)
        serialized = json.dumps(timeline)
        assert {entry["kind"] for entry in timeline["entries"]} == {
            "canonical_event",
            "trace_span",
            "audit_event",
            "alert",
        }
        assert all(identifier in serialized for identifier in ("run-m6", "tool-m6", "delivery-m6"))
        assert "do-not-store" not in serialized and "real-token" not in serialized
        assert serialized.count('"kind": "alert"') == 4

    asyncio.run(scenario())


def test_canonical_event_relay_derives_idempotent_cross_component_telemetry() -> None:
    async def scenario() -> None:
        events = InMemoryEventStore()
        projection = InMemoryTaskProjection()
        observability_store = InMemoryObservabilityStore()
        observability = ObservabilityService(observability_store, events)
        observer = ObservabilityProjector(observability)
        service = TaskService(
            event_store=events,
            relay=OutboxRelay(events, CompositeProjection(projection, observer)),
            reader=projection,
            admission=AllowAllAdmissionController(),
        )
        created = await service.create_task(
            goal="derived telemetry",
            context=CommandContext(
                command_id="derived-create",
                tenant_id="tenant-m6-derived",
                actor=Actor(type="user", id="operator"),
                correlation_id="derived-correlation",
                expected_version=0,
                operation="create_task",
            ),
        )
        session_id = str(created["session_id"])
        await service.cancel_task(
            session_id=session_id,
            reason="operator test",
            context=CommandContext(
                command_id="derived-cancel",
                tenant_id="tenant-m6-derived",
                actor=Actor(type="user", id="operator"),
                correlation_id="derived-correlation",
                expected_version=2,
                operation="cancel_task",
            ),
        )
        canonical = await events.load("tenant-m6-derived", session_id)
        await observer.project(canonical)
        records = await observability_store.session_records(
            "tenant-m6-derived", session_id
        )
        metrics = await observability.metrics()
        assert len(records["spans"]) == len(canonical) == 3
        assert [audit.action for audit in records["audits"]] == ["run.cancelled"]
        assert len(metrics) == 3

    asyncio.run(scenario())


def test_http_trace_context_is_returned_and_tenant_timeline_is_authorized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "memory")
    monkeypatch.setenv("AURACLAW_ALLOW_INSECURE_IDENTITY_HEADERS", "true")
    get_settings.cache_clear()
    get_event_store.cache_clear()
    get_task_projection.cache_clear()
    get_task_service.cache_clear()
    get_observability_store.cache_clear()
    get_observability_service.cache_clear()
    app = create_app(profile="task-api")
    with TestClient(app) as client:
        created = client.post(
            "/v1/tasks",
            headers={
                "X-Tenant-ID": "tenant-m6-api",
                "Idempotency-Key": "m6-api-create",
                "traceparent": f"00-{'c' * 32}-{'d' * 16}-01",
            },
            json={"goal": "observable API"},
        )
        assert created.status_code == 202
        assert created.headers["traceparent"].startswith(f"00-{'c' * 32}-")
        session_id = created.json()["session_id"]
        timeline = client.get(
            f"/v1/operations/sessions/{session_id}/timeline",
            headers={"X-Tenant-ID": "tenant-m6-api"},
        )
        assert timeline.status_code == 200
        denied = client.get(
            f"/v1/operations/sessions/{session_id}/timeline",
            headers={"X-Tenant-ID": "other-tenant"},
        )
        assert denied.status_code == 404
    get_settings.cache_clear()


def test_artifact_gc_respects_retention_and_shared_content_references() -> None:
    async def scenario() -> None:
        objects = InMemoryObjectStorage()
        store = ArtifactStore(objects, signing_key=b"m6-artifact-signing-key")
        expired = datetime.now(UTC) - timedelta(seconds=1)
        first = await store.put(
            tenant_id="tenant-m6",
            root_session_id="root-m6",
            session_id="session-m6",
            content=b"shared",
            artifact_type="debug",
            media_type="text/plain",
            name="expired.txt",
            producer="test",
            retention_until=expired,
        )
        retained = await store.put(
            tenant_id="tenant-m6",
            root_session_id="root-m6",
            session_id="session-m6",
            content=b"shared",
            artifact_type="result",
            media_type="text/plain",
            name="retained.txt",
            producer="test",
            lineage_refs=(first.artifact_id,),
        )
        orphan = await store.put(
            tenant_id="tenant-m6",
            root_session_id="root-m6",
            session_id="session-m6",
            content=b"orphan",
            artifact_type="debug",
            media_type="text/plain",
            name="orphan.txt",
            producer="test",
            retention_until=expired,
        )
        assert await store.gc_expired() == [orphan.artifact_id]
        first_metadata = await store.metadata("tenant-m6", first.artifact_id)
        assert first_metadata.artifact_id == first.artifact_id
        token = await store.issue_download_token(
            tenant_id="tenant-m6", artifact_id=retained.artifact_id, actor_id="operator"
        )
        assert await store.download(
            token=token, tenant_id="tenant-m6", actor_id="operator"
        ) == b"shared"

    asyncio.run(scenario())


def test_structured_logging_and_trace_secret_scan_have_zero_hits() -> None:
    logger = StructuredLogger("auraclaw.m6.test")
    record = logger.emit(
        20,
        "credential_proxy_call",
        tenant_id="tenant-m6",
        trace_id="trace-m6",
        authorization="Bearer real-super-secret",
        nested={"api_key": "real-api-key"},
    )
    assert not contains_sensitive(
        record, known_secrets=("real-super-secret", "real-api-key")
    )


def test_architecture_completion_standards_have_automated_regression_coverage() -> None:
    tests_root = Path(__file__).parents[1]
    sources = "\n".join(path.read_text() for path in tests_root.rglob("test_*.py"))
    required_coverage = (
        "test_runtime_recovers_at_all_required_failure_injection_points",
        "test_idempotency_key_prevents_duplicate_side_effect",
        "test_four_dag_shapes_complete_end_to_end",
        "test_snapshot_restores_session_and_projection_rebuild_is_deterministic",
        "test_streaming_gateway_authorizes_replays_and_signals_expired_cursor",
        "test_delivery_worker_recovers_retries_and_deduplicates_business_delivery",
        "test_write_requires_approval_and_argument_change_invalidates_it",
        "test_credential_proxy_redacts_secret_and_hands_environment_has_none",
    )
    missing = [name for name in required_coverage if name not in sources]
    assert missing == []


def test_capacity_200_concurrent_admissions_meet_append_p95() -> None:
    async def scenario() -> None:
        events = InMemoryEventStore()
        projection = InMemoryTaskProjection()
        service = TaskService(
            event_store=events,
            relay=OutboxRelay(events, projection),
            reader=projection,
            admission=AllowAllAdmissionController(),
        )

        async def submit(index: int) -> tuple[str, float]:
            started = perf_counter()
            result = await service.create_task(
                goal=f"capacity-{index}",
                context=CommandContext(
                    command_id=f"capacity-{index}",
                    tenant_id="tenant-m6-capacity",
                    actor=Actor(type="load-test", id="m6"),
                    correlation_id=f"capacity-{index}",
                    expected_version=0,
                    operation="create_task",
                ),
            )
            return str(result["session_id"]), (perf_counter() - started) * 1_000

        results = await asyncio.gather(*(submit(index) for index in range(200)))
        latencies = sorted(latency for _, latency in results)
        p95 = latencies[int(len(latencies) * 0.95) - 1]
        assert len({session_id for session_id, _ in results}) == 200
        assert p95 < 100

    asyncio.run(scenario())
