import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.observability import TraceContext
from auraclaw.infrastructure.observability.stores import PostgresObservabilityStore
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url as _asyncpg_url
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.persistence.postgres_operations_store import PostgresOperationsStore
from auraclaw.observability.service import ObservabilityService

SETTINGS = get_settings()
DATABASE_URL = _asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0007_m6_observability_reliability.sql").read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_postgres_observability_alert_retention_and_operations() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        try:
            if await connection.fetchval(
                "SELECT to_regclass('observability.trace_span')"
            ) is None:
                await connection.execute(MIGRATION)
        finally:
            await connection.close()

        tenant_id = f"tenant-m6-pg-{uuid4().hex}"
        session_id = f"session-m6-pg-{uuid4().hex}"
        trace_id = uuid4().hex
        context = TraceContext(
            trace_id=trace_id,
            span_id=uuid4().hex[:16],
            tenant_id=tenant_id,
            root_session_id=session_id,
            session_id=session_id,
            run_id="run-m6-pg",
            tool_invocation_id="tool-m6-pg",
            delivery_id="delivery-m6-pg",
            approval_id="approval-m6-pg",
        )
        events = PostgresEventStore(DATABASE_URL)
        store = PostgresObservabilityStore(DATABASE_URL)
        operations = PostgresOperationsStore(DATABASE_URL)
        service = ObservabilityService(store, events)
        try:
            await service.record_span(
                context=context,
                component="integration",
                operation="m6",
                started_at=datetime.now(UTC),
                status="ok",
                attributes={"password": "must-redact"},
            )
            await service.audit(
                context=context,
                action="delivery.redrive",
                outcome="accepted",
                actor_type="operator",
                actor_id="m6-test",
                metadata={"authorization": "Bearer must-redact"},
            )
            await service.metric("delivery.dlq.count", 1, context=context)
            await service.metric("model.ttft.seconds", 0.1, context=context)
            await service.metric("model.ttft.seconds", 0.3, context=context)
            records = await store.session_records(tenant_id, session_id)
            assert len(records["spans"]) == len(records["audits"]) == len(records["alerts"]) == 1
            serialized = repr(records)
            assert "must-redact" not in serialized
            summary = await operations.failure_queue_summary(tenant_id)
            assert summary.projection_poison == summary.delivery_dlq == 0
            metric_summaries = await store.metric_summary(
                tenant_id, window_hours=24
            )
            ttft = next(
                item for item in metric_summaries if item.name == "model.ttft.seconds"
            )
            assert ttft.count == 2
            assert ttft.p50 == pytest.approx(0.2)
            assert ttft.p95 == pytest.approx(0.29)

            connection = await asyncpg.connect(DATABASE_URL)
            try:
                await connection.execute(
                    """UPDATE observability.metric_point SET observed_at=$2
                    WHERE tenant_id=$1""",
                    tenant_id,
                    datetime.now(UTC) - timedelta(days=31),
                )
            finally:
                await connection.close()
            deleted = await operations.apply_retention()
            assert deleted["metric"] >= 1
        finally:
            connection = await asyncpg.connect(DATABASE_URL)
            try:
                for table in ("alert", "audit_event", "metric_point", "trace_span"):
                    await connection.execute(
                        f"DELETE FROM observability.{table} WHERE tenant_id=$1",  # noqa: S608
                        tenant_id,
                    )
            finally:
                await connection.close()
            await events.close()
            await store.close()
            await operations.close()

    asyncio.run(scenario())
