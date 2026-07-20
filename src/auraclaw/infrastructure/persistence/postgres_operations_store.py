from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from auraclaw.infrastructure.persistence.postgres_common import LazyPool as _LazyPool


@dataclass(frozen=True)
class FailureQueueSummary:
    projection_outbox_pending: int
    projection_poison: int
    delivery_dlq: int


class PostgresOperationsStore(_LazyPool):
    async def failure_queue_summary(self, tenant_id: str | None = None) -> FailureQueueSummary:
        pool = await self.pool()
        return FailureQueueSummary(
            projection_outbox_pending=int(
                await pool.fetchval(
                    """SELECT count(*) FROM session_core.outbox WHERE destination='projection'
                    AND published_at IS NULL AND ($1::text IS NULL OR payload->>'tenant_id'=$1)""",
                    tenant_id,
                )
            ),
            projection_poison=int(
                await pool.fetchval(
                    """SELECT count(*) FROM projection.poison_event
                    WHERE $1::text IS NULL OR tenant_id=$1""",
                    tenant_id,
                )
            ),
            delivery_dlq=int(
                await pool.fetchval(
                    """SELECT count(*) FROM delivery.delivery_job
                    WHERE status='dead_lettered' AND ($1::text IS NULL OR tenant_id=$1)""",
                    tenant_id,
                )
            ),
        )

    async def redrive_projection_poison(self, tenant_id: str, event_id: str) -> bool:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            deleted = await connection.fetchrow(
                """DELETE FROM projection.poison_event WHERE tenant_id=$1 AND event_id=$2
                RETURNING event_id""",
                tenant_id,
                event_id,
            )
            if deleted is None:
                return False
            await connection.execute(
                """UPDATE session_core.outbox SET published_at=NULL,next_attempt_at=now(),
                last_error=NULL WHERE event_id=$1 AND destination='projection'""",
                event_id,
            )
            return True

    async def redrive_delivery(self, tenant_id: str, delivery_id: str) -> bool:
        pool = await self.pool()
        result: str = await pool.execute(
            """UPDATE delivery.delivery_job SET status='pending',next_attempt_at=now(),
            completed_at=NULL WHERE tenant_id=$1 AND delivery_id=$2
            AND status IN ('dead_lettered','failed')""",
            tenant_id,
            delivery_id,
        )
        return result == "UPDATE 1"

    async def apply_retention(self, *, now: datetime | None = None) -> dict[str, int]:
        cutoff = now or datetime.now(UTC)
        pool = await self.pool()
        policies = {
            str(row["data_class"]): int(row["retention_days"])
            for row in await pool.fetch("SELECT * FROM observability.retention_policy")
        }
        tables = {
            "metric": ("metric_point", "observed_at"),
            "trace": ("trace_span", "ended_at"),
            "audit": ("audit_event", "occurred_at"),
            "alert": ("alert", "fired_at"),
        }
        deleted: dict[str, int] = {}
        for data_class, (table, column) in tables.items():
            days = policies[data_class]
            result = await pool.execute(
                f"DELETE FROM observability.{table} WHERE {column} < $1",  # noqa: S608
                cutoff - timedelta(days=days),
            )
            deleted[data_class] = int(result.rsplit(" ", 1)[-1])
        return deleted
