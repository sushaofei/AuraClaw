from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import CanonicalEvent, NewEvent, utc_now
from auraclaw.infrastructure.persistence.memory_event_store import DELIVERY_TRIGGER_EVENTS
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    event_from_record,
    json_dumps,
    json_loads,
)
from auraclaw.session.ports import AppendResult, SessionSnapshot


@dataclass
class PostgresOutboxRecord:
    outbox_id: int
    event_id: str
    event: CanonicalEvent


class PostgresEventStore(LazyPool):
    """PostgreSQL canonical log with command dedup and transactional outbox."""

    async def load(
        self, tenant_id: str, session_id: str, *, from_version: int = 1
    ) -> list[CanonicalEvent]:
        pool = await self.pool()
        rows = await pool.fetch(
            """
            SELECT * FROM session_core.canonical_event
            WHERE tenant_id = $1 AND session_id = $2 AND aggregate_version >= $3
            ORDER BY aggregate_version
            """,
            tenant_id,
            session_id,
            from_version,
        )
        return [event_from_record(row) for row in rows]

    async def load_all(self, tenant_id: str | None = None) -> list[CanonicalEvent]:
        pool = await self.pool()
        if tenant_id is None:
            rows = await pool.fetch(
                """SELECT * FROM session_core.canonical_event
                ORDER BY tenant_id, session_id, aggregate_version"""
            )
        else:
            rows = await pool.fetch(
                """SELECT * FROM session_core.canonical_event
                WHERE tenant_id = $1 ORDER BY session_id, aggregate_version""",
                tenant_id,
            )
        return [event_from_record(row) for row in rows]

    async def get_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM session_core.snapshot
            WHERE tenant_id = $1 AND session_id = $2
            ORDER BY aggregate_version DESC LIMIT 1""",
            tenant_id,
            session_id,
        )
        if row is None:
            return None
        return SessionSnapshot(
            tenant_id=tenant_id,
            session_id=session_id,
            aggregate_version=int(row["aggregate_version"]),
            schema_version=int(row["schema_version"]),
            state=dict(json_loads(row["state"])),
        )

    async def save_snapshot(self, snapshot: SessionSnapshot) -> None:
        pool = await self.pool()
        await pool.execute(
            """
            INSERT INTO session_core.snapshot
                (tenant_id, session_id, aggregate_version, schema_version, state)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT (tenant_id, session_id, aggregate_version) DO NOTHING
            """,
            snapshot.tenant_id,
            snapshot.session_id,
            snapshot.aggregate_version,
            snapshot.schema_version,
            json_dumps(snapshot.state),
        )

    async def append(
        self,
        *,
        root_session_id: str,
        session_id: str,
        run_id: str | None,
        context: CommandContext,
        events: Sequence[NewEvent],
        command_result: dict[str, Any],
    ) -> AppendResult:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            lock_key = f"{context.tenant_id}:{context.operation}:{context.command_id}"
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lock_key
            )
            previous = await connection.fetchrow(
                """SELECT response FROM session_core.command_dedup
                WHERE tenant_id = $1 AND operation = $2 AND command_id = $3""",
                context.tenant_id,
                context.operation,
                context.command_id,
            )
            if previous is not None:
                return AppendResult(
                    events=[],
                    command_result=dict(json_loads(previous["response"])),
                    deduplicated=True,
                )

            if context.expected_version == 0:
                await connection.execute(
                    """INSERT INTO session_core.session_head
                    (tenant_id, session_id, root_session_id, aggregate_version)
                    VALUES ($1, $2, $3, 0) ON CONFLICT DO NOTHING""",
                    context.tenant_id,
                    session_id,
                    root_session_id,
                )
            head = await connection.fetchrow(
                """SELECT aggregate_version FROM session_core.session_head
                WHERE tenant_id = $1 AND session_id = $2 FOR UPDATE""",
                context.tenant_id,
                session_id,
            )
            actual_version = int(head["aggregate_version"]) if head is not None else -1
            if actual_version != context.expected_version:
                raise VersionConflictError(
                    f"expected Session version {context.expected_version}, got {actual_version}"
                )

            canonical: list[CanonicalEvent] = []
            for offset, new_event in enumerate(events, start=1):
                event = CanonicalEvent(
                    event_id=f"evt_{uuid4().hex}",
                    tenant_id=context.tenant_id,
                    root_session_id=root_session_id,
                    session_id=session_id,
                    run_id=run_id,
                    aggregate_version=context.expected_version + offset,
                    type=new_event.type,
                    occurred_at=utc_now(),
                    actor=context.actor,
                    correlation_id=context.correlation_id,
                    causation_id=context.causation_id or context.command_id,
                    visibility=new_event.visibility,
                    schema_version=1,
                    payload=dict(new_event.payload),
                )
                canonical.append(event)
                await connection.execute(
                    """INSERT INTO session_core.canonical_event
                    (event_id, tenant_id, root_session_id, session_id, run_id,
                     aggregate_version, event_type, occurred_at, actor, correlation_id,
                     causation_id, visibility, schema_version, payload)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14::jsonb)""",
                    event.event_id,
                    event.tenant_id,
                    event.root_session_id,
                    event.session_id,
                    event.run_id,
                    event.aggregate_version,
                    event.type,
                    event.occurred_at,
                    json_dumps({"type": event.actor.type, "id": event.actor.id}),
                    event.correlation_id,
                    event.causation_id,
                    event.visibility.value,
                    event.schema_version,
                    json_dumps(event.payload),
                )
                await connection.execute(
                    """INSERT INTO session_core.outbox (event_id, destination, payload)
                    VALUES ($1, 'projection', $2::jsonb)""",
                    event.event_id,
                    json_dumps(event.as_dict()),
                )
                if event.type in DELIVERY_TRIGGER_EVENTS:
                    await connection.execute(
                        """INSERT INTO session_core.outbox (event_id, destination, payload)
                        VALUES ($1, 'delivery', $2::jsonb)""",
                        event.event_id,
                        json_dumps(event.as_dict()),
                    )

            new_version = context.expected_version + len(canonical)
            await connection.execute(
                """UPDATE session_core.session_head
                SET aggregate_version = $3, updated_at = now()
                WHERE tenant_id = $1 AND session_id = $2""",
                context.tenant_id,
                session_id,
                new_version,
            )
            await connection.execute(
                """INSERT INTO session_core.command_dedup
                (tenant_id, operation, command_id, response)
                VALUES ($1, $2, $3, $4::jsonb)""",
                context.tenant_id,
                context.operation,
                context.command_id,
                json_dumps(command_result),
            )
            return AppendResult(events=canonical, command_result=dict(command_result))

    async def pending_outbox(self) -> list[PostgresOutboxRecord]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT o.outbox_id, o.event_id, e.*
            FROM session_core.outbox o
            JOIN session_core.canonical_event e ON e.event_id = o.event_id
            WHERE o.destination = 'projection' AND o.published_at IS NULL
              AND o.next_attempt_at <= now()
            ORDER BY o.outbox_id LIMIT 100"""
        )
        return [
            PostgresOutboxRecord(
                outbox_id=int(row["outbox_id"]),
                event_id=str(row["event_id"]),
                event=event_from_record(row),
            )
            for row in rows
        ]

    async def pending_delivery_outbox(self) -> list[PostgresOutboxRecord]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT o.outbox_id, o.event_id, e.*
            FROM session_core.outbox o
            JOIN session_core.canonical_event e ON e.event_id = o.event_id
            WHERE o.destination = 'delivery' AND o.published_at IS NULL
              AND o.next_attempt_at <= now()
            ORDER BY o.outbox_id LIMIT 100"""
        )
        return [
            PostgresOutboxRecord(
                outbox_id=int(row["outbox_id"]),
                event_id=str(row["event_id"]),
                event=event_from_record(row),
            )
            for row in rows
        ]

    async def mark_outbox_published(self, outbox_id: int) -> None:
        pool = await self.pool()
        await pool.execute(
            "UPDATE session_core.outbox SET published_at = now() WHERE outbox_id = $1",
            outbox_id,
        )

    async def mark_outbox_failed(self, outbox_id: int) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE session_core.outbox SET publish_attempt = publish_attempt + 1,
            next_attempt_at = now() + interval '1 second' * LEAST(60, power(2, publish_attempt))
            WHERE outbox_id = $1""",
            outbox_id,
        )
