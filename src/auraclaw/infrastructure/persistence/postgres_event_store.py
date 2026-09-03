from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import CanonicalEvent, NewEvent, utc_now
from auraclaw.infrastructure.persistence.memory_event_store import (
    CONTROL_TRIGGER_EVENTS,
    DELIVERY_TRIGGER_EVENTS,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    event_from_record,
    json_dumps,
    json_loads,
)
from auraclaw.session.ports import AppendResult, ClaimedOutboxRecord, SessionSnapshot


@dataclass
class PostgresOutboxRecord:
    outbox_id: int
    event_id: str
    event: CanonicalEvent


class PostgresEventStore(LazyPool):
    """PostgreSQL canonical log with command dedup and transactional outbox."""

    async def load(
        self,
        tenant_id: str,
        session_id: str,
        *,
        from_version: int = 1,
        event_types: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[CanonicalEvent]:
        pool = await self.pool()
        params: list[Any] = [tenant_id, session_id, from_version]
        clauses = [
            "tenant_id = $1",
            "session_id = $2",
            "aggregate_version >= $3",
        ]
        if event_types is not None:
            params.append(list(event_types))
            clauses.append(f"event_type = ANY(${len(params)}::text[])")
        query = f"""
            SELECT * FROM session_core.canonical_event
            WHERE {" AND ".join(clauses)}
            ORDER BY aggregate_version
        """
        if limit is not None:
            params.append(limit)
            query += f" LIMIT ${len(params)}"
        rows = await pool.fetch(query, *params)
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

    async def has_skill_package_reference(self, tenant_id: str, package_digest: str) -> bool:
        pool = await self.pool()
        query = """SELECT EXISTS(SELECT 1 FROM session_core.canonical_event
            WHERE tenant_id=$1 AND event_type='skill.activated'
              AND (payload->>'package_digest'=$2
                OR payload#>>'{activation,binding,package_digest}'=$2))"""
        return bool(await pool.fetchval(query, tenant_id, package_digest))

    async def has_active_skill_reference(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        package_digest: str | None = None,
    ) -> bool:
        pool = await self.pool()
        query = """SELECT EXISTS(
            SELECT 1 FROM session_core.canonical_event a
            WHERE a.tenant_id=$1 AND a.event_type='skill.activated'
              AND (($4::text IS NULL AND (
                    (a.payload#>>'{activation,binding,publisher}'=$2
                     AND COALESCE(
                       a.payload#>>'{activation,binding,skill_name}',
                       a.payload#>>'{activation,binding,name}')=$3)
                    OR EXISTS(
                      SELECT 1 FROM jsonb_array_elements(COALESCE(
                        a.payload#>'{activation,binding,resolved_skills}',
                        '[]'::jsonb)) dependency
                      WHERE dependency->>'publisher'=$2
                        AND COALESCE(
                          dependency->>'skill_name',dependency->>'name')=$3)))
                   OR ($4::text IS NOT NULL AND (
                    a.payload->>'package_digest'=$4
                    OR a.payload#>>'{activation,binding,package_digest}'=$4
                    OR EXISTS(
                      SELECT 1 FROM jsonb_array_elements(COALESCE(
                        a.payload#>'{activation,binding,resolved_skills}',
                        '[]'::jsonb)) dependency
                      WHERE dependency->>'package_digest'=$4))))
              AND ((NOT EXISTS(
                SELECT 1 FROM session_core.canonical_event t
                WHERE t.tenant_id=a.tenant_id
                  AND t.session_id=a.session_id AND t.run_id=a.run_id
                  AND t.event_type IN ('run.completed','run.failed','run.cancelled'))
                AND NOT EXISTS(
                  SELECT 1 FROM session_core.canonical_event t
                  WHERE t.tenant_id=a.tenant_id AND t.session_id=a.session_id AND t.run_id=a.run_id
                    AND t.payload->>'skill_activation_id'=a.payload->>'skill_activation_id'
                    AND t.event_type IN ('skill.completed','skill.failed','skill.cancelled')))
                OR EXISTS(
                  SELECT 1 FROM session_core.canonical_event pending
                  WHERE pending.tenant_id=a.tenant_id AND pending.session_id=a.session_id
                    AND pending.run_id=a.run_id AND pending.event_type='skill.invocation.requested'
                    AND pending.payload->>'skill_activation_id'=a.payload->>'skill_activation_id'
                    AND NOT EXISTS(
                      SELECT 1 FROM session_core.canonical_event settled
                      WHERE settled.tenant_id=pending.tenant_id
                        AND settled.session_id=pending.session_id AND settled.run_id=pending.run_id
                        AND settled.event_type='skill.invocation.settled'
                        AND settled.aggregate_version>pending.aggregate_version
                        AND settled.payload->>'invocation_cycle' IS NOT DISTINCT FROM
                            pending.payload->>'invocation_cycle'
                        AND settled.payload->>'skill_activation_id'
                          =pending.payload->>'skill_activation_id'
                        AND settled.payload->>'tool_invocation_id'
                          =pending.payload->>'tool_invocation_id')))

        )"""
        return bool(await pool.fetchval(query, tenant_id, publisher, name, package_digest))

    async def load_root(
        self,
        tenant_id: str,
        root_session_id: str,
        *,
        event_types: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[CanonicalEvent]:
        pool = await self.pool()
        params: list[Any] = [tenant_id, root_session_id]
        clauses = ["tenant_id = $1", "root_session_id = $2"]
        if event_types is not None:
            params.append(list(event_types))
            clauses.append(f"event_type = ANY(${len(params)}::text[])")
        query = f"""
            SELECT * FROM session_core.canonical_event
            WHERE {" AND ".join(clauses)}
            ORDER BY occurred_at, session_id, aggregate_version
        """
        if limit is not None:
            params.append(limit)
            query += f" LIMIT ${len(params)}"
        rows = await pool.fetch(query, *params)
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
            try:
                previous = await connection.fetchrow(
                    """SELECT response FROM session_core.command_dedup
                    WHERE tenant_id = $1 AND operation = $2 AND command_id = $3""",
                    context.tenant_id,
                    context.operation,
                    context.command_id,
                )
                if previous is not None:
                    saved_response = dict(json_loads(previous["response"]))
                    if command_result.get("_request_fingerprint") != saved_response.get(
                        "_request_fingerprint"
                    ):
                        raise VersionConflictError("command was reused with a different request")
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
                    if event.type in CONTROL_TRIGGER_EVENTS:
                        await connection.execute(
                            """INSERT INTO session_core.outbox (event_id, destination, payload)
                            VALUES ($1, 'control', $2::jsonb)""",
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
            finally:
                pass

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
            next_attempt_at = now() + interval '1 second' * LEAST(
                60, power(2, LEAST(publish_attempt, 6))
            )
            WHERE outbox_id = $1""",
            outbox_id,
        )

    async def claim_outbox(
        self,
        destination: str,
        worker_id: str,
        *,
        limit: int,
        claim_ttl: timedelta,
        wait_seconds: float = 0,
    ) -> list[ClaimedOutboxRecord]:
        deadline = time.monotonic() + max(0.0, wait_seconds)
        while True:
            claimed = await self._claim_outbox_once(
                destination, worker_id, limit=limit, claim_ttl=claim_ttl
            )
            if claimed or wait_seconds <= 0:
                return claimed
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            await asyncio.sleep(min(0.05, remaining))

    async def _claim_outbox_once(
        self,
        destination: str,
        worker_id: str,
        *,
        limit: int,
        claim_ttl: timedelta,
    ) -> list[ClaimedOutboxRecord]:
        pool = await self.pool()
        claimed: list[ClaimedOutboxRecord] = []
        async with pool.acquire() as connection, connection.transaction():
            lock_clause = "FOR UPDATE OF o SKIP LOCKED LIMIT $2"
            claim_ttl_sql = "claim_expires_at=now() + $4::interval"
            rows = await connection.fetch(
                f"""SELECT o.outbox_id, o.event_id, o.publish_attempt, e.*
                FROM session_core.outbox o
                JOIN session_core.canonical_event e ON e.event_id = o.event_id
                WHERE o.destination = $1 AND o.published_at IS NULL
                  AND o.poisoned_at IS NULL AND o.next_attempt_at <= now()
                  AND (o.claim_token IS NULL OR o.claim_expires_at <= now())
                  AND NOT EXISTS (
                    SELECT 1
                    FROM session_core.outbox earlier
                    JOIN session_core.canonical_event earlier_event
                      ON earlier_event.event_id = earlier.event_id
                    WHERE earlier.destination = o.destination
                      AND earlier.published_at IS NULL
                      AND earlier.outbox_id < o.outbox_id
                      AND earlier_event.tenant_id = e.tenant_id
                      AND earlier_event.session_id = e.session_id
                  )
                ORDER BY o.outbox_id
                {lock_clause}""",
                destination,
                limit,
            )
            for row in rows:
                token = f"clm_{uuid4().hex}"
                outbox_id = int(row["outbox_id"])
                await connection.execute(
                    f"""UPDATE session_core.outbox
                    SET claimed_by=$2, claim_token=$3,
                        {claim_ttl_sql},
                        publish_attempt=publish_attempt + 1
                    WHERE outbox_id=$1""",
                    outbox_id,
                    worker_id,
                    token,
                    claim_ttl,
                )
                claimed.append(
                    ClaimedOutboxRecord(
                        outbox_id=str(outbox_id),
                        event_id=str(row["event_id"]),
                        event=event_from_record(row),
                        claim_token=token,
                        attempt=int(row["publish_attempt"]) + 1,
                    )
                )
        return claimed

    async def disposition_outbox(
        self,
        destination: str,
        worker_id: str,
        outbox_id: str,
        claim_token: str,
        disposition: str,
        reason: str | None = None,
    ) -> bool:
        pool = await self.pool()
        assignments = {
            "ack": "published_at=now()",
            "nack": (
                "next_attempt_at=now() + interval '1 second' * "
                "LEAST(60, power(2, LEAST(publish_attempt, 6)))"
            ),
            "poison": "poisoned_at=now()",
        }
        selected = assignments.get(disposition)
        if selected is None:
            return False
        result = await pool.execute(
            f"""UPDATE session_core.outbox SET {selected}, last_error=$5,
                claimed_by=NULL, claim_token=NULL, claim_expires_at=NULL
            WHERE outbox_id=$1 AND destination=$2 AND claimed_by=$3
              AND claim_token=$4 AND claim_expires_at > now()
              AND published_at IS NULL AND poisoned_at IS NULL""",
            int(outbox_id),
            destination,
            worker_id,
            claim_token,
            None if disposition == "ack" else reason,
        )
        return str(result) == "UPDATE 1"
