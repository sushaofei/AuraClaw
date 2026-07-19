from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

import asyncpg  # type: ignore[import-untyped]

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent, utc_now
from auraclaw.contracts.state import Visibility
from auraclaw.contracts.tools import ApprovalRecord, ApprovalStatus, RiskLevel
from auraclaw.domain.collaboration import CollaborationAggregate
from auraclaw.domain.ports import AppendResult, SessionSnapshot
from auraclaw.infrastructure.memory import DELIVERY_TRIGGER_EVENTS
from auraclaw.projections.approvals import APPROVAL_EVENTS
from auraclaw.projections.collaboration import COLLABORATION_EVENTS
from auraclaw.projections.tasks import (
    KNOWN_TASK_EVENTS,
    InMemoryTaskProjection,
    ProjectionGapError,
    UnsupportedEventError,
)


def _asyncpg_url(database_url: str) -> str:
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _event_from_record(row: asyncpg.Record) -> CanonicalEvent:
    actor = _decode_json(row["actor"])
    return CanonicalEvent(
        event_id=str(row["event_id"]),
        tenant_id=str(row["tenant_id"]),
        root_session_id=str(row["root_session_id"]),
        session_id=str(row["session_id"]),
        run_id=str(row["run_id"]) if row["run_id"] is not None else None,
        aggregate_version=int(row["aggregate_version"]),
        type=str(row["event_type"]),
        occurred_at=row["occurred_at"],
        actor=Actor(type=str(actor["type"]), id=str(actor["id"])),
        correlation_id=str(row["correlation_id"]),
        causation_id=str(row["causation_id"]),
        visibility=Visibility(str(row["visibility"])),
        schema_version=int(row["schema_version"]),
        payload=dict(_decode_json(row["payload"])),
    )


class _LazyPool:
    def __init__(self, database_url: str) -> None:
        self._database_url = _asyncpg_url(database_url)
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    self._pool = await asyncpg.create_pool(
                        self._database_url,
                        min_size=1,
                        max_size=5,
                        command_timeout=30,
                    )
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None


@dataclass
class PostgresOutboxRecord:
    outbox_id: int
    event_id: str
    event: CanonicalEvent


class PostgresEventStore(_LazyPool):
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
        return [_event_from_record(row) for row in rows]

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
        return [_event_from_record(row) for row in rows]

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
            state=dict(_decode_json(row["state"])),
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
            _json(snapshot.state),
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
                    command_result=dict(_decode_json(previous["response"])),
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
                    causation_id=context.command_id,
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
                    _json({"type": event.actor.type, "id": event.actor.id}),
                    event.correlation_id,
                    event.causation_id,
                    event.visibility.value,
                    event.schema_version,
                    _json(event.payload),
                )
                await connection.execute(
                    """INSERT INTO session_core.outbox (event_id, destination, payload)
                    VALUES ($1, 'projection', $2::jsonb)""",
                    event.event_id,
                    _json(event.as_dict()),
                )
                if event.type in DELIVERY_TRIGGER_EVENTS:
                    await connection.execute(
                        """INSERT INTO session_core.outbox (event_id, destination, payload)
                        VALUES ($1, 'delivery', $2::jsonb)""",
                        event.event_id,
                        _json(event.as_dict()),
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
                _json(command_result),
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
                event=_event_from_record(row),
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
                event=_event_from_record(row),
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


class PostgresTaskProjection(_LazyPool):
    """Disposable Task read model with atomic checkpoint and event dedup."""

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        pool = await self.pool()
        for event in events:
            if event.type not in KNOWN_TASK_EVENTS:
                await pool.execute(
                    """INSERT INTO projection.poison_event
                    (projector_id, event_id, tenant_id, session_id, reason, payload)
                    VALUES ('task', $1, $2, $3, $4, $5::jsonb)
                    ON CONFLICT (projector_id, event_id) DO NOTHING""",
                    event.event_id,
                    event.tenant_id,
                    event.session_id,
                    f"unsupported canonical event: {event.type}",
                    _json(event.as_dict()),
                )
                raise UnsupportedEventError(f"unsupported canonical event: {event.type}")
            async with pool.acquire() as connection, connection.transaction():
                inserted = await connection.fetchval(
                    """INSERT INTO projection.processed_event (projector_id, event_id)
                    VALUES ('task', $1) ON CONFLICT DO NOTHING RETURNING event_id""",
                    event.event_id,
                )
                if inserted is None:
                    continue
                row = await connection.fetchrow(
                    """SELECT * FROM projection.task_view
                    WHERE tenant_id = $1 AND session_id = $2 FOR UPDATE""",
                    event.tenant_id,
                    event.session_id,
                )
                view = dict(row) if row is not None else InMemoryTaskProjection._new_view(event)
                if row is not None:
                    view["projection_version"] = int(row["source_version"])
                    view["result_ref"] = _decode_json(row["result_ref"])
                    view["artifact_refs"] = _decode_json(row["artifact_refs"])
                    view["error"] = _decode_json(row["error"])
                current_version = int(view["projection_version"])
                if event.aggregate_version != current_version + 1:
                    raise ProjectionGapError(
                        f"projection gap for {event.session_id}: "
                        f"expected {current_version + 1}, got {event.aggregate_version}"
                    )
                InMemoryTaskProjection._apply(view, event)
                view["projection_version"] = event.aggregate_version
                view["projected_at"] = event.occurred_at
                await connection.execute(
                    """INSERT INTO projection.task_view
                    (tenant_id, session_id, root_session_id, run_id, status, goal, role,
                     parent_session_id, progress, current_stage, result_summary, result_ref,
                     artifact_refs, error, delivery_status, delivery_id,
                     delivery_attempt_count, delivery_response_summary,
                     source_version, source_event_id, projected_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,
                            $14::jsonb,$15,$16,$17,$18,$19,$20,$21)
                    ON CONFLICT (tenant_id, session_id) DO UPDATE SET
                      run_id=EXCLUDED.run_id, status=EXCLUDED.status, goal=EXCLUDED.goal,
                      role=EXCLUDED.role, parent_session_id=EXCLUDED.parent_session_id,
                      progress=EXCLUDED.progress, current_stage=EXCLUDED.current_stage,
                      result_summary=EXCLUDED.result_summary, result_ref=EXCLUDED.result_ref,
                      artifact_refs=EXCLUDED.artifact_refs, error=EXCLUDED.error,
                      delivery_status=EXCLUDED.delivery_status,
                      delivery_id=EXCLUDED.delivery_id,
                      delivery_attempt_count=EXCLUDED.delivery_attempt_count,
                      delivery_response_summary=EXCLUDED.delivery_response_summary,
                      source_version=EXCLUDED.source_version,
                      source_event_id=EXCLUDED.source_event_id,
                      projected_at=EXCLUDED.projected_at""",
                    event.tenant_id,
                    event.session_id,
                    event.root_session_id,
                    view.get("run_id"),
                    view["status"],
                    view.get("goal", ""),
                    view.get("role", "root"),
                    view.get("parent_session_id"),
                    view["progress"],
                    view["current_stage"],
                    view.get("result_summary"),
                    _json(view.get("result_ref")),
                    _json(view.get("artifact_refs", [])),
                    _json(view.get("error")),
                    view.get("delivery_status"),
                    view.get("delivery_id"),
                    view.get("delivery_attempt_count", 0),
                    view.get("delivery_response_summary"),
                    event.aggregate_version,
                    event.event_id,
                    event.occurred_at,
                )
                await connection.execute(
                    """INSERT INTO projection.projector_checkpoint
                    (projector_id, partition_id, checkpoint)
                    VALUES ('task', $1, $2::jsonb)
                    ON CONFLICT (projector_id, partition_id) DO UPDATE SET
                      checkpoint=EXCLUDED.checkpoint, updated_at=now()""",
                    f"{event.tenant_id}:{event.session_id}",
                    _json({"version": event.aggregate_version, "event_id": event.event_id}),
                )

    async def get_task(self, tenant_id: str, session_id: str) -> dict[str, Any] | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            "SELECT * FROM projection.task_view WHERE tenant_id = $1 AND session_id = $2",
            tenant_id,
            session_id,
        )
        if row is None:
            return None
        return {
            "tenant_id": str(row["tenant_id"]),
            "session_id": str(row["session_id"]),
            "root_session_id": str(row["root_session_id"]),
            "run_id": str(row["run_id"]) if row["run_id"] is not None else None,
            "status": str(row["status"]),
            "goal": str(row["goal"]),
            "role": str(row["role"]),
            "parent_session_id": row["parent_session_id"],
            "progress": float(row["progress"]),
            "current_stage": str(row["current_stage"]),
            "result_summary": row["result_summary"],
            "result_ref": _decode_json(row["result_ref"]),
            "artifact_refs": list(_decode_json(row["artifact_refs"])),
            "error": _decode_json(row["error"]),
            "delivery_status": row["delivery_status"],
            "delivery_id": row["delivery_id"],
            "delivery_attempt_count": int(row["delivery_attempt_count"]),
            "delivery_response_summary": row["delivery_response_summary"],
            "projection_version": int(row["source_version"]),
            "projected_at": row["projected_at"].isoformat(),
        }

    async def rebuild(self, events: Sequence[CanonicalEvent], tenant_id: str | None = None) -> int:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            if tenant_id is None:
                await connection.execute("DELETE FROM projection.task_view")
                await connection.execute(
                    "DELETE FROM projection.processed_event WHERE projector_id = 'task'"
                )
                await connection.execute(
                    "DELETE FROM projection.projector_checkpoint WHERE projector_id = 'task'"
                )
            else:
                event_ids = [event.event_id for event in events if event.tenant_id == tenant_id]
                await connection.execute(
                    "DELETE FROM projection.task_view WHERE tenant_id = $1", tenant_id
                )
                await connection.execute(
                    """DELETE FROM projection.projector_checkpoint
                    WHERE projector_id = 'task' AND partition_id LIKE $1""",
                    f"{tenant_id}:%",
                )
                if event_ids:
                    await connection.execute(
                        """DELETE FROM projection.processed_event
                        WHERE projector_id = 'task' AND event_id = ANY($1::text[])""",
                        event_ids,
                    )
        selected = [event for event in events if tenant_id is None or event.tenant_id == tenant_id]
        await self.project(selected)
        return len(selected)


class PostgresCollaborationProjection(_LazyPool):
    """Rebuildable Child DAG and review view derived from Canonical Events."""

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        pool = await self.pool()
        for event in events:
            if event.type not in COLLABORATION_EVENTS:
                continue
            async with pool.acquire() as connection, connection.transaction():
                inserted = await connection.fetchval(
                    """INSERT INTO projection.processed_event (projector_id, event_id)
                    VALUES ('collaboration', $1)
                    ON CONFLICT DO NOTHING RETURNING event_id""",
                    event.event_id,
                )
                if inserted is None:
                    continue
                rows = await connection.fetch(
                    """SELECT * FROM session_core.canonical_event
                    WHERE tenant_id=$1 AND root_session_id=$2
                    ORDER BY occurred_at, session_id, aggregate_version""",
                    event.tenant_id,
                    event.root_session_id,
                )
                canonical = [_event_from_record(row) for row in rows]
                graph = CollaborationAggregate.from_events(
                    event.tenant_id, event.root_session_id, canonical
                )
                latest = {
                    item.session_id: item
                    for item in canonical
                    if item.type in COLLABORATION_EVENTS
                }
                await connection.execute(
                    """DELETE FROM projection.collaboration_view
                    WHERE tenant_id=$1 AND root_session_id=$2""",
                    event.tenant_id,
                    event.root_session_id,
                )
                for node in graph.nodes.values():
                    source = latest[node.session_id]
                    result = node.result or {}
                    await connection.execute(
                        """INSERT INTO projection.collaboration_view
                        (tenant_id, root_session_id, session_id, run_id, parent_session_id, role,
                         task_key, goal, dependency_ids, owner, status, runnable,
                         output_contract, budget, result_ref, artifact_refs, target_session_id,
                         review_decision, evidence_refs, source_version, source_event_id,
                         projected_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13::jsonb,$14,
                                $15,$16::jsonb,$17,$18,$19::jsonb,$20,$21,$22)""",
                        node.tenant_id,
                        node.root_session_id,
                        node.session_id,
                        node.run_id,
                        node.parent_session_id,
                        node.role.value,
                        node.task_key,
                        node.goal,
                        _json(node.dependency_ids),
                        node.owner,
                        node.status,
                        node.status == "runnable",
                        _json(node.output_contract.as_dict()),
                        node.budget,
                        result.get("result_ref"),
                        _json(result.get("artifact_refs", [])),
                        node.target_session_id,
                        result.get("decision"),
                        _json(result.get("evidence_refs", [])),
                        source.aggregate_version,
                        source.event_id,
                        source.occurred_at,
                    )

    async def get(self, tenant_id: str, session_id: str) -> dict[str, Any] | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM projection.collaboration_view
            WHERE tenant_id=$1 AND session_id=$2""",
            tenant_id,
            session_id,
        )
        return self._view(row) if row is not None else None

    async def list_children(
        self, tenant_id: str, root_session_id: str
    ) -> list[dict[str, Any]]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM projection.collaboration_view
            WHERE tenant_id=$1 AND root_session_id=$2 AND session_id<>root_session_id
            ORDER BY projected_at, session_id""",
            tenant_id,
            root_session_id,
        )
        return [self._view(row) for row in rows]

    async def list_runnable(
        self, tenant_id: str, root_session_id: str
    ) -> list[dict[str, Any]]:
        return [
            view
            for view in await self.list_children(tenant_id, root_session_id)
            if view["runnable"]
        ]

    async def rebuild(
        self, events: Sequence[CanonicalEvent], tenant_id: str | None = None
    ) -> int:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            if tenant_id is None:
                await connection.execute("DELETE FROM projection.collaboration_view")
                await connection.execute(
                    """DELETE FROM projection.processed_event
                    WHERE projector_id='collaboration'"""
                )
            else:
                event_ids = [event.event_id for event in events if event.tenant_id == tenant_id]
                await connection.execute(
                    "DELETE FROM projection.collaboration_view WHERE tenant_id=$1", tenant_id
                )
                if event_ids:
                    await connection.execute(
                        """DELETE FROM projection.processed_event
                        WHERE projector_id='collaboration' AND event_id=ANY($1::text[])""",
                        event_ids,
                    )
        selected = [event for event in events if tenant_id is None or event.tenant_id == tenant_id]
        await self.project(selected)
        return len(selected)

    @staticmethod
    def _view(row: asyncpg.Record) -> dict[str, Any]:
        return {
            "tenant_id": str(row["tenant_id"]),
            "root_session_id": str(row["root_session_id"]),
            "session_id": str(row["session_id"]),
            "run_id": str(row["run_id"]) if row["run_id"] is not None else None,
            "parent_session_id": row["parent_session_id"],
            "role": str(row["role"]),
            "task_key": str(row["task_key"]),
            "goal": str(row["goal"]),
            "dependency_ids": list(_decode_json(row["dependency_ids"])),
            "owner": row["owner"],
            "status": str(row["status"]),
            "runnable": bool(row["runnable"]),
            "output_contract": dict(_decode_json(row["output_contract"])),
            "budget": float(row["budget"]),
            "result_ref": row["result_ref"],
            "artifact_refs": list(_decode_json(row["artifact_refs"])),
            "target_session_id": row["target_session_id"],
            "review_decision": row["review_decision"],
            "evidence_refs": list(_decode_json(row["evidence_refs"])),
            "projection_version": int(row["source_version"]),
            "projected_at": row["projected_at"].isoformat(),
        }


class PostgresApprovalProjection(_LazyPool):
    """Rebuildable Approval view; Canonical Session Events remain the fact source."""

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        pool = await self.pool()
        for event in events:
            if event.type not in APPROVAL_EVENTS or event.type == "human.response.recorded":
                continue
            async with pool.acquire() as connection, connection.transaction():
                inserted = await connection.fetchval(
                    """INSERT INTO projection.processed_event (projector_id, event_id)
                    VALUES ('approval', $1) ON CONFLICT DO NOTHING RETURNING event_id""",
                    event.event_id,
                )
                if inserted is None:
                    continue
                payload = event.payload
                if event.type == "approval.requested":
                    await connection.execute(
                        """INSERT INTO projection.approval_view
                        (tenant_id, approval_id, session_id, run_id, action_digest,
                         tool_name, redacted_arguments, risk, reason, expected_effect,
                         allowed_decisions, assigned_approvers, policy_version, expires_at,
                         status, source_version, source_event_id, projected_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11::jsonb,
                                $12::jsonb,$13,$14,$15,$16,$17,$18)
                        ON CONFLICT (tenant_id, approval_id) DO NOTHING""",
                        event.tenant_id,
                        str(payload["approval_id"]),
                        event.session_id,
                        str(payload.get("run_id") or event.run_id or ""),
                        str(payload["action_digest"]),
                        str(payload["tool_name"]),
                        _json(payload.get("redacted_arguments", {})),
                        str(payload["risk"]),
                        str(payload.get("reason", "")),
                        str(payload.get("expected_effect", "")),
                        _json(payload.get("allowed_decisions", [])),
                        _json(payload.get("assigned_approvers", [])),
                        str(payload["policy_version"]),
                        datetime.fromisoformat(str(payload["expires_at"])),
                        str(payload.get("status", "waiting")),
                        event.aggregate_version,
                        event.event_id,
                        event.occurred_at,
                    )
                else:
                    status = event.type.split(".", 1)[1]
                    await connection.execute(
                        """UPDATE projection.approval_view
                        SET status=$3, decision=$4, feedback=$5, source_version=$6,
                            source_event_id=$7, projected_at=$8
                        WHERE tenant_id=$1 AND approval_id=$2""",
                        event.tenant_id,
                        str(payload["approval_id"]),
                        status,
                        payload.get("decision"),
                        payload.get("feedback"),
                        event.aggregate_version,
                        event.event_id,
                        event.occurred_at,
                    )

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM projection.approval_view
            WHERE tenant_id=$1 AND approval_id=$2""",
            tenant_id,
            approval_id,
        )
        return self._record(row) if row is not None else None

    async def find_approved(
        self, tenant_id: str, session_id: str, digest: str, policy_version: str
    ) -> ApprovalRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM projection.approval_view
            WHERE tenant_id=$1 AND session_id=$2 AND action_digest=$3
              AND policy_version=$4 AND status='approved' AND expires_at > now()
            ORDER BY projected_at DESC LIMIT 1""",
            tenant_id,
            session_id,
            digest,
            policy_version,
        )
        return self._record(row) if row is not None else None

    @staticmethod
    def _record(row: asyncpg.Record) -> ApprovalRecord:
        return ApprovalRecord(
            approval_id=str(row["approval_id"]),
            tenant_id=str(row["tenant_id"]),
            session_id=str(row["session_id"]),
            run_id=str(row["run_id"]),
            action_digest=str(row["action_digest"]),
            tool_name=str(row["tool_name"]),
            redacted_arguments=dict(_decode_json(row["redacted_arguments"])),
            risk=RiskLevel(str(row["risk"])),
            reason=str(row["reason"]),
            expected_effect=str(row["expected_effect"]),
            allowed_decisions=tuple(_decode_json(row["allowed_decisions"])),
            assigned_approvers=tuple(_decode_json(row["assigned_approvers"])),
            policy_version=str(row["policy_version"]),
            expires_at=row["expires_at"],
            status=ApprovalStatus(str(row["status"])),
            decision=row["decision"],
            feedback=row["feedback"],
        )
