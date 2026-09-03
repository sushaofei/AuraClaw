from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from auraclaw.contracts.approval_mode import ApprovalConfiguration
from auraclaw.contracts.events import CanonicalEvent
from auraclaw.infrastructure.persistence.postgres_common import LazyPool, json_dumps, json_loads
from auraclaw.projection.task.projector import (
    KNOWN_TASK_EVENTS,
    InMemoryTaskProjection,
    ProjectionGapError,
    UnsupportedEventError,
)


class PostgresTaskProjection(LazyPool):
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
                    json_dumps(event.as_dict()),
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
                    view["result_ref"] = json_loads(row["result_ref"])
                    view["artifact_refs"] = json_loads(row["artifact_refs"])
                    view["error"] = json_loads(row["error"])
                    view["approval"] = json_loads(row["approval"])
                    view["skill_activations"] = json_loads(row["skill_activations"])
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
                    (tenant_id, session_id, root_session_id, run_id, status, goal, source,
                     schedule_id, occurrence_id, role,
                     parent_session_id, progress, current_stage, run_status,
                     result_summary, result_ref,
                     artifact_refs, error, delivery_status, delivery_id,
                     delivery_attempt_count, delivery_response_summary,
                     skill_activations, source_version, source_event_id, projected_at, approval)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,
                            $17::jsonb,$18::jsonb,$19,$20,$21,$22,$23::jsonb,$24,$25,$26,$27::jsonb)
                    ON CONFLICT (tenant_id, session_id) DO UPDATE SET
                      approval=EXCLUDED.approval,
                      run_id=EXCLUDED.run_id, status=EXCLUDED.status, goal=EXCLUDED.goal,
                      source=EXCLUDED.source, schedule_id=EXCLUDED.schedule_id,
                      occurrence_id=EXCLUDED.occurrence_id,
                      role=EXCLUDED.role, parent_session_id=EXCLUDED.parent_session_id,
                      progress=EXCLUDED.progress, current_stage=EXCLUDED.current_stage,
                      run_status=EXCLUDED.run_status,
                      result_summary=EXCLUDED.result_summary, result_ref=EXCLUDED.result_ref,
                      artifact_refs=EXCLUDED.artifact_refs, error=EXCLUDED.error,
                      delivery_status=EXCLUDED.delivery_status,
                      delivery_id=EXCLUDED.delivery_id,
                      delivery_attempt_count=EXCLUDED.delivery_attempt_count,
                      delivery_response_summary=EXCLUDED.delivery_response_summary,
                      skill_activations=EXCLUDED.skill_activations,
                      source_version=EXCLUDED.source_version,
                      source_event_id=EXCLUDED.source_event_id,
                      projected_at=EXCLUDED.projected_at""",
                    event.tenant_id,
                    event.session_id,
                    event.root_session_id,
                    view.get("run_id"),
                    view["status"],
                    view.get("goal", ""),
                    view.get("source", "chat"),
                    view.get("schedule_id"),
                    view.get("occurrence_id"),
                    view.get("role", "root"),
                    view.get("parent_session_id"),
                    view["progress"],
                    view["current_stage"],
                    view.get("run_status"),
                    view.get("result_summary"),
                    json_dumps(view.get("result_ref")),
                    json_dumps(view.get("artifact_refs", [])),
                    json_dumps(view.get("error")),
                    view.get("delivery_status"),
                    view.get("delivery_id"),
                    view.get("delivery_attempt_count", 0),
                    view.get("delivery_response_summary"),
                    json_dumps(view.get("skill_activations", [])),
                    event.aggregate_version,
                    event.event_id,
                    event.occurred_at,
                    json_dumps(view.get("approval", {})),
                )
                await connection.execute(
                    """INSERT INTO projection.projector_checkpoint
                    (projector_id, partition_id, checkpoint)
                    VALUES ('task', $1, $2::jsonb)
                    ON CONFLICT (projector_id, partition_id) DO UPDATE SET
                      checkpoint=EXCLUDED.checkpoint, updated_at=now()""",
                    f"{event.tenant_id}:{event.session_id}",
                    json_dumps({"version": event.aggregate_version, "event_id": event.event_id}),
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
        return self._task_from_row(row)

    async def list_tasks(
        self,
        tenant_id: str,
        *,
        kind: str | None = None,
        status: str | None = None,
        cursor: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        from datetime import datetime

        from auraclaw.projection.task.listing import (
            decode_task_cursor,
            encode_task_cursor,
            source_for_kind,
        )

        pool = await self.pool()
        source = source_for_kind(kind)
        clauses = ["tenant_id = $1", "role = 'root'"]
        args: list[Any] = [tenant_id]
        if source is not None:
            args.append(source)
            clauses.append(f"source = ${len(args)}")
        if status is not None:
            args.append(status)
            clauses.append(f"status = ${len(args)}")
        if cursor:
            projected_at, session_id = decode_task_cursor(cursor)
            args.append(datetime.fromisoformat(projected_at))
            args.append(session_id)
            clauses.append(f"(projected_at, session_id) < (${len(args) - 1}, ${len(args)})")
        args.append(limit + 1)
        sql = f"""
            SELECT * FROM projection.task_view
            WHERE {" AND ".join(clauses)}
            ORDER BY projected_at DESC, session_id DESC
            LIMIT ${len(args)}
        """
        rows = await pool.fetch(sql, *args)
        tasks = [self._task_from_row(row) for row in rows[:limit]]
        next_cursor = None
        if len(rows) > limit:
            last = tasks[-1]
            next_cursor = encode_task_cursor(
                projected_at=str(last["projected_at"]),
                session_id=str(last["session_id"]),
            )
        return {"tasks": tasks, "next_cursor": next_cursor}

    @staticmethod
    def _task_from_row(row: Any) -> dict[str, Any]:
        source = "chat"
        schedule_id = None
        occurrence_id = None
        try:
            raw_source = row["source"]
            source = str(raw_source) if raw_source is not None else "chat"
            schedule_id = row["schedule_id"]
            occurrence_id = row["occurrence_id"]
        except KeyError:
            pass
        return {
            **ApprovalConfiguration.model_validate(json_loads(row["approval"])).public_dict(),
            "tenant_id": str(row["tenant_id"]),
            "session_id": str(row["session_id"]),
            "root_session_id": str(row["root_session_id"]),
            "run_id": str(row["run_id"]) if row["run_id"] is not None else None,
            "status": str(row["status"]),
            "run_status": str(row["run_status"]) if row["run_status"] is not None else None,
            "goal": str(row["goal"]),
            "source": source,
            "schedule_id": schedule_id,
            "occurrence_id": occurrence_id,
            "role": str(row["role"]),
            "parent_session_id": row["parent_session_id"],
            "progress": float(row["progress"]),
            "current_stage": str(row["current_stage"]),
            "result_summary": row["result_summary"],
            "result_ref": json_loads(row["result_ref"]),
            "artifact_refs": list(json_loads(row["artifact_refs"])),
            "error": json_loads(row["error"]),
            "delivery_status": row["delivery_status"],
            "delivery_id": row["delivery_id"],
            "delivery_attempt_count": int(row["delivery_attempt_count"]),
            "delivery_response_summary": row["delivery_response_summary"],
            "skill_activations": list(json_loads(row["skill_activations"])),
            "projection_version": int(row["source_version"]),
            "projected_at": row["projected_at"].isoformat(),
        }

    async def redrive_poison(self, tenant_id: str, event_id: str) -> bool:
        pool = await self.pool()
        result: str = await pool.execute(
            """DELETE FROM projection.poison_event
            WHERE tenant_id=$1 AND event_id=$2""",
            tenant_id,
            event_id,
        )
        return result == "DELETE 1"

    async def poison_count(self, tenant_id: str | None = None) -> int:
        pool = await self.pool()
        return int(
            await pool.fetchval(
                """SELECT count(*) FROM projection.poison_event
                WHERE $1::text IS NULL OR tenant_id=$1""",
                tenant_id,
            )
        )

    async def session_keys(self, tenant_id: str | None = None) -> list[tuple[str, str]]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT tenant_id,session_id FROM projection.task_view
            WHERE $1::text IS NULL OR tenant_id=$1 ORDER BY tenant_id,session_id""",
            tenant_id,
        )
        return [(str(row["tenant_id"]), str(row["session_id"])) for row in rows]

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
