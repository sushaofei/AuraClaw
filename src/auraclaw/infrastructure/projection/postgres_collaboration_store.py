from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.domain.collaboration import CollaborationAggregate
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    event_from_record,
    json_dumps,
    json_loads,
)
from auraclaw.projection.collaboration.projector import COLLABORATION_EVENTS


class PostgresCollaborationProjection(LazyPool):
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
                canonical = [event_from_record(row) for row in rows]
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
                    # The primary key is tenant/session while rebuild cleanup is root-scoped.
                    # Remove any stale row whose historical root metadata differs before insert.
                    await connection.execute(
                        """DELETE FROM projection.collaboration_view
                        WHERE tenant_id=$1 AND session_id=$2""",
                        node.tenant_id,
                        node.session_id,
                    )
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
                        json_dumps(node.dependency_ids),
                        node.owner,
                        node.status,
                        node.status == "runnable",
                        json_dumps(node.output_contract.as_dict()),
                        node.budget,
                        result.get("result_ref"),
                        json_dumps(result.get("artifact_refs", [])),
                        node.target_session_id,
                        result.get("decision"),
                        json_dumps(result.get("evidence_refs", [])),
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
            "dependency_ids": list(json_loads(row["dependency_ids"])),
            "owner": row["owner"],
            "status": str(row["status"]),
            "runnable": bool(row["runnable"]),
            "output_contract": dict(json_loads(row["output_contract"])),
            "budget": float(row["budget"]),
            "result_ref": row["result_ref"],
            "artifact_refs": list(json_loads(row["artifact_refs"])),
            "target_session_id": row["target_session_id"],
            "review_decision": row["review_decision"],
            "evidence_refs": list(json_loads(row["evidence_refs"])),
            "projection_version": int(row["source_version"]),
            "projected_at": row["projected_at"].isoformat(),
        }
