from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.infrastructure.persistence.postgres_common import LazyPool, json_dumps, json_loads
from auraclaw.projection.activity_view import activity_node_id, fold_activity_event


class PostgresActivityProjection(LazyPool):
    """Indexed, rebuildable Activity cache derived from Canonical Events."""

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        pool = await self.pool()
        for event in events:
            async with pool.acquire() as connection, connection.transaction():
                inserted = await connection.fetchval(
                    """INSERT INTO projection.processed_event (projector_id,event_id)
                    VALUES ('activity',$1) ON CONFLICT DO NOTHING RETURNING event_id""",
                    event.event_id,
                )
                if inserted is None:
                    continue
                state = await connection.fetchrow(
                    """SELECT source_version,complete FROM projection.activity_state
                    WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE""",
                    event.tenant_id,
                    event.session_id,
                )
                current = int(state["source_version"]) if state is not None else 0
                complete = (
                    bool(state["complete"])
                    if state is not None
                    else event.aggregate_version == 1
                )
                if complete and event.aggregate_version != current + 1:
                    raise ValueError(
                        f"activity projection gap for {event.session_id}: "
                        f"expected {current + 1}, got {event.aggregate_version}"
                    )
                if event.aggregate_version > current:
                    node_id = activity_node_id(event)
                    if node_id is not None:
                        existing = await connection.fetchval(
                            """SELECT node FROM projection.activity_node
                            WHERE tenant_id=$1 AND session_id=$2 AND node_id=$3""",
                            event.tenant_id,
                            event.session_id,
                            node_id,
                        )
                        node = fold_activity_event(
                            event,
                            dict(json_loads(existing)) if existing is not None else None,
                        )
                        if node is not None:
                            await connection.execute(
                                """INSERT INTO projection.activity_node
                                (tenant_id,session_id,node_id,sequence,updated_version,node,projected_at)
                                VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)
                                ON CONFLICT (tenant_id,session_id,node_id) DO UPDATE SET
                                  sequence=EXCLUDED.sequence,
                                  updated_version=EXCLUDED.updated_version,
                                  node=EXCLUDED.node,
                                  projected_at=EXCLUDED.projected_at""",
                                event.tenant_id,
                                event.session_id,
                                node_id,
                                int(node["sequence"]),
                                int(node["updated_version"]),
                                json_dumps(node),
                                event.occurred_at,
                            )
                    await connection.execute(
                        """INSERT INTO projection.activity_state
                        (tenant_id,session_id,source_version,source_event_id,complete,projected_at)
                        VALUES ($1,$2,$3,$4,$5,$6)
                        ON CONFLICT (tenant_id,session_id) DO UPDATE SET
                          source_version=EXCLUDED.source_version,
                          source_event_id=EXCLUDED.source_event_id,
                          complete=projection.activity_state.complete AND EXCLUDED.complete,
                          projected_at=EXCLUDED.projected_at""",
                        event.tenant_id,
                        event.session_id,
                        event.aggregate_version,
                        event.event_id,
                        complete,
                        event.occurred_at,
                    )

    async def get_activity_page(
        self,
        tenant_id: str,
        session_id: str,
        *,
        after_version: int,
        limit: int,
        min_source_version: int,
    ) -> dict[str, Any] | None:
        pool = await self.pool()
        state = await pool.fetchrow(
            """SELECT source_version,complete FROM projection.activity_state
            WHERE tenant_id=$1 AND session_id=$2""",
            tenant_id,
            session_id,
        )
        if (
            state is None
            or not bool(state["complete"])
            or int(state["source_version"]) < min_source_version
        ):
            return None
        rows = await pool.fetch(
            """SELECT node FROM projection.activity_node
            WHERE tenant_id=$1 AND session_id=$2 AND updated_version > $3
            ORDER BY updated_version,sequence,node_id LIMIT $4""",
            tenant_id,
            session_id,
            after_version,
            limit + 1,
        )
        nodes = [dict(json_loads(row["node"])) for row in rows[:limit]]
        nodes.sort(key=lambda node: (int(node["sequence"]), str(node["id"])))
        return {
            "source_version": int(state["source_version"]),
            "nodes": nodes,
            "next_after_version": max(
                (int(node["updated_version"]) for node in nodes),
                default=after_version,
            ),
            "has_more": len(rows) > limit,
        }

    async def rebuild(
        self, events: Sequence[CanonicalEvent], tenant_id: str | None = None
    ) -> int:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            if tenant_id is None:
                await connection.execute("DELETE FROM projection.activity_node")
                await connection.execute("DELETE FROM projection.activity_state")
                await connection.execute(
                    "DELETE FROM projection.processed_event WHERE projector_id='activity'"
                )
            else:
                event_ids = [event.event_id for event in events if event.tenant_id == tenant_id]
                await connection.execute(
                    "DELETE FROM projection.activity_node WHERE tenant_id=$1", tenant_id
                )
                await connection.execute(
                    "DELETE FROM projection.activity_state WHERE tenant_id=$1", tenant_id
                )
                if event_ids:
                    await connection.execute(
                        """DELETE FROM projection.processed_event
                        WHERE projector_id='activity' AND event_id=ANY($1::text[])""",
                        event_ids,
                    )
        selected = [event for event in events if tenant_id is None or event.tenant_id == tenant_id]
        await self.project(selected)
        return len(selected)
