from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from auraclaw.action.skill_lifecycle_events import (
    SkillLifecycleSignal,
    SkillLifecycleSignalRecord,
    SkillLifecycleSignalStore,
)
from auraclaw.infrastructure.persistence.postgres_common import LazyPool


class PostgresSkillLifecycleSignalStore(LazyPool, SkillLifecycleSignalStore):
    async def enqueue(
        self,
        *,
        tenant_id: str,
        change_type: str,
        snapshot_digest: str | None,
        origin_replica: str,
    ) -> SkillLifecycleSignal:
        pool = await self.pool()
        occurred_at = datetime.now(UTC)
        event_id = f"sle_{uuid4().hex}"
        async with pool.acquire() as connection, connection.transaction():
            revision = await connection.fetchval(
                """INSERT INTO hands.skill_lifecycle_revision
                (tenant_id,revision,updated_at) VALUES ($1,1,$2)
                ON CONFLICT (tenant_id) DO UPDATE SET
                    revision=hands.skill_lifecycle_revision.revision+1,
                    updated_at=EXCLUDED.updated_at
                RETURNING revision""",
                tenant_id,
                occurred_at,
            )
            await connection.execute(
                """INSERT INTO hands.skill_lifecycle_broadcast_outbox
                (event_id,tenant_id,revision,change_type,snapshot_digest,
                 origin_replica,created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7)""",
                event_id,
                tenant_id,
                int(revision),
                change_type,
                snapshot_digest,
                origin_replica,
                occurred_at,
            )
        return SkillLifecycleSignal(
            event_id=event_id,
            tenant_id=tenant_id,
            revision=int(revision),
            change_type=change_type,
            snapshot_digest=snapshot_digest,
            origin_replica=origin_replica,
            occurred_at=occurred_at,
        )

    async def claim(
        self,
        *,
        owner: str,
        limit: int,
        claim_ttl: timedelta,
    ) -> tuple[SkillLifecycleSignalRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """UPDATE hands.skill_lifecycle_broadcast_outbox target SET
                claimed_by=$1,claim_expires_at=now()+$3::interval
            WHERE outbox_id IN (
                SELECT outbox_id
                FROM hands.skill_lifecycle_broadcast_outbox
                WHERE published_at IS NULL AND next_attempt_at <= now()
                  AND (claimed_by IS NULL OR claim_expires_at <= now())
                ORDER BY outbox_id FOR UPDATE SKIP LOCKED LIMIT $2
            )
            RETURNING *""",
            owner,
            limit,
            claim_ttl,
        )
        return tuple(
            _record(dict(row))
            for row in sorted(rows, key=lambda item: int(item["outbox_id"]))
        )

    async def complete(self, *, outbox_id: str, owner: str) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE hands.skill_lifecycle_broadcast_outbox SET
                published_at=now(),claimed_by=NULL,claim_expires_at=NULL,
                last_error=NULL
            WHERE outbox_id=$1 AND claimed_by=$2 AND published_at IS NULL""",
            int(outbox_id),
            owner,
        )
        return str(result) == "UPDATE 1"

    async def fail(
        self, *, outbox_id: str, owner: str, safe_error_code: str
    ) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE hands.skill_lifecycle_broadcast_outbox SET
                claimed_by=NULL,claim_expires_at=NULL,
                publish_attempt=publish_attempt+1,
                next_attempt_at=now()+interval '1 second',last_error=$3
            WHERE outbox_id=$1 AND claimed_by=$2 AND published_at IS NULL""",
            int(outbox_id),
            owner,
            safe_error_code[:128],
        )
        return str(result) == "UPDATE 1"


def _record(row: dict[str, object]) -> SkillLifecycleSignalRecord:
    return SkillLifecycleSignalRecord(
        outbox_id=str(row["outbox_id"]),
        signal=SkillLifecycleSignal(
            event_id=str(row["event_id"]),
            tenant_id=str(row["tenant_id"]),
            revision=int(str(row["revision"])),
            change_type=str(row["change_type"]),
            snapshot_digest=(
                None
                if row.get("snapshot_digest") is None
                else str(row["snapshot_digest"])
            ),
            origin_replica=str(row["origin_replica"]),
            occurred_at=row["created_at"],  # type: ignore[arg-type]
        ),
        attempt=int(str(row["publish_attempt"])),
    )
