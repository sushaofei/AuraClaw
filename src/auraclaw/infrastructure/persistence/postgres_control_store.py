from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import Any
from uuid import uuid4

from auraclaw.contracts.errors import (
    FencingTokenError,
    LeaseConflictError,
)
from auraclaw.control.ports import (
    ClaimedAssignment,
    ClaimedRunnable,
    RunnableItem,
    RuntimeAssignment,
    RuntimeBudget,
    RuntimeCheckpoint,
    RuntimeInstance,
    RuntimeLease,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool as _LazyPool,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    json_dumps as _json,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    json_loads as _decode_json,
)


class PostgresControlStateStore(_LazyPool):
    """PostgreSQL control plane using conditional writes and row-level claims."""

    async def enqueue(self, item: RunnableItem) -> bool:
        pool = await self.pool()
        inserted = await pool.fetchval(
            """
            INSERT INTO control.runnable_item
              (task_id, tenant_id, root_session_id, session_id, run_id, source_version,
               priority, required_capability, queue_partition, status, role, deadline, budget)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,'queued',$10,$11,$12::jsonb)
            ON CONFLICT DO NOTHING RETURNING task_id
            """,
            item.task_id,
            item.tenant_id,
            item.root_session_id,
            item.session_id,
            item.run_id,
            item.source_version,
            item.priority,
            _json(item.required_capability),
            item.queue_partition,
            item.role,
            item.deadline,
            _json(
                {
                    "max_steps": item.budget.max_steps,
                    "max_output_tokens": item.budget.max_output_tokens,
                    "max_cost": item.budget.max_cost,
                }
            ),
        )
        return inserted is not None

    async def claim(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> list[ClaimedRunnable]:
        pool = await self.pool()
        rows = await pool.fetch(
            """
            WITH candidates AS (
              SELECT task_id FROM control.runnable_item
              WHERE available_at <= now()
                AND (status = 'queued'
                     OR (status = 'claimed' AND claim_expires_at <= now()))
              ORDER BY priority DESC, available_at, task_id
              FOR UPDATE SKIP LOCKED LIMIT $2
            )
            UPDATE control.runnable_item q
            SET status = 'claimed', claimed_by = $1, claim_token = $3,
                claim_expires_at = now() + $4::interval, attempt = attempt + 1
            FROM candidates c WHERE q.task_id = c.task_id
            RETURNING q.*
            """,
            worker_id,
            limit,
            uuid4().hex,
            claim_ttl,
        )
        return [
            ClaimedRunnable(
                item=self._item_from_row(row),
                claimed_by=worker_id,
                claim_token=str(row["claim_token"]),
                claim_expires_at=row["claim_expires_at"],
            )
            for row in rows
        ]

    async def reschedule(
        self,
        task_id: str,
        *,
        worker_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """UPDATE control.runnable_item
                SET status='queued', claimed_by=NULL, claim_token=NULL,
                    claim_expires_at=NULL, available_at=now()
                WHERE task_id=$1
                  AND ($2::text IS NULL OR (
                    claimed_by=$2 AND claim_token=$3 AND claim_expires_at > now()
                  ))""",
                task_id,
                worker_id,
                claim_token,
            )
            await connection.execute(
                """UPDATE control.assignment SET assignment_status='failed', completed_at=now()
                WHERE task_id=$1 AND assignment_status IN ('assigned','running')""",
                task_id,
            )

    async def acquire_lease(
        self, resource_id: str, owner: str, *, ttl: timedelta
    ) -> RuntimeLease | None:
        pool = await self.pool()
        lease_id = f"lea_{uuid4().hex}"
        row = await pool.fetchrow(
            """
            INSERT INTO control.runtime_lease
              (resource_id, lease_id, lease_owner, expires_at, fencing_token, lease_version)
            VALUES ($1,$2,$3,now() + $4::interval,1,1)
            ON CONFLICT (resource_id) DO UPDATE SET
              lease_id=EXCLUDED.lease_id,
              lease_owner=EXCLUDED.lease_owner,
              expires_at=EXCLUDED.expires_at,
              fencing_token=control.runtime_lease.fencing_token + 1,
              lease_version=control.runtime_lease.lease_version + 1
            WHERE control.runtime_lease.expires_at <= now()
            RETURNING *
            """,
            resource_id,
            lease_id,
            owner,
            ttl,
        )
        return self._lease_from_row(row) if row is not None else None

    async def renew_lease(self, lease: RuntimeLease, *, ttl: timedelta) -> RuntimeLease:
        pool = await self.pool()
        row = await pool.fetchrow(
            """
            UPDATE control.runtime_lease
            SET expires_at=now() + $5::interval, lease_version=lease_version + 1
            WHERE resource_id=$1 AND lease_id=$2 AND lease_owner=$3
              AND fencing_token=$4 AND expires_at > now()
            RETURNING *
            """,
            lease.resource_id,
            lease.lease_id,
            lease.owner,
            lease.fencing_token,
            ttl,
        )
        if row is None:
            raise LeaseConflictError(f"lease is no longer owned: {lease.resource_id}")
        return self._lease_from_row(row)

    async def release_lease(self, lease: RuntimeLease) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE control.runtime_lease SET expires_at=now()
            WHERE resource_id=$1 AND lease_id=$2 AND fencing_token=$3""",
            lease.resource_id,
            lease.lease_id,
            lease.fencing_token,
        )

    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None:
        pool = await self.pool()
        valid = await pool.fetchval(
            """SELECT EXISTS(SELECT 1 FROM control.runtime_lease
            WHERE resource_id=$1 AND fencing_token=$2 AND expires_at > now())""",
            resource_id,
            fencing_token,
        )
        if not valid:
            raise FencingTokenError(f"stale fencing token {fencing_token} for {resource_id}")

    async def assign(
        self, task_id: str, assignment: RuntimeAssignment, *, claim_token: str
    ) -> bool:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            claim_owner = await connection.fetchval(
                """SELECT claimed_by FROM control.runnable_item
                WHERE task_id=$1 AND status='claimed' AND claim_token=$2
                  AND claim_expires_at > now()
                FOR UPDATE""",
                task_id,
                claim_token,
            )
            lease_owner = await connection.fetchval(
                """SELECT lease_owner FROM control.runtime_lease
                WHERE lease_id=$1 AND fencing_token=$2 AND expires_at > now()""",
                assignment.lease_id,
                assignment.fencing_token,
            )
            if claim_owner is None or claim_owner != lease_owner:
                return False
            capacity = await connection.fetchval(
                """SELECT capacity FROM control.runtime_instance
                WHERE runtime_id=$1 AND status='ready'
                  AND last_heartbeat_at > now() - interval '30 seconds'
                FOR UPDATE""",
                assignment.runtime_id,
            )
            if capacity is None:
                return False
            active = await connection.fetchval(
                """SELECT count(*) FROM control.assignment
                WHERE runtime_id=$1 AND assignment_status IN ('assigned','running')""",
                assignment.runtime_id,
            )
            if int(active) >= int(capacity):
                return False
            inserted = await connection.fetchval(
                """
                INSERT INTO control.assignment
                  (task_id, tenant_id, root_session_id, session_id, run_id, runtime_id,
                   lease_id, assignment_status, assigned_at, deadline, fencing_token,
                   role, resource_profile)
                VALUES ($1,$2,$3,$4,$5,$6,$7,'assigned',now(),$8,$9,$10,$11::jsonb)
                ON CONFLICT (task_id) DO UPDATE SET
                  runtime_id=EXCLUDED.runtime_id, lease_id=EXCLUDED.lease_id,
                  assignment_status='assigned', assigned_at=now(), started_at=NULL,
                  completed_at=NULL, deadline=EXCLUDED.deadline,
                  fencing_token=EXCLUDED.fencing_token, role=EXCLUDED.role,
                  resource_profile=EXCLUDED.resource_profile
                WHERE control.assignment.assignment_status
                  IN ('expired','completed','failed')
                RETURNING task_id
                """,
                task_id,
                assignment.tenant_id,
                assignment.root_session_id,
                assignment.session_id,
                assignment.run_id,
                assignment.runtime_id,
                assignment.lease_id,
                assignment.deadline,
                assignment.fencing_token,
                assignment.role,
                _json(assignment.resource_profile),
            )
            if inserted is None:
                return False
            await connection.execute(
                """UPDATE control.runnable_item SET status='assigned',
                claim_token=NULL,claim_expires_at=NULL WHERE task_id=$1""",
                task_id,
            )
            return True

    async def get_assignment(self, task_id: str) -> RuntimeAssignment | None:
        pool = await self.pool()
        row = await pool.fetchrow("SELECT * FROM control.assignment WHERE task_id=$1", task_id)
        if row is None:
            return None
        budget_row = await pool.fetchval(
            "SELECT budget FROM control.runnable_item WHERE task_id=$1", task_id
        )
        budget = self._budget(_decode_json(budget_row) if budget_row is not None else {})
        return RuntimeAssignment(
            tenant_id=str(row["tenant_id"]),
            root_session_id=str(row["root_session_id"]),
            session_id=str(row["session_id"]),
            run_id=str(row["run_id"]),
            runtime_id=str(row["runtime_id"]),
            lease_id=str(row["lease_id"]),
            fencing_token=int(row["fencing_token"]),
            role=str(row["role"]),
            resource_profile=dict(_decode_json(row["resource_profile"])),
            deadline=row["deadline"],
            budget=budget,
        )

    async def select_runtime(self, item: RunnableItem) -> RuntimeInstance | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT runtime.*, count(assignment.task_id) AS active_count
            FROM control.runtime_instance runtime
            LEFT JOIN control.assignment assignment
              ON assignment.runtime_id=runtime.runtime_id
             AND assignment.assignment_status IN ('assigned','running')
            WHERE runtime.role=$1 AND runtime.status='ready'
              AND runtime.last_heartbeat_at > now() - interval '30 seconds'
              AND runtime.capabilities @> $2::jsonb
            GROUP BY runtime.runtime_id
            HAVING count(assignment.task_id) < runtime.capacity
            ORDER BY count(assignment.task_id),runtime.last_heartbeat_at DESC,runtime.runtime_id
            LIMIT 1""",
            item.role,
            _json(item.required_capability),
        )
        if row is None:
            return None
        return RuntimeInstance(
            runtime_id=str(row["runtime_id"]),
            runtime_type=str(row["runtime_type"]),
            role=str(row["role"]),
            node_id=str(row["node_id"]),
            capabilities=dict(_decode_json(row["capabilities"])),
            capacity=int(row["capacity"]),
        )

    async def claim_assignments(
        self, runtime_id: str, role: str, *, limit: int = 1
    ) -> list[ClaimedAssignment]:
        pool = await self.pool()
        rows = await pool.fetch(
            """WITH candidates AS (
                SELECT task_id FROM control.assignment
                WHERE runtime_id=$1 AND role=$2 AND assignment_status='assigned'
                ORDER BY assigned_at, task_id
                FOR UPDATE SKIP LOCKED LIMIT $3
            )
            UPDATE control.assignment a
            SET assignment_status='running', started_at=COALESCE(started_at, now())
            FROM candidates c WHERE a.task_id=c.task_id RETURNING a.task_id""",
            runtime_id,
            role,
            limit,
        )
        claimed: list[ClaimedAssignment] = []
        for row in rows:
            task_id = str(row["task_id"])
            assignment = await self.get_assignment(task_id)
            if assignment is not None:
                lease_expires_at = await pool.fetchval(
                    "SELECT expires_at FROM control.runtime_lease WHERE lease_id=$1",
                    assignment.lease_id,
                )
                assignment = replace(
                    assignment, lease_expires_at=lease_expires_at
                )
                claimed.append(
                    ClaimedAssignment(task_id=task_id, assignment=assignment)
                )
        return claimed

    async def finish_assignment(self, task_id: str, outcome: str) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            if outcome in {"completed", "failed", "cancelled"}:
                await connection.execute(
                    """DELETE FROM control.runtime_lease AS lease
                    USING control.assignment AS assignment
                    WHERE assignment.task_id=$1
                      AND lease.resource_id=(
                        'session:' || assignment.tenant_id || ':' || assignment.session_id
                      )
                      AND lease.lease_id=assignment.lease_id""",
                    task_id,
                )
            await connection.execute(
                """UPDATE control.assignment SET assignment_status=$2, completed_at=now()
                WHERE task_id=$1""",
                task_id,
                outcome,
            )
            await connection.execute(
                "UPDATE control.runnable_item SET status='acked' WHERE task_id=$1", task_id
            )

    async def register_runtime(self, instance: RuntimeInstance) -> None:
        pool = await self.pool()
        await pool.execute(
            """
            INSERT INTO control.runtime_instance
              (runtime_id,runtime_type,role,node_id,capabilities,status,capacity)
            VALUES ($1,$2,$3,$4,$5::jsonb,'ready',$6)
            ON CONFLICT (runtime_id) DO UPDATE SET status='ready',
              capabilities=EXCLUDED.capabilities, capacity=EXCLUDED.capacity,
              last_heartbeat_at=now()
            """,
            instance.runtime_id,
            instance.runtime_type,
            instance.role,
            instance.node_id,
            _json(instance.capabilities),
            instance.capacity,
        )

    async def heartbeat(self, runtime_id: str, fencing_token: int | None = None) -> None:
        pool = await self.pool()
        if fencing_token is not None:
            valid = await pool.fetchval(
                """SELECT EXISTS(SELECT 1 FROM control.assignment
                WHERE runtime_id=$1 AND fencing_token=$2
                  AND assignment_status IN ('assigned','running'))""",
                runtime_id,
                fencing_token,
            )
            if not valid:
                raise FencingTokenError(f"stale runtime heartbeat: {runtime_id}")
        updated = await pool.fetchval(
            """UPDATE control.runtime_instance SET last_heartbeat_at=now()
            WHERE runtime_id=$1 RETURNING runtime_id""",
            runtime_id,
        )
        if updated is None:
            raise LeaseConflictError(f"unknown runtime: {runtime_id}")

    async def reserve_capacity(self, scope: str, amount: int, *, limit: int) -> bool:
        if amount < 0:
            return False
        pool = await self.pool()
        row = await pool.fetchval(
            """
            INSERT INTO control.capacity_reservation (scope,reserved)
            SELECT $1,$2::integer WHERE $2::integer <= $3::integer
            ON CONFLICT (scope) DO UPDATE SET
              reserved=control.capacity_reservation.reserved + $2::integer, updated_at=now()
            WHERE control.capacity_reservation.reserved + $2::integer <= $3::integer
            RETURNING reserved
            """,
            scope,
            amount,
            limit,
        )
        return row is not None and int(row) <= limit

    async def release_capacity(self, scope: str, amount: int) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE control.capacity_reservation
            SET reserved=GREATEST(0,reserved-$2), updated_at=now() WHERE scope=$1""",
            scope,
            amount,
        )

    async def save_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None:
        pool = await self.pool()
        resource_id = f"session:{checkpoint.tenant_id}:{checkpoint.session_id}"
        saved = await pool.fetchval(
            """
            INSERT INTO control.runtime_checkpoint
              (tenant_id,session_id,run_id,fencing_token,phase,state,updated_at)
            SELECT $1,$2,$3,$4,$5,$6::jsonb,$7
            WHERE EXISTS(SELECT 1 FROM control.runtime_lease
              WHERE resource_id=$8 AND fencing_token=$4 AND expires_at > now())
            ON CONFLICT (tenant_id,session_id,run_id) DO UPDATE SET
              fencing_token=EXCLUDED.fencing_token, phase=EXCLUDED.phase,
              state=EXCLUDED.state, updated_at=EXCLUDED.updated_at
            WHERE control.runtime_checkpoint.fencing_token <= EXCLUDED.fencing_token
            RETURNING run_id
            """,
            checkpoint.tenant_id,
            checkpoint.session_id,
            checkpoint.run_id,
            checkpoint.fencing_token,
            checkpoint.phase,
            _json(checkpoint.state),
            checkpoint.updated_at,
            resource_id,
        )
        if saved is None:
            raise FencingTokenError("checkpoint rejected for stale Runtime")

    async def load_checkpoint(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> RuntimeCheckpoint | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM control.runtime_checkpoint
            WHERE tenant_id=$1 AND session_id=$2 AND run_id=$3""",
            tenant_id,
            session_id,
            run_id,
        )
        if row is None:
            return None
        return RuntimeCheckpoint(
            tenant_id=tenant_id,
            session_id=session_id,
            run_id=run_id,
            fencing_token=int(row["fencing_token"]),
            phase=str(row["phase"]),
            state=dict(_decode_json(row["state"])),
            updated_at=row["updated_at"],
        )

    async def request_cancel(self, tenant_id: str, session_id: str, run_id: str) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO control.runtime_cancellation (tenant_id,session_id,run_id)
            VALUES ($1,$2,$3) ON CONFLICT DO NOTHING""",
            tenant_id,
            session_id,
            run_id,
        )

    async def is_cancelled(self, tenant_id: str, session_id: str, run_id: str) -> bool:
        pool = await self.pool()
        return bool(
            await pool.fetchval(
                """SELECT EXISTS(SELECT 1 FROM control.runtime_cancellation
                WHERE tenant_id=$1 AND session_id=$2 AND run_id=$3)""",
                tenant_id,
                session_id,
                run_id,
            )
        )

    async def recover_expired(self) -> int:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                "SELECT resource_id FROM control.runtime_lease WHERE expires_at <= now()"
            )
            resources = [str(row["resource_id"]) for row in rows]
            if not resources:
                return 0
            repaired = await connection.fetch(
                """UPDATE control.assignment SET assignment_status='expired', completed_at=now()
                WHERE ('session:' || tenant_id || ':' || session_id) = ANY($1::text[])
                  AND assignment_status IN ('assigned','running') RETURNING task_id""",
                resources,
            )
            task_ids = [str(row["task_id"]) for row in repaired]
            if task_ids:
                await connection.execute(
                    """UPDATE control.runnable_item
                    SET status='queued', claimed_by=NULL, claim_token=NULL,
                        claim_expires_at=NULL, available_at=now()
                    WHERE task_id=ANY($1::text[])""",
                    task_ids,
                )
            recovered_claims = await connection.fetchval(
                """WITH recovered AS (
                    UPDATE control.runnable_item
                    SET status='queued',claimed_by=NULL,claim_token=NULL,
                        claim_expires_at=NULL,available_at=now()
                    WHERE status='claimed' AND claim_expires_at <= now()
                    RETURNING task_id
                ) SELECT count(*) FROM recovered"""
            )
            return len(task_ids) + int(recovered_claims or 0)

    @staticmethod
    def _lease_from_row(row: Any) -> RuntimeLease:
        return RuntimeLease(
            resource_id=str(row["resource_id"]),
            lease_id=str(row["lease_id"]),
            owner=str(row["lease_owner"]),
            fencing_token=int(row["fencing_token"]),
            expires_at=row["expires_at"],
        )

    @classmethod
    def _item_from_row(cls, row: Any) -> RunnableItem:
        return RunnableItem(
            task_id=str(row["task_id"]),
            tenant_id=str(row["tenant_id"]),
            root_session_id=str(row["root_session_id"]),
            session_id=str(row["session_id"]),
            run_id=str(row["run_id"]),
            source_version=int(row["source_version"]),
            priority=int(row["priority"]),
            queue_partition=str(row["queue_partition"]),
            role=str(row["role"]),
            required_capability=dict(_decode_json(row["required_capability"])),
            deadline=row["deadline"],
            budget=cls._budget(_decode_json(row["budget"])),
        )

    @staticmethod
    def _budget(data: Any) -> RuntimeBudget:
        values = dict(data or {})
        return RuntimeBudget(
            max_steps=int(values.get("max_steps", 16)),
            max_output_tokens=int(values.get("max_output_tokens", 8192)),
            max_cost=float(values["max_cost"]) if values.get("max_cost") is not None else None,
        )
