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
    AGENT_RUNTIME_POOL,
    DEFAULT_RUNTIME_MAX_STEPS,
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

_USER_ID_PROFILE_KEY = "_auraclaw_user_id"
_DEPT_ID_PROFILE_KEY = "_auraclaw_dept_id"


def _profile_with_identity(
    profile: dict[str, Any], user_id: str | None, dept_id: str | None = None
) -> dict[str, Any]:
    encoded = dict(profile)
    if user_id:
        encoded[_USER_ID_PROFILE_KEY] = user_id
    else:
        encoded.pop(_USER_ID_PROFILE_KEY, None)
    if dept_id:
        encoded[_DEPT_ID_PROFILE_KEY] = dept_id
    else:
        encoded.pop(_DEPT_ID_PROFILE_KEY, None)
    return encoded


def _split_identity(profile: dict[str, Any]) -> tuple[dict[str, Any], str | None, str | None]:
    decoded = dict(profile)
    user_id = decoded.pop(_USER_ID_PROFILE_KEY, None)
    dept_id = decoded.pop(_DEPT_ID_PROFILE_KEY, None)
    return (
        decoded,
        None if user_id is None else str(user_id),
        None if dept_id is None else str(dept_id),
    )


class PostgresControlStateStore(_LazyPool):
    """PostgreSQL control plane using conditional writes and row-level claims."""

    async def enqueue(self, item: RunnableItem) -> bool:
        pool = await self.pool()
        query = """
            INSERT INTO control.runnable_item
              (task_id, tenant_id, root_session_id, session_id, run_id, source_version,
               priority, required_capability, queue_partition, status, role, deadline, budget)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,'queued',$10,$11,$12::jsonb)
            ON CONFLICT DO NOTHING
            """
        args = (
            item.task_id,
            item.tenant_id,
            item.root_session_id,
            item.session_id,
            item.run_id,
            item.source_version,
            item.priority,
            _json(_profile_with_identity(item.required_capability, item.user_id, item.dept_id)),
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
        inserted = await pool.fetchval(
            query + " RETURNING task_id",
            *args,
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
        delay: timedelta = timedelta(0),
    ) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """UPDATE control.runnable_item
                SET status='queued', claimed_by=NULL, claim_token=NULL,
                    claim_expires_at=NULL, available_at=now() + $4::interval
                WHERE task_id=$1
                  AND ($2::text IS NULL OR (
                    claimed_by=$2 AND claim_token=$3 AND claim_expires_at > now()
                  ))""",
                task_id,
                worker_id,
                claim_token,
                delay,
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
            if (
                claim_owner is None
                or lease_owner is None
                or str(claim_owner) != str(lease_owner)
            ):
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
                      IN ('expired','completed','failed','waiting_children',
                          'waiting_for_human','waiting_for_tool')
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
                    _json(
                        _profile_with_identity(
                            assignment.resource_profile,
                            assignment.user_id,
                            assignment.dept_id,
                        )
                    ),
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
        profile, user_id, dept_id = _split_identity(dict(_decode_json(row["resource_profile"])))
        return RuntimeAssignment(
            tenant_id=str(row["tenant_id"]),
            root_session_id=str(row["root_session_id"]),
            session_id=str(row["session_id"]),
            run_id=str(row["run_id"]),
            runtime_id=str(row["runtime_id"]),
            lease_id=str(row["lease_id"]),
            fencing_token=int(row["fencing_token"]),
            role=str(row["role"]),
            resource_profile=profile,
            deadline=row["deadline"],
            budget=budget,
            user_id=user_id,
            dept_id=dept_id,
            execution_claim_token=(
                None
                if row["execution_claim_token"] is None
                else str(row["execution_claim_token"])
            ),
            execution_claim_expires_at=row["execution_claim_expires_at"],
        )

    async def select_runtime(self, item: RunnableItem) -> RuntimeInstance | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT runtime.*, count(assignment.task_id) AS active_count
            FROM control.runtime_instance runtime
            LEFT JOIN control.assignment assignment
              ON assignment.runtime_id=runtime.runtime_id
             AND assignment.assignment_status IN ('assigned','running')
            WHERE runtime.role IN ($1, $2) AND runtime.status='ready'
              AND runtime.last_heartbeat_at > now() - interval '30 seconds'
              AND runtime.capabilities @> $3::jsonb
            GROUP BY runtime.runtime_id
            HAVING count(assignment.task_id) < runtime.capacity
            ORDER BY count(assignment.task_id),runtime.last_heartbeat_at DESC,runtime.runtime_id
            LIMIT 1""",
            AGENT_RUNTIME_POOL,
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
            registration_id=str(row["registration_id"]),
        )

    async def claim_assignments(
        self,
        runtime_id: str,
        role: str,
        *,
        registration_id: str = "legacy",
        limit: int = 1,
    ) -> list[ClaimedAssignment]:
        if registration_id == "legacy":
            registration_id = f"legacy-{runtime_id}"
        pool = await self.pool()
        claim_token = uuid4().hex
        rows = await pool.fetch(
            """WITH candidates AS (
                SELECT assignment.task_id FROM control.assignment assignment
                WHERE assignment.runtime_id=$1
                  AND EXISTS (
                    SELECT 1 FROM control.runtime_instance runtime
                    WHERE runtime.runtime_id=$1 AND runtime.role=$2
                      AND runtime.registration_id=$3
                      AND runtime.status='ready'
                  )
                  AND EXISTS (
                    SELECT 1 FROM control.runtime_lease lease
                    WHERE lease.resource_id=(
                      'session:' || assignment.tenant_id || ':' || assignment.session_id
                    )
                      AND lease.fencing_token=assignment.fencing_token
                      AND lease.expires_at > now()
                  )
                  AND assignment.assignment_status='assigned'
                ORDER BY assignment.assigned_at, assignment.task_id
                FOR UPDATE SKIP LOCKED LIMIT $4
            )
            UPDATE control.assignment a
            SET assignment_status='running', started_at=now(),
                execution_claim_token=$5,
                execution_claim_expires_at=now() + interval '30 seconds'
            FROM candidates c WHERE a.task_id=c.task_id RETURNING a.task_id""",
            runtime_id,
            role,
            registration_id,
            limit,
            claim_token,
        )
        claimed: list[ClaimedAssignment] = []
        for row in rows:
            task_id = str(row["task_id"])
            assignment = await self.get_assignment(task_id)
            if assignment is not None:
                lease_expires_at = await pool.fetchval(
                    """UPDATE control.runtime_lease
                    SET expires_at=LEAST(expires_at, now() + interval '30 seconds')
                    WHERE lease_id=$1 RETURNING expires_at""",
                    assignment.lease_id,
                )
                assignment = replace(assignment, lease_expires_at=lease_expires_at)
                claimed.append(
                    ClaimedAssignment(task_id=task_id, assignment=assignment)
                )
        return claimed

    async def renew_assignment_claim(
        self,
        task_id: str,
        *,
        runtime_id: str,
        registration_id: str,
        execution_claim_token: str,
        lease_id: str,
        fencing_token: int,
    ) -> RuntimeAssignment:
        if registration_id == "legacy":
            registration_id = f"legacy-{runtime_id}"
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """SELECT assignment.tenant_id, assignment.session_id
                FROM control.assignment assignment
                JOIN control.runtime_instance runtime
                  ON runtime.runtime_id=assignment.runtime_id
                WHERE assignment.task_id=$1 AND assignment.runtime_id=$2
                  AND assignment.assignment_status='running'
                  AND assignment.execution_claim_token=$3
                  AND assignment.execution_claim_expires_at > now()
                  AND assignment.lease_id=$4 AND assignment.fencing_token=$5
                  AND runtime.registration_id=$6
                FOR UPDATE""",
                task_id,
                runtime_id,
                execution_claim_token,
                lease_id,
                fencing_token,
                registration_id,
            )
            if row is None:
                raise LeaseConflictError("execution claim is no longer owned")
            resource_id = f"session:{row['tenant_id']}:{row['session_id']}"
            lease = await connection.fetchrow(
                """SELECT * FROM control.runtime_lease
                WHERE resource_id=$1 AND lease_id=$2 AND fencing_token=$3
                  AND expires_at > now() FOR UPDATE""",
                resource_id,
                lease_id,
                fencing_token,
            )
            if lease is None:
                raise LeaseConflictError("assignment lease is no longer owned")
            lease_expires_at = await connection.fetchval(
                    """UPDATE control.runtime_lease
                    SET expires_at=now() + $4::interval
                    WHERE resource_id=$1 AND lease_id=$2 AND fencing_token=$3
                    RETURNING expires_at""",
                    resource_id,
                    lease_id,
                    fencing_token,
                    timedelta(seconds=30),
            )
            await connection.execute(
                    """UPDATE control.assignment
                    SET execution_claim_expires_at=now() + $3::interval
                    WHERE task_id=$1 AND execution_claim_token=$2""",
                    task_id,
                    execution_claim_token,
                    timedelta(seconds=30),
            )
            await connection.execute(
                """UPDATE control.runtime_instance SET last_heartbeat_at=now()
                WHERE runtime_id=$1 AND registration_id=$2""",
                runtime_id,
                registration_id,
            )
        assignment = await self.get_assignment(task_id)
        if assignment is None:
            raise LeaseConflictError("execution assignment disappeared")
        assignment.lease_expires_at = lease_expires_at
        return assignment

    async def abandon_stale_assignment(
        self,
        task_id: str,
        *,
        runtime_id: str,
        lease_id: str,
        fencing_token: int,
    ) -> bool:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """SELECT tenant_id, session_id, runtime_id, lease_id, fencing_token,
                          assignment_status
                   FROM control.assignment
                   WHERE task_id=$1 FOR UPDATE""",
                task_id,
            )
            if row is None:
                return False
            if (
                str(row["runtime_id"]) != runtime_id
                or str(row["lease_id"]) != lease_id
                or int(row["fencing_token"]) != fencing_token
            ):
                return False
            terminal = {
                "expired",
                "completed",
                "failed",
                "cancelled",
                "waiting_children",
                "waiting_for_human",
                "waiting_for_tool",
            }
            if str(row["assignment_status"]) in terminal:
                return True
            resource_id = f"session:{row['tenant_id']}:{row['session_id']}"
            lease_valid = await connection.fetchval(
                """SELECT EXISTS(
                    SELECT 1 FROM control.runtime_lease
                    WHERE resource_id=$1 AND fencing_token=$2 AND expires_at > now())""",
                resource_id,
                fencing_token,
            )
            if lease_valid:
                return False
            await connection.execute(
                """UPDATE control.assignment
                SET assignment_status='expired', completed_at=now()
                WHERE task_id=$1""",
                task_id,
            )
            await connection.execute(
                """UPDATE control.runnable_item
                SET status='queued', claimed_by=NULL, claim_token=NULL,
                    claim_expires_at=NULL, available_at=now()
                WHERE task_id=$1""",
                task_id,
            )
            return True

    async def finish_assignment(self, task_id: str, outcome: str) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            if outcome in {
                "completed",
                "failed",
                "cancelled",
                "waiting_children",
                "waiting_for_human",
                "waiting_for_tool",
            }:
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

    async def suspend_assignment(self, task_id: str, reason: str) -> None:
        if reason not in {"waiting_children", "waiting_for_human", "waiting_for_tool"}:
            raise ValueError(f"unsupported assignment suspension: {reason}")
        await self.finish_assignment(task_id, reason)

    async def suspend_with_checkpoint(
        self,
        task_id: str,
        checkpoint: RuntimeCheckpoint,
        reason: str,
    ) -> None:
        if reason not in {"waiting_children", "waiting_for_human", "waiting_for_tool"}:
            raise ValueError(f"unsupported assignment suspension: {reason}")
        pool = await self.pool()
        resource_id = f"session:{checkpoint.tenant_id}:{checkpoint.session_id}"
        async with pool.acquire() as connection, connection.transaction():
            saved = await connection.fetchval(
                """
                INSERT INTO control.runtime_checkpoint
                  (tenant_id,session_id,run_id,fencing_token,phase,state,updated_at)
                SELECT $1,$2,$3,$4,$5,$6::jsonb,$7
                WHERE EXISTS(SELECT 1 FROM control.runtime_lease
                  WHERE resource_id=$8 AND fencing_token=$4 AND expires_at > now())
                  AND EXISTS(SELECT 1 FROM control.assignment
                    WHERE task_id=$9 AND tenant_id=$1 AND session_id=$2 AND run_id=$3
                      AND fencing_token=$4 FOR UPDATE)
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
                task_id,
            )
            if saved is None:
                raise FencingTokenError(
                    "checkpoint suspension rejected for stale Runtime"
                )
            await connection.execute(
                """UPDATE control.assignment
                SET assignment_status=$2, completed_at=now() WHERE task_id=$1""",
                task_id,
                reason,
            )
            await connection.execute(
                "UPDATE control.runnable_item SET status='acked' WHERE task_id=$1",
                task_id,
            )
            await connection.execute(
                "DELETE FROM control.runtime_lease WHERE resource_id=$1",
                resource_id,
            )

    async def wake_assignment(self, task_id: str) -> bool:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            status = await connection.fetchval(
                "SELECT assignment_status FROM control.assignment WHERE task_id=$1 FOR UPDATE",
                task_id,
            )
            if status is None or str(status) not in {
                "waiting_children",
                "waiting_for_human",
                "waiting_for_tool",
            }:
                return False
            updated = await connection.fetchval(
                """UPDATE control.runnable_item
                SET status='queued', claimed_by=NULL, claim_token=NULL,
                    claim_expires_at=NULL, available_at=now()
                WHERE task_id=$1 RETURNING task_id""",
                task_id,
            )
            return updated is not None

    async def list_waiting_assignments(
        self, *, limit: int = 100
    ) -> tuple[RuntimeAssignment, ...]:
        pool = await self.pool()
        task_ids = await pool.fetch(
            """SELECT task_id FROM control.assignment
               WHERE assignment_status='waiting_children'
               ORDER BY completed_at, task_id LIMIT $1""",
            max(0, limit),
        )
        assignments: list[RuntimeAssignment] = []
        for row in task_ids:
            assignment = await self.get_assignment(str(row["task_id"]))
            if assignment is not None:
                assignments.append(assignment)
        return tuple(assignments)

    async def register_runtime(self, instance: RuntimeInstance) -> None:
        pool = await self.pool()
        if instance.registration_id == "legacy":
            instance = replace(
                instance, registration_id=f"legacy-{instance.runtime_id}"
            )
        async with pool.acquire() as connection, connection.transaction():
            current = await connection.fetchrow(
                """SELECT registration_id, last_heartbeat_at
                FROM control.runtime_instance WHERE runtime_id=$1 FOR UPDATE""",
                instance.runtime_id,
            )
            if current is not None and str(current["registration_id"]) != instance.registration_id:
                stale_before = "now() - interval '30 seconds'"
                active = await connection.fetchval(
                    f"""SELECT last_heartbeat_at > {stale_before}
                    FROM control.runtime_instance WHERE runtime_id=$1""",
                    instance.runtime_id,
                )
                if active:
                    raise LeaseConflictError(
                        f"runtime id is already registered: {instance.runtime_id}"
                    )
            await connection.execute(
                """
                INSERT INTO control.runtime_instance
                  (runtime_id,runtime_type,role,node_id,capabilities,status,capacity,
                   registration_id)
                VALUES ($1,$2,$3,$4,$5::jsonb,'ready',$6,$7)
                ON CONFLICT (runtime_id) DO UPDATE SET status='ready',
                  runtime_type=EXCLUDED.runtime_type, role=EXCLUDED.role,
                  node_id=EXCLUDED.node_id, capabilities=EXCLUDED.capabilities,
                  capacity=EXCLUDED.capacity, registration_id=EXCLUDED.registration_id,
                  started_at=now(), last_heartbeat_at=now()
                """,
                instance.runtime_id,
                instance.runtime_type,
                instance.role,
                instance.node_id,
                _json(instance.capabilities),
                instance.capacity,
                instance.registration_id,
            )

    async def heartbeat(
        self,
        runtime_id: str,
        fencing_token: int | None = None,
        *,
        registration_id: str = "legacy",
    ) -> None:
        pool = await self.pool()
        if registration_id == "legacy":
            registration_id = f"legacy-{runtime_id}"
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
            WHERE runtime_id=$1 AND registration_id=$2 RETURNING runtime_id""",
            runtime_id,
            registration_id,
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
            # Reclaim work held by missing/dead runtimes (e.g. after rolling
            # recreate). Live workers must heartbeat during execute; the stale
            # window is intentionally below a typical model-call duration only
            # because RemoteRuntimeWorker keeps last_heartbeat_at fresh.
            stale_heartbeat = "now() - interval '30 seconds'"
            stale_rows = await connection.fetch(
                f"""SELECT DISTINCT CONCAT(
                        'session:', a.tenant_id, ':', a.session_id
                    ) AS resource_id
                FROM control.assignment a
                LEFT JOIN control.runtime_instance r ON r.runtime_id = a.runtime_id
                WHERE a.assignment_status IN ('assigned', 'running')
                  AND (
                    r.runtime_id IS NULL
                    OR r.last_heartbeat_at IS NULL
                    OR r.last_heartbeat_at <= {stale_heartbeat}
                    OR (a.assignment_status='running'
                        AND a.execution_claim_expires_at <= now())
                  )"""
            )
            for row in stale_rows:
                resource_id = str(row["resource_id"])
                if resource_id not in resources:
                    resources.append(resource_id)
            if stale_rows:
                await connection.execute(
                    """UPDATE control.runtime_lease
                    SET expires_at = now() - interval '1 second'
                    WHERE resource_id = ANY($1::text[])
                      AND expires_at > now()""",
                    [str(row["resource_id"]) for row in stale_rows],
                )
            repaired_count = 0
            task_ids: list[str] = []
            if resources:
                repaired = await connection.fetch(
                        """UPDATE control.assignment
                        SET assignment_status='expired', completed_at=now()
                        WHERE ('session:' || tenant_id || ':' || session_id) = ANY($1::text[])
                          AND assignment_status IN ('assigned','running') RETURNING task_id""",
                        resources,
                )
                task_ids = [str(row["task_id"]) for row in repaired]
                repaired_count = len(task_ids)
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
            return repaired_count + int(recovered_claims or 0)

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
        capability_payload = dict(_decode_json(row["required_capability"]))
        capability, user_id, dept_id = _split_identity(capability_payload)
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
            required_capability=capability,
            deadline=row["deadline"],
            budget=cls._budget(_decode_json(row["budget"])),
            user_id=user_id,
            dept_id=dept_id,
        )

    @staticmethod
    def _budget(data: Any) -> RuntimeBudget:
        values = dict(data or {})
        return RuntimeBudget(
            max_steps=int(values.get("max_steps", DEFAULT_RUNTIME_MAX_STEPS)),
            max_output_tokens=int(values.get("max_output_tokens", 8192)),
            max_cost=float(values["max_cost"]) if values.get("max_cost") is not None else None,
        )
