from __future__ import annotations

from datetime import timedelta
from typing import Any

from auraclaw.action.ports import InvocationBegin, InvocationStatusRecord
from auraclaw.contracts.tools import ArtifactRef, ToolInvocation, ToolResult, ToolResultStatus
from auraclaw.infrastructure.persistence.postgres_common import LazyPool, json_dumps, json_loads


def _tool_result(payload: dict[str, Any]) -> ToolResult:
    content = payload.get("content")
    if isinstance(content, dict) and "artifact_ref" in content:
        content = ArtifactRef(**dict(content["artifact_ref"]))
    return ToolResult(
        status=ToolResultStatus(str(payload["status"])),
        content=content,
        summary=str(payload.get("summary", "")),
        metadata=dict(payload.get("metadata", {})),
        error_code=payload.get("error_code"),
        side_effect_status=str(payload.get("side_effect_status", "not_started")),
    )


def _recovery_result(side_effect_status: str = "unknown") -> ToolResult:
    return ToolResult(
        status=ToolResultStatus.UNKNOWN,
        summary="persisted invocation requires operator recovery",
        error_code="invocation_recovery_required",
        side_effect_status=side_effect_status,
    )


def _in_progress_result(side_effect_status: str) -> ToolResult:
    return ToolResult(
        status=ToolResultStatus.UNKNOWN,
        summary="tool invocation is owned by another Hands replica",
        error_code="invocation_in_progress",
        side_effect_status=side_effect_status,
    )


class PostgresInvocationStore(LazyPool):
    async def begin(
        self,
        invocation: ToolInvocation,
        argument_digest: str,
        *,
        owner: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> InvocationBegin:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            inserted = await connection.fetchval(
                """INSERT INTO hands.invocation
                (tenant_id,tool_invocation_id,idempotency_key,root_session_id,session_id,
                 run_id,tool_name,tool_version,argument_digest,normalized_arguments,status,
                 fencing_token,deadline,execution_owner,execution_claim_token,
                 execution_claim_expires_at,execution_heartbeat_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'accepted',$11,$12,
                        $13,$14,now()+$15::interval,now())
                ON CONFLICT DO NOTHING
                RETURNING tool_invocation_id""",
                invocation.tenant_id,
                invocation.tool_invocation_id,
                invocation.idempotency_key,
                invocation.root_session_id,
                invocation.session_id,
                invocation.run_id,
                invocation.tool_name,
                invocation.tool_version,
                argument_digest,
                json_dumps(invocation.arguments),
                invocation.fencing_token,
                invocation.deadline,
                owner,
                claim_token,
                claim_ttl,
            )
            if inserted is not None:
                await connection.execute(
                    """INSERT INTO hands.invocation_attempt
                    (tenant_id,tool_invocation_id,attempt,status)
                    VALUES ($1,$2,1,'accepted')""",
                    invocation.tenant_id,
                    invocation.tool_invocation_id,
                )
                return InvocationBegin(acquired=True, claim_token=claim_token)

            row = await connection.fetchrow(
                """SELECT idempotency_key,argument_digest,normalized_result,status,
                          side_effect_status,
                          execution_claim_expires_at > now() AS claim_active
                FROM hands.invocation
                WHERE tenant_id=$1
                  AND (idempotency_key=$2 OR tool_invocation_id=$3)
                FOR UPDATE""",
                invocation.tenant_id,
                invocation.idempotency_key,
                invocation.tool_invocation_id,
            )
            assert row is not None
            if (
                str(row["idempotency_key"]) != invocation.idempotency_key
                or str(row["argument_digest"]) != argument_digest
            ):
                return InvocationBegin(conflict=True)

            status = str(row["status"])
            side_effect_status = str(row["side_effect_status"])
            stored = row["normalized_result"]
            if status == "waiting_approval" and invocation.approval_id is None:
                return InvocationBegin(
                    cached_result=(
                        _tool_result(dict(json_loads(stored)))
                        if stored is not None
                        else _recovery_result("not_started")
                    )
                )
            if stored is not None and status not in {"accepted", "executing", "waiting_approval"}:
                return InvocationBegin(cached_result=_tool_result(dict(json_loads(stored))))
            if status == "executing" and not bool(row["claim_active"]):
                recovery = _recovery_result("unknown")
                await connection.execute(
                    """UPDATE hands.invocation SET status='unknown',normalized_result=$3::jsonb,
                       side_effect_status='unknown',execution_owner=NULL,
                       execution_claim_token=NULL,execution_claim_expires_at=NULL,updated_at=now()
                    WHERE tenant_id=$1 AND idempotency_key=$2""",
                    invocation.tenant_id,
                    invocation.idempotency_key,
                    json_dumps(recovery.as_dict()),
                )
                await connection.execute(
                    """UPDATE hands.invocation_attempt AS attempt
                       SET status='unknown',error_code='invocation_recovery_required',
                           completed_at=now()
                    FROM hands.invocation AS invocation
                    WHERE invocation.tenant_id=$1 AND invocation.idempotency_key=$2
                      AND attempt.tenant_id=invocation.tenant_id
                      AND attempt.tool_invocation_id=invocation.tool_invocation_id
                      AND attempt.attempt=1""",
                    invocation.tenant_id,
                    invocation.idempotency_key,
                )
                return InvocationBegin(cached_result=recovery)
            if status in {"accepted", "executing"} and bool(row["claim_active"]):
                return InvocationBegin(cached_result=_in_progress_result(side_effect_status))
            if status not in {"accepted", "waiting_approval"}:
                return InvocationBegin(
                    cached_result=(
                        _tool_result(dict(json_loads(stored)))
                        if stored is not None
                        else _recovery_result(side_effect_status)
                    )
                )

            await connection.execute(
                """UPDATE hands.invocation SET status='accepted',normalized_result=NULL,
                   execution_owner=$3,execution_claim_token=$4,
                   execution_claim_expires_at=now()+$5::interval,
                   execution_heartbeat_at=now(),cancel_requested_at=NULL,updated_at=now()
                WHERE tenant_id=$1 AND idempotency_key=$2""",
                invocation.tenant_id,
                invocation.idempotency_key,
                owner,
                claim_token,
                claim_ttl,
            )
            return InvocationBegin(acquired=True, claim_token=claim_token)

    async def mark_executing(
        self, invocation: ToolInvocation, *, claim_token: str
    ) -> bool:
        return await self._set_claimed_status(invocation, "executing", claim_token=claim_token)

    async def wait_for_approval(
        self, invocation: ToolInvocation, result: Any, *, claim_token: str
    ) -> bool:
        if not isinstance(result, ToolResult):
            raise TypeError("Invocation result must be ToolResult")
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE hands.invocation SET status='waiting_approval',
               normalized_result=$4::jsonb,side_effect_status='not_started',
               execution_owner=NULL,execution_claim_token=NULL,
               execution_claim_expires_at=NULL,updated_at=now()
            WHERE tenant_id=$1 AND idempotency_key=$2 AND execution_claim_token=$3
              AND execution_claim_expires_at > now()""",
            invocation.tenant_id,
            invocation.idempotency_key,
            claim_token,
            json_dumps(result.as_dict()),
        )
        if status == "UPDATE 1":
            await self._set_attempt_status(invocation, "waiting_approval", result.error_code)
            return True
        return False

    async def renew(
        self,
        invocation: ToolInvocation,
        *,
        owner: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> bool:
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE hands.invocation SET execution_claim_expires_at=now()+$5::interval,
               execution_heartbeat_at=now(),updated_at=now()
            WHERE tenant_id=$1 AND idempotency_key=$2 AND execution_owner=$3
              AND execution_claim_token=$4 AND status IN ('accepted','executing')
              AND execution_claim_expires_at > now()""",
            invocation.tenant_id,
            invocation.idempotency_key,
            owner,
            claim_token,
            claim_ttl,
        )
        return str(status) == "UPDATE 1"

    async def request_cancel(self, tenant_id: str, tool_invocation_id: str) -> bool:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """UPDATE hands.invocation SET
                   cancel_requested_at=COALESCE(cancel_requested_at,now()),
                   status=CASE WHEN status='waiting_approval' THEN 'cancelled' ELSE status END,
                   normalized_result=CASE WHEN status='waiting_approval' THEN
                     jsonb_build_object(
                       'status','cancelled','content',NULL,
                       'summary','tool invocation was cancelled',
                       'metadata','{}'::jsonb,'error_code','tool_cancelled',
                       'side_effect_status','not_started')
                     ELSE normalized_result END,
                   updated_at=now()
                WHERE tenant_id=$1 AND tool_invocation_id=$2
                  AND status IN ('accepted','executing','waiting_approval')
                RETURNING status""",
                tenant_id,
                tool_invocation_id,
            )
            if row is None:
                return False
            if str(row["status"]) == "cancelled":
                await connection.execute(
                    """UPDATE hands.invocation_attempt SET status='cancelled',
                       error_code='tool_cancelled',completed_at=now()
                    WHERE tenant_id=$1 AND tool_invocation_id=$2 AND attempt=1""",
                    tenant_id,
                    tool_invocation_id,
                )
            return True

    async def is_cancel_requested(
        self, invocation: ToolInvocation, *, claim_token: str
    ) -> bool:
        pool = await self.pool()
        value = await pool.fetchval(
            """SELECT cancel_requested_at IS NOT NULL
            FROM hands.invocation
            WHERE tenant_id=$1 AND idempotency_key=$2 AND execution_claim_token=$3""",
            invocation.tenant_id,
            invocation.idempotency_key,
            claim_token,
        )
        return bool(value)

    async def get_status(
        self, tenant_id: str, tool_invocation_id: str
    ) -> InvocationStatusRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT invocation.status,invocation.side_effect_status,
                      invocation.cancel_requested_at IS NOT NULL AS cancel_requested,
                      attempt.error_code
            FROM hands.invocation AS invocation
            LEFT JOIN hands.invocation_attempt AS attempt
              ON attempt.tenant_id=invocation.tenant_id
             AND attempt.tool_invocation_id=invocation.tool_invocation_id
             AND attempt.attempt=1
            WHERE invocation.tenant_id=$1 AND invocation.tool_invocation_id=$2""",
            tenant_id,
            tool_invocation_id,
        )
        if row is None:
            return None
        return InvocationStatusRecord(
            status=str(row["status"]),
            side_effect_status=str(row["side_effect_status"]),
            error_code=(str(row["error_code"]) if row["error_code"] is not None else None),
            cancel_requested=bool(row["cancel_requested"]),
        )

    async def complete(
        self, invocation: ToolInvocation, result: Any, *, claim_token: str
    ) -> bool:
        if not isinstance(result, ToolResult):
            raise TypeError("Invocation result must be ToolResult")
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE hands.invocation SET status=$4,normalized_result=$5::jsonb,
               side_effect_status=$6,execution_owner=NULL,execution_claim_token=NULL,
               execution_claim_expires_at=NULL,updated_at=now()
            WHERE tenant_id=$1 AND idempotency_key=$2 AND execution_claim_token=$3
              AND execution_claim_expires_at > now()""",
            invocation.tenant_id,
            invocation.idempotency_key,
            claim_token,
            result.status.value,
            json_dumps(result.as_dict()),
            result.side_effect_status,
        )
        if status == "UPDATE 1":
            await self._set_attempt_status(invocation, result.status.value, result.error_code)
            return True
        return False

    async def _set_claimed_status(
        self, invocation: ToolInvocation, status: str, *, claim_token: str
    ) -> bool:
        pool = await self.pool()
        updated = await pool.execute(
            """UPDATE hands.invocation SET status=$4,
               side_effect_status=CASE WHEN $4='executing' THEN 'unknown'
                                       ELSE side_effect_status END,
               updated_at=now()
            WHERE tenant_id=$1 AND idempotency_key=$2 AND execution_claim_token=$3
              AND execution_claim_expires_at > now()""",
            invocation.tenant_id,
            invocation.idempotency_key,
            claim_token,
            status,
        )
        if updated == "UPDATE 1":
            await self._set_attempt_status(invocation, status, None)
            return True
        return False

    async def _set_attempt_status(
        self, invocation: ToolInvocation, status: str, error_code: str | None
    ) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE hands.invocation_attempt AS attempt SET status=$3,error_code=$4,
               completed_at=CASE WHEN $3 IN
                 ('success','error','denied','timeout','cancelled','unknown')
                 THEN now() ELSE attempt.completed_at END
            FROM hands.invocation AS invocation
            WHERE invocation.tenant_id=$1 AND invocation.idempotency_key=$2
              AND attempt.tenant_id=invocation.tenant_id
              AND attempt.tool_invocation_id=invocation.tool_invocation_id
              AND attempt.attempt=1""",
            invocation.tenant_id,
            invocation.idempotency_key,
            status,
            error_code,
        )
