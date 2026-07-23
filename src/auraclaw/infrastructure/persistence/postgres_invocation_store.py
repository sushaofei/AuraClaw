from __future__ import annotations

from typing import Any

from auraclaw.action.ports import InvocationBegin
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


class PostgresInvocationStore(LazyPool):
    async def begin(
        self, invocation: ToolInvocation, argument_digest: str
    ) -> InvocationBegin:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """SELECT argument_digest,normalized_result,status,side_effect_status
                FROM hands.invocation
                WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE""",
                invocation.tenant_id,
                invocation.idempotency_key,
            )
            if row is not None:
                if str(row["argument_digest"]) != argument_digest:
                    return InvocationBegin(conflict=True)
                stored = row["normalized_result"]
                if stored is None and str(row["status"]) != "waiting_approval":
                    return InvocationBegin(
                        cached_result=ToolResult(
                            status=ToolResultStatus.UNKNOWN,
                            summary="persisted invocation requires operator recovery",
                            error_code="invocation_recovery_required",
                            side_effect_status=str(row["side_effect_status"]),
                        )
                    )
                return InvocationBegin(
                    cached_result=(
                        _tool_result(dict(json_loads(stored)))
                        if stored is not None
                        else None
                    )
                )
            await connection.execute(
                """INSERT INTO hands.invocation
                (tenant_id,tool_invocation_id,idempotency_key,root_session_id,session_id,
                 run_id,tool_name,tool_version,argument_digest,normalized_arguments,status,
                 fencing_token,deadline)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'accepted',$11,$12)""",
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
            )
            await connection.execute(
                """INSERT INTO hands.invocation_attempt
                (tenant_id,tool_invocation_id,attempt,status) VALUES ($1,$2,1,'accepted')""",
                invocation.tenant_id,
                invocation.tool_invocation_id,
            )
        return InvocationBegin()

    async def set_status(
        self,
        invocation: ToolInvocation,
        status: str,
        *,
        error_code: str | None = None,
    ) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE hands.invocation SET status=$3,updated_at=now()
            WHERE tenant_id=$1 AND tool_invocation_id=$2""",
            invocation.tenant_id,
            invocation.tool_invocation_id,
            status,
        )
        await pool.execute(
            """UPDATE hands.invocation_attempt SET status=$3,error_code=$4,
            completed_at=CASE WHEN $3 IN ('success','error','denied','timeout','cancelled')
                              THEN now() ELSE completed_at END
            WHERE tenant_id=$1 AND tool_invocation_id=$2 AND attempt=1""",
            invocation.tenant_id,
            invocation.tool_invocation_id,
            status,
            error_code,
        )

    async def complete(self, invocation: ToolInvocation, result: Any) -> None:
        if not isinstance(result, ToolResult):
            raise TypeError("Invocation result must be ToolResult")
        pool = await self.pool()
        await pool.execute(
            """UPDATE hands.invocation SET status=$3,normalized_result=$4::jsonb,
            side_effect_status=$5,updated_at=now()
            WHERE tenant_id=$1 AND tool_invocation_id=$2""",
            invocation.tenant_id,
            invocation.tool_invocation_id,
            result.status.value,
            json_dumps(result.as_dict()),
            result.side_effect_status,
        )
        await self.set_status(
            invocation, result.status.value, error_code=result.error_code
        )
