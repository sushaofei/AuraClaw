from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from auraclaw.contracts.errors import LeaseConflictError, VersionConflictError
from auraclaw.contracts.internal import ModelGenerateResponse
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
)
from auraclaw.model_gateway.ports import (
    ModelCallExecution,
    ModelCallReservation,
    ModelCancellation,
)


class PostgresModelStateStore(LazyPool):
    async def reserve(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        run_id: str,
        request_digest: str,
        reserved_tokens: int,
        token_limit: int,
        execution_owner: str = "model-gateway",
        provider_request_ref: str | None = None,
        actor: str = "model-gateway",
        correlation_id: str = "model-call",
        causation_id: str = "model-call",
        claim_ttl: timedelta = timedelta(seconds=30),
        window: timedelta = timedelta(hours=1),
    ) -> ModelCallReservation:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """INSERT INTO model_gateway.usage_budget
                       (tenant_id, window_started_at, window_seconds, token_limit)
                   VALUES ($1, now(), $2, $3)
                   ON CONFLICT (tenant_id) DO NOTHING""",
                tenant_id,
                int(window.total_seconds()),
                token_limit,
            )
            budget = await connection.fetchrow(
                """SELECT * FROM model_gateway.usage_budget
                   WHERE tenant_id=$1 FOR UPDATE""",
                tenant_id,
            )
            assert budget is not None
            await connection.execute(
                """UPDATE model_gateway.usage_budget SET
                       window_started_at=now(), tokens_reserved=0, tokens_used=0,
                       window_seconds=$2, token_limit=$3, updated_at=now()
                   WHERE tenant_id=$1
                     AND window_started_at + make_interval(secs => window_seconds) <= now()""",
                tenant_id,
                int(window.total_seconds()),
                token_limit,
            )
            budget = await connection.fetchrow(
                "SELECT * FROM model_gateway.usage_budget WHERE tenant_id=$1",
                tenant_id,
            )
            assert budget is not None
            existing = await connection.fetchrow(
                """SELECT *,now() AS database_now FROM model_gateway.model_call
                   WHERE tenant_id=$1 AND model_call_id=$2 FOR UPDATE""",
                tenant_id,
                model_call_id,
            )
            if existing is not None:
                if str(existing["request_digest"]) != request_digest:
                    return ModelCallReservation("conflict")
                if existing["status"] == "completed" and existing["response"] is not None:
                    return ModelCallReservation(
                        "completed",
                        ModelGenerateResponse.model_validate(
                            dict(json_loads(existing["response"]))
                        ),
                    )
                if existing["status"] == "cancelled":
                    return ModelCallReservation("cancelled")
                if existing["status"] == "reconciling":
                    return ModelCallReservation("reconciling")
                if existing["status"] != "failed":
                    if (
                        existing["claim_expires_at"] is None
                        or existing["claim_expires_at"] <= existing["database_now"]
                    ):
                        await connection.execute(
                            """UPDATE model_gateway.model_call
                            SET status='reconciling',error_code='execution_owner_lost',
                                updated_at=now()
                            WHERE tenant_id=$1 AND model_call_id=$2""",
                            tenant_id,
                            model_call_id,
                        )
                        return ModelCallReservation("reconciling")
                    return ModelCallReservation("in_progress")
            if (
                int(budget["tokens_used"])
                + int(budget["tokens_reserved"])
                + reserved_tokens
                > token_limit
            ):
                return ModelCallReservation("quota_exceeded")
            claim_token = uuid4().hex
            if existing is None:
                await connection.execute(
                    """INSERT INTO model_gateway.model_call
                           (tenant_id,model_call_id,run_id,request_digest,status,reserved_tokens,
                            execution_owner,claim_token,provider_request_ref,actor,
                            correlation_id,causation_id,started_at,
                            heartbeat_at,claim_expires_at)
                       VALUES ($1,$2,$3,$4,'executing',$5,$6,$7,$8,$9,$10,$11,
                               now(),now(),now()+$12::interval)""",
                    tenant_id,
                    model_call_id,
                    run_id,
                    request_digest,
                    reserved_tokens,
                    execution_owner,
                    claim_token,
                    provider_request_ref,
                    actor,
                    correlation_id,
                    causation_id,
                    claim_ttl,
                )
            else:
                await connection.execute(
                    """UPDATE model_gateway.model_call SET status='executing',
                           reserved_tokens=$3,error_code=NULL,execution_owner=$4,
                           claim_token=$5,started_at=now(),heartbeat_at=now(),
                           claim_expires_at=now()+$6::interval,
                           provider_request_ref=$7,
                           actor=$8,correlation_id=$9,causation_id=$10,
                           cancel_requested_at=NULL,cancelled_at=NULL,completed_at=NULL,
                           response=NULL,usage='{}'::jsonb,updated_at=now()
                       WHERE tenant_id=$1 AND model_call_id=$2""",
                    tenant_id,
                    model_call_id,
                    reserved_tokens,
                    execution_owner,
                    claim_token,
                    claim_ttl,
                    provider_request_ref,
                    actor,
                    correlation_id,
                    causation_id,
                )
            await connection.execute(
                """UPDATE model_gateway.usage_budget
                   SET tokens_reserved=tokens_reserved+$2,token_limit=$3,updated_at=now()
                   WHERE tenant_id=$1""",
                tenant_id,
                reserved_tokens,
                token_limit,
            )
            return ModelCallReservation("reserved", claim_token=claim_token)

    async def complete(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        response: ModelGenerateResponse,
        claim_token: str | None = None,
    ) -> None:
        used_tokens = self._usage_tokens(response)
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            call = await connection.fetchrow(
                """SELECT reserved_tokens,status,claim_token,claim_expires_at,
                          now() AS database_now
                   FROM model_gateway.model_call
                   WHERE tenant_id=$1 AND model_call_id=$2 FOR UPDATE""",
                tenant_id,
                model_call_id,
            )
            if call is None or call["status"] == "completed":
                return
            if call["status"] == "cancelled":
                raise LeaseConflictError("cancelled model call cannot complete")
            if claim_token is not None and (
                call["claim_token"] != claim_token
                or call["claim_expires_at"] is None
                or call["claim_expires_at"] <= call["database_now"]
            ):
                raise LeaseConflictError("model call execution claim is no longer owned")
            await connection.execute(
                """UPDATE model_gateway.usage_budget SET
                       tokens_reserved=GREATEST(0,tokens_reserved-$2),
                       tokens_used=tokens_used+$3,updated_at=now()
                   WHERE tenant_id=$1""",
                tenant_id,
                int(call["reserved_tokens"]),
                used_tokens,
            )
            await connection.execute(
                """UPDATE model_gateway.model_call SET status='completed',
                       provider=$3,model=$4,usage=$5::jsonb,response=$6::jsonb,
                       completed_at=now(),execution_owner=NULL,claim_token=NULL,
                       heartbeat_at=NULL,claim_expires_at=NULL,updated_at=now()
                   WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                model_call_id,
                response.provider,
                response.model,
                json_dumps(response.usage),
                json_dumps(response.model_dump(mode="json")),
            )

    async def fail(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        error_code: str,
        claim_token: str | None = None,
    ) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            call = await connection.fetchrow(
                """SELECT reserved_tokens,status,claim_token,claim_expires_at,
                          now() AS database_now
                   FROM model_gateway.model_call
                   WHERE tenant_id=$1 AND model_call_id=$2 FOR UPDATE""",
                tenant_id,
                model_call_id,
            )
            if call is None or call["status"] not in {"executing", "cancel_requested"}:
                return
            if claim_token is not None:
                if call["claim_token"] != claim_token:
                    raise LeaseConflictError("model call execution claim is no longer owned")
                if (
                    call["claim_expires_at"] is None
                    or call["claim_expires_at"] <= call["database_now"]
                ):
                    await connection.execute(
                        """UPDATE model_gateway.model_call
                        SET status='reconciling',error_code='execution_owner_lost',
                            heartbeat_at=NULL,claim_expires_at=NULL,updated_at=now()
                        WHERE tenant_id=$1 AND model_call_id=$2""",
                        tenant_id,
                        model_call_id,
                    )
                    return
            await connection.execute(
                """UPDATE model_gateway.usage_budget SET
                       tokens_reserved=GREATEST(0,tokens_reserved-$2),updated_at=now()
                   WHERE tenant_id=$1""",
                tenant_id,
                int(call["reserved_tokens"]),
            )
            await connection.execute(
                """UPDATE model_gateway.model_call SET status='failed',error_code=$3,
                       execution_owner=NULL,claim_token=NULL,heartbeat_at=NULL,
                       claim_expires_at=NULL,updated_at=now()
                   WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                model_call_id,
                error_code,
            )

    async def heartbeat(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        execution_owner: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> ModelCallExecution:
        pool = await self.pool()
        row = await pool.fetchrow(
            """UPDATE model_gateway.model_call
            SET heartbeat_at=now(),claim_expires_at=now()+$5::interval,updated_at=now()
            WHERE tenant_id=$1 AND model_call_id=$2 AND execution_owner=$3
              AND claim_token=$4 AND status IN ('executing','cancel_requested')
              AND claim_expires_at > now()
            RETURNING status,cancel_requested_at,error_code""",
            tenant_id,
            model_call_id,
            execution_owner,
            claim_token,
            claim_ttl,
        )
        if row is not None:
            return ModelCallExecution(
                status=str(row["status"]),
                owned=True,
                cancel_requested=row["cancel_requested_at"] is not None,
                error_code=row["error_code"],
            )
        current = await pool.fetchrow(
            """SELECT status,cancel_requested_at,error_code
            FROM model_gateway.model_call WHERE tenant_id=$1 AND model_call_id=$2""",
            tenant_id,
            model_call_id,
        )
        return ModelCallExecution(
            status="not_found" if current is None else str(current["status"]),
            owned=False,
            cancel_requested=(
                current is not None and current["cancel_requested_at"] is not None
            ),
            error_code=None if current is None else current["error_code"],
        )

    async def request_cancel(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        run_id: str,
        actor: str = "agent-runtime",
        correlation_id: str = "model-cancel",
        causation_id: str = "model-cancel",
    ) -> ModelCancellation:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """SELECT *,now() AS database_now FROM model_gateway.model_call
                WHERE tenant_id=$1 AND model_call_id=$2 FOR UPDATE""",
                tenant_id,
                model_call_id,
            )
            if row is None:
                return ModelCancellation("not_found", False)
            if str(row["run_id"]) != run_id:
                raise VersionConflictError("model cancel run_id does not match")
            status = str(row["status"])
            if status == "completed":
                return ModelCancellation(status, False)
            if status == "cancelled":
                return ModelCancellation(status, True)
            if status == "failed":
                return ModelCancellation(status, False)
            if status == "reconciling":
                return ModelCancellation(status, False, row["execution_owner"])
            if (
                row["claim_expires_at"] is None
                or row["claim_expires_at"] <= row["database_now"]
            ):
                await connection.execute(
                    """UPDATE model_gateway.model_call
                    SET status='reconciling',error_code='execution_owner_lost',
                        updated_at=now()
                    WHERE tenant_id=$1 AND model_call_id=$2""",
                    tenant_id,
                    model_call_id,
                )
                return ModelCancellation("reconciling", False, row["execution_owner"])
            await connection.execute(
                """UPDATE model_gateway.model_call
                SET status='cancel_requested',cancel_requested_at=COALESCE(
                    cancel_requested_at,now()),cancel_actor=$3,
                    cancel_correlation_id=$4,cancel_causation_id=$5,updated_at=now()
                WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                model_call_id,
                actor,
                correlation_id,
                causation_id,
            )
            return ModelCancellation("cancel_requested", True, row["execution_owner"])

    async def mark_cancelled(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        execution_owner: str,
        claim_token: str,
        usage: dict[str, int | float],
    ) -> bool:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            call = await connection.fetchrow(
                """SELECT reserved_tokens,status FROM model_gateway.model_call
                WHERE tenant_id=$1 AND model_call_id=$2 AND execution_owner=$3
                  AND claim_token=$4 AND claim_expires_at > now() FOR UPDATE""",
                tenant_id,
                model_call_id,
                execution_owner,
                claim_token,
            )
            if call is None:
                return False
            if call["status"] == "cancelled":
                return True
            if call["status"] != "cancel_requested":
                return False
            await connection.execute(
                """UPDATE model_gateway.usage_budget SET
                tokens_reserved=GREATEST(0,tokens_reserved-$2),
                tokens_used=tokens_used+$3,updated_at=now()
                WHERE tenant_id=$1""",
                tenant_id,
                int(call["reserved_tokens"]),
                self._usage_value(usage),
            )
            await connection.execute(
                """UPDATE model_gateway.model_call SET status='cancelled',
                usage=$3::jsonb,cancelled_at=now(),execution_owner=NULL,claim_token=NULL,
                heartbeat_at=NULL,claim_expires_at=NULL,updated_at=now()
                WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                model_call_id,
                json_dumps(usage),
            )
            return True

    async def mark_reconciling(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        execution_owner: str,
        claim_token: str,
        error_code: str,
    ) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE model_gateway.model_call SET status='reconciling',
            error_code=$5,heartbeat_at=NULL,claim_expires_at=NULL,updated_at=now()
            WHERE tenant_id=$1 AND model_call_id=$2 AND execution_owner=$3
              AND claim_token=$4 AND status IN ('executing','cancel_requested')""",
            tenant_id,
            model_call_id,
            execution_owner,
            claim_token,
            error_code,
        )
        return str(result) == "UPDATE 1"

    @staticmethod
    def _usage_tokens(response: ModelGenerateResponse) -> int:
        return PostgresModelStateStore._usage_value(response.usage)

    @staticmethod
    def _usage_value(usage: dict[str, int | float]) -> int:
        if "total_tokens" in usage:
            return max(0, int(usage["total_tokens"]))
        return max(
            0,
            int(usage.get("input_tokens", usage.get("prompt_tokens", 0)))
            + int(usage.get("output_tokens", usage.get("completion_tokens", 0))),
        )
