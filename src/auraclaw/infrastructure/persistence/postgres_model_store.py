from __future__ import annotations

from datetime import timedelta

from auraclaw.contracts.internal import ModelGenerateResponse
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
)
from auraclaw.model_gateway.ports import ModelCallReservation


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
                """SELECT * FROM model_gateway.model_call
                   WHERE tenant_id=$1 AND model_call_id=$2""",
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
                if existing["status"] != "failed":
                    return ModelCallReservation("in_progress")
            if (
                int(budget["tokens_used"])
                + int(budget["tokens_reserved"])
                + reserved_tokens
                > token_limit
            ):
                return ModelCallReservation("quota_exceeded")
            if existing is None:
                await connection.execute(
                    """INSERT INTO model_gateway.model_call
                           (tenant_id,model_call_id,run_id,request_digest,status,reserved_tokens)
                       VALUES ($1,$2,$3,$4,'reserved',$5)""",
                    tenant_id,
                    model_call_id,
                    run_id,
                    request_digest,
                    reserved_tokens,
                )
            else:
                await connection.execute(
                    """UPDATE model_gateway.model_call SET status='reserved',
                           reserved_tokens=$3,error_code=NULL,updated_at=now()
                       WHERE tenant_id=$1 AND model_call_id=$2""",
                    tenant_id,
                    model_call_id,
                    reserved_tokens,
                )
            await connection.execute(
                """UPDATE model_gateway.usage_budget
                   SET tokens_reserved=tokens_reserved+$2,token_limit=$3,updated_at=now()
                   WHERE tenant_id=$1""",
                tenant_id,
                reserved_tokens,
                token_limit,
            )
            return ModelCallReservation("reserved")

    async def complete(
        self,
        *,
        tenant_id: str,
        model_call_id: str,
        response: ModelGenerateResponse,
    ) -> None:
        used_tokens = self._usage_tokens(response)
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            call = await connection.fetchrow(
                """SELECT reserved_tokens,status FROM model_gateway.model_call
                   WHERE tenant_id=$1 AND model_call_id=$2 FOR UPDATE""",
                tenant_id,
                model_call_id,
            )
            if call is None or call["status"] == "completed":
                return
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
                       updated_at=now()
                   WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                model_call_id,
                response.provider,
                response.model,
                json_dumps(response.usage),
                json_dumps(response.model_dump(mode="json")),
            )

    async def fail(
        self, *, tenant_id: str, model_call_id: str, error_code: str
    ) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            call = await connection.fetchrow(
                """SELECT reserved_tokens,status FROM model_gateway.model_call
                   WHERE tenant_id=$1 AND model_call_id=$2 FOR UPDATE""",
                tenant_id,
                model_call_id,
            )
            if call is None or call["status"] != "reserved":
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
                       updated_at=now()
                   WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                model_call_id,
                error_code,
            )

    @staticmethod
    def _usage_tokens(response: ModelGenerateResponse) -> int:
        usage = response.usage
        if "total_tokens" in usage:
            return max(0, int(usage["total_tokens"]))
        return max(
            0,
            int(usage.get("input_tokens", usage.get("prompt_tokens", 0)))
            + int(usage.get("output_tokens", usage.get("completion_tokens", 0))),
        )
