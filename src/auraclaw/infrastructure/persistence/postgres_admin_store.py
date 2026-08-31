from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal

from auraclaw.admin.ports import AdminOperationClaim
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.internal import AdminOperationRequest, AdminOperationResponse
from auraclaw.infrastructure.persistence.postgres_common import LazyPool, json_dumps, json_loads

AdminSchema = Literal["projection", "delivery", "artifact"]


class PostgresAdminOperationStore(LazyPool):
    def __init__(self, database_url: str, *, schema: AdminSchema) -> None:
        super().__init__(database_url)
        self._schema = schema

    async def get(self, operation_id: str) -> AdminOperationResponse | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            f"SELECT * FROM {self._schema}.admin_operation "  # noqa: S608
            "WHERE operation_id=$1",
            operation_id,
        )
        if row is None:
            return None
        return AdminOperationResponse(
            operation_id=str(row["operation_id"]),
            status=str(row["status"]),
            result=dict(json_loads(row["result"])),
        )

    async def claim(
        self,
        request: AdminOperationRequest,
        *,
        request_digest: str,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> AdminOperationClaim:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            inserted = await connection.fetchval(
                f"""INSERT INTO {self._schema}.admin_operation  -- noqa: S608
                (operation_id,tenant_id,owner_service,operation,parameters,
                 request_digest,actor_identity,correlation_id,causation_id,
                 status,result,claimed_by,claim_token,claim_expires_at,started_at)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,
                        'running','{{}}'::jsonb,$10,$11,now()+$12::interval,now())
                ON CONFLICT (operation_id) DO NOTHING
                RETURNING operation_id""",
                request.operation_id,
                request.context.tenant_id,
                request.owner_service.value,
                request.operation,
                json_dumps(request.parameters),
                request_digest,
                request.context.service_identity.value,
                request.context.correlation_id,
                request.context.causation_id,
                claimed_by,
                claim_token,
                claim_ttl,
            )
            if inserted is not None:
                return AdminOperationClaim(acquired=True, claim_token=claim_token)
            row = await connection.fetchrow(
                f"SELECT *, claim_expires_at <= now() AS claim_expired "  # noqa: S608
                f"FROM {self._schema}.admin_operation "
                "WHERE operation_id=$1 FOR UPDATE",
                request.operation_id,
            )
            assert row is not None
            if (
                str(row["tenant_id"]) != request.context.tenant_id
                or str(row["owner_service"]) != request.owner_service.value
                or str(row["operation"]) != request.operation
                or str(row["request_digest"]) != request_digest
            ):
                raise VersionConflictError(
                    "admin operation id was already used for a different request"
                )
            status = str(row["status"])
            if status in {"completed", "failed"}:
                return AdminOperationClaim(response=self._response(row))
            if bool(row["claim_expired"]):
                recovery = AdminOperationResponse(
                    operation_id=request.operation_id,
                    status="failed",
                    result={
                        "error": "admin operation requires manual recovery",
                        "error_code": "unknown_side_effect",
                    },
                )
                await connection.execute(
                    f"""UPDATE {self._schema}.admin_operation  -- noqa: S608
                    SET status='failed',result=$2::jsonb,last_error_code='unknown_side_effect',
                        claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,
                        completed_at=now(),updated_at=now()
                    WHERE operation_id=$1""",
                    request.operation_id,
                    json_dumps(recovery.result),
                )
                return AdminOperationClaim(response=recovery)
            return AdminOperationClaim(
                response=AdminOperationResponse(
                    operation_id=request.operation_id,
                    status="running",
                    result={},
                )
            )

    async def complete(
        self,
        request: AdminOperationRequest,
        response: AdminOperationResponse,
        *,
        claim_token: str,
        error_code: str | None = None,
    ) -> bool:
        pool = await self.pool()
        completed = await pool.fetchval(
            f"""UPDATE {self._schema}.admin_operation  -- noqa: S608
            SET status=$3,result=$4::jsonb,last_error_code=$5,
                claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,
                completed_at=now(),updated_at=now()
            WHERE operation_id=$1 AND tenant_id=$2 AND status='running'
              AND claim_token=$6
            RETURNING operation_id""",
            request.operation_id,
            request.context.tenant_id,
            response.status,
            json_dumps(response.result),
            error_code,
            claim_token,
        )
        return completed is not None

    async def renew(
        self,
        request: AdminOperationRequest,
        *,
        claimed_by: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> bool:
        pool = await self.pool()
        renewed = await pool.fetchval(
            f"""UPDATE {self._schema}.admin_operation  -- noqa: S608
            SET claim_expires_at=now()+$4::interval,updated_at=now()
            WHERE operation_id=$1 AND tenant_id=$2 AND status='running'
              AND claimed_by=$3 AND claim_token=$5 AND claim_expires_at > now()
            RETURNING operation_id""",
            request.operation_id,
            request.context.tenant_id,
            claimed_by,
            claim_ttl,
            claim_token,
        )
        return renewed is not None

    @staticmethod
    def _response(row: Any) -> AdminOperationResponse:
        return AdminOperationResponse(
            operation_id=str(row["operation_id"]),
            status=str(row["status"]),
            result=dict(json_loads(row["result"])),
        )
