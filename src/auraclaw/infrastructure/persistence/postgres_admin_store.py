from __future__ import annotations

from typing import Literal

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

    async def save(
        self, request: AdminOperationRequest, response: AdminOperationResponse
    ) -> None:
        pool = await self.pool()
        await pool.execute(
            f"""INSERT INTO {self._schema}.admin_operation  -- noqa: S608
            (operation_id,operation,parameters,status,result)
            VALUES ($1,$2,$3::jsonb,$4,$5::jsonb)
            ON CONFLICT (operation_id) DO UPDATE SET
              status=EXCLUDED.status,result=EXCLUDED.result,updated_at=now()""",
            request.operation_id,
            request.operation,
            json_dumps(request.parameters),
            response.status,
            json_dumps(response.result),
        )
