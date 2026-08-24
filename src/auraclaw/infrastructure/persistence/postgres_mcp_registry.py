from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from auraclaw.action.mcp_registry import McpServerRegistryStore
from auraclaw.contracts.errors import NotFoundError, VersionConflictError
from auraclaw.contracts.mcp_registry import (
    McpActiveSnapshotEntry,
    McpDesiredState,
    McpObservedState,
    McpRegistryOperationKind,
    McpRegistryOperationStatus,
    McpServerConfig,
    McpServerOperationRecord,
    McpServerRecord,
    McpServerRevisionRecord,
    McpServerRuntimeRecord,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
)


class PostgresMcpServerRegistryStore(LazyPool, McpServerRegistryStore):
    async def get_server(self, server_id: str) -> McpServerRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            "SELECT * FROM hands.mcp_server WHERE server_id=$1",
            server_id,
        )
        if row is None:
            return None
        return await self._hydrate(dict(row))

    async def list_servers(self, tenant_id: str) -> tuple[McpServerRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.mcp_server
            WHERE desired_state <> 'retired'
              AND (tenant_id=$1 OR tenant_id IS NULL)
            ORDER BY server_id""",
            tenant_id,
        )
        return tuple([await self._hydrate(dict(row)) for row in rows])

    async def get_revision(
        self, server_id: str, revision: int
    ) -> McpServerRevisionRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.mcp_server_revision
            WHERE server_id=$1 AND revision=$2""",
            server_id,
            revision,
        )
        return None if row is None else _revision(dict(row))

    async def get_operation(
        self, operation_id: str
    ) -> McpServerOperationRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            "SELECT * FROM hands.mcp_server_operation WHERE operation_id=$1",
            operation_id,
        )
        return None if row is None else _operation(dict(row))

    async def get_operation_by_command(
        self, command_id: str, tenant_id: str
    ) -> McpServerOperationRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.mcp_server_operation
            WHERE tenant_id=$1 AND command_id=$2""",
            tenant_id,
            command_id,
        )
        return None if row is None else _operation(dict(row))

    async def insert_candidate(
        self,
        *,
        record: McpServerRecord,
        revision: McpServerRevisionRecord,
        operation: McpServerOperationRecord,
        create: bool,
    ) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
                if create:
                    inserted = await connection.fetchval(
                        """INSERT INTO hands.mcp_server
                        (server_id,tenant_id,desired_state,latest_revision,
                         active_revision,created_by,created_at,updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (server_id) DO NOTHING
                        RETURNING server_id""",
                        record.server_id,
                        record.tenant_id,
                        record.desired_state.value,
                        record.latest_revision,
                        record.active_revision,
                        record.created_by,
                        record.created_at,
                        record.updated_at,
                    )
                    if inserted is None:
                        raise VersionConflictError("MCP server already exists")
                else:
                    updated = await connection.fetchval(
                        """UPDATE hands.mcp_server
                        SET latest_revision=$1, tenant_id=$2, updated_at=$3
                        WHERE server_id=$4 AND latest_revision=$5
                        RETURNING server_id""",
                        record.latest_revision,
                        record.tenant_id,
                        record.updated_at,
                        record.server_id,
                        record.latest_revision - 1,
                    )
                    if updated is None:
                        existing = await connection.fetchval(
                            "SELECT server_id FROM hands.mcp_server WHERE server_id=$1",
                            record.server_id,
                        )
                        if existing is None:
                            raise NotFoundError("MCP server was not found")
                        raise VersionConflictError("MCP server revision conflict")
                await connection.execute(
                    """INSERT INTO hands.mcp_server_revision
                    (server_id,revision,config_json,config_digest,created_by,created_at)
                    VALUES ($1,$2,$3::jsonb,$4,$5,$6)""",
                    revision.server_id,
                    revision.revision,
                    json_dumps(revision.config.model_dump(mode="json")),
                    revision.config_digest,
                    revision.created_by,
                    revision.created_at,
                )
                await connection.execute(
                    """INSERT INTO hands.mcp_server_runtime
                    (server_id,observed_state,updated_at)
                    VALUES ($1,'pending',$2)
                    ON CONFLICT (server_id) DO NOTHING""",
                    record.server_id,
                    record.updated_at,
                )
                await _upsert_operation(connection, operation)

    async def set_desired_state(
        self,
        *,
        server_id: str,
        expected_revision: int,
        desired_state: McpDesiredState,
        active_revision: int | None,
        operation: McpServerOperationRecord,
    ) -> McpServerRecord:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
                row = await connection.fetchrow(
                    """UPDATE hands.mcp_server
                    SET desired_state=$1, active_revision=$2, updated_at=$3
                    WHERE server_id=$4 AND latest_revision=$5
                    RETURNING *""",
                    desired_state.value,
                    active_revision,
                    datetime.now(UTC),
                    server_id,
                    expected_revision,
                )
                if row is None:
                    existing = await connection.fetchval(
                        "SELECT server_id FROM hands.mcp_server WHERE server_id=$1",
                        server_id,
                    )
                    if existing is None:
                        raise NotFoundError("MCP server was not found")
                    raise VersionConflictError("MCP server revision conflict")
                await _upsert_operation(connection, operation)
        hydrated = await self._hydrate(dict(row))
        return hydrated

    async def complete_operation(
        self, operation: McpServerOperationRecord
    ) -> McpServerOperationRecord:
        pool = await self.pool()
        async with pool.acquire() as connection:
            await _upsert_operation(connection, operation)
        return operation

    async def update_runtime(self, runtime: McpServerRuntimeRecord) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO hands.mcp_server_runtime
            (server_id,loaded_revision,observed_state,last_test_at,last_sync_at,
             consecutive_failures,safe_error_code,updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (server_id) DO UPDATE SET
              loaded_revision=EXCLUDED.loaded_revision,
              observed_state=EXCLUDED.observed_state,
              last_test_at=EXCLUDED.last_test_at,
              last_sync_at=EXCLUDED.last_sync_at,
              consecutive_failures=EXCLUDED.consecutive_failures,
              safe_error_code=EXCLUDED.safe_error_code,
              updated_at=EXCLUDED.updated_at""",
            runtime.server_id,
            runtime.loaded_revision,
            runtime.observed_state.value,
            runtime.last_test_at,
            runtime.last_sync_at,
            runtime.consecutive_failures,
            runtime.safe_error_code,
            runtime.updated_at,
        )

    async def list_active_snapshot(self) -> tuple[McpActiveSnapshotEntry, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT s.server_id, s.tenant_id, s.active_revision, s.desired_state,
                      r.config_json, rt.observed_state
            FROM hands.mcp_server AS s
            JOIN hands.mcp_server_revision AS r
              ON r.server_id=s.server_id AND r.revision=s.active_revision
            LEFT JOIN hands.mcp_server_runtime AS rt ON rt.server_id=s.server_id
            WHERE s.desired_state='enabled' AND s.active_revision IS NOT NULL
            ORDER BY s.server_id"""
        )
        return tuple(
            McpActiveSnapshotEntry(
                server_id=str(row["server_id"]),
                tenant_id=row["tenant_id"],
                revision=int(row["active_revision"]),
                config=McpServerConfig.model_validate(json_loads(row["config_json"])),
                desired_state=McpDesiredState(str(row["desired_state"])),
                observed_state=McpObservedState(
                    str(row["observed_state"] or "pending")
                ),
            )
            for row in rows
        )

    async def _hydrate(self, row: dict[str, Any]) -> McpServerRecord:
        server_id = str(row["server_id"])
        latest = await self.get_revision(server_id, int(row["latest_revision"]))
        active_revision = row["active_revision"]
        active = (
            None
            if active_revision is None
            else await self.get_revision(server_id, int(active_revision))
        )
        pool = await self.pool()
        runtime_row = await pool.fetchrow(
            "SELECT * FROM hands.mcp_server_runtime WHERE server_id=$1",
            server_id,
        )
        return McpServerRecord(
            server_id=server_id,
            tenant_id=row["tenant_id"],
            desired_state=McpDesiredState(str(row["desired_state"])),
            latest_revision=int(row["latest_revision"]),
            active_revision=None if active_revision is None else int(active_revision),
            created_by=str(row["created_by"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            latest_config=None if latest is None else latest.config,
            active_config=None if active is None else active.config,
            runtime=None if runtime_row is None else _runtime(dict(runtime_row)),
        )


async def _upsert_operation(connection: Any, operation: McpServerOperationRecord) -> None:
    await connection.execute(
        """INSERT INTO hands.mcp_server_operation
        (operation_id,server_id,tenant_id,target_revision,command_id,actor_id,
         correlation_id,causation_id,operation,status,safe_error_code,result_json,
         created_at,completed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14)
        ON CONFLICT (operation_id) DO UPDATE SET
          status=EXCLUDED.status,
          safe_error_code=EXCLUDED.safe_error_code,
          result_json=EXCLUDED.result_json,
          completed_at=EXCLUDED.completed_at""",
        operation.operation_id,
        operation.server_id,
        operation.tenant_id or "platform",
        operation.target_revision,
        operation.command_id,
        operation.actor_id,
        operation.correlation_id,
        operation.causation_id,
        operation.operation.value,
        operation.status.value,
        operation.safe_error_code,
        json_dumps(operation.result),
        operation.created_at,
        operation.completed_at,
    )


def _revision(row: dict[str, Any]) -> McpServerRevisionRecord:
    return McpServerRevisionRecord(
        server_id=str(row["server_id"]),
        revision=int(row["revision"]),
        config=McpServerConfig.model_validate(json_loads(row["config_json"])),
        config_digest=str(row["config_digest"]),
        created_by=str(row["created_by"]),
        created_at=row["created_at"],
    )


def _runtime(row: dict[str, Any]) -> McpServerRuntimeRecord:
    return McpServerRuntimeRecord(
        server_id=str(row["server_id"]),
        loaded_revision=(
            None if row["loaded_revision"] is None else int(row["loaded_revision"])
        ),
        observed_state=McpObservedState(str(row["observed_state"])),
        last_test_at=row["last_test_at"],
        last_sync_at=row["last_sync_at"],
        consecutive_failures=int(row["consecutive_failures"]),
        safe_error_code=row["safe_error_code"],
        updated_at=row["updated_at"],
    )


def _operation(row: dict[str, Any]) -> McpServerOperationRecord:
    return McpServerOperationRecord(
        operation_id=str(row["operation_id"]),
        server_id=str(row["server_id"]),
        tenant_id=row["tenant_id"],
        target_revision=(
            None if row["target_revision"] is None else int(row["target_revision"])
        ),
        command_id=str(row["command_id"]),
        actor_id=str(row["actor_id"]),
        correlation_id=str(row["correlation_id"]),
        causation_id=str(row["causation_id"]),
        operation=McpRegistryOperationKind(str(row["operation"])),
        status=McpRegistryOperationStatus(str(row["status"])),
        safe_error_code=row["safe_error_code"],
        result=dict(json_loads(row["result_json"] or "{}")),
        created_at=row["created_at"],
        completed_at=row["completed_at"],
    )
