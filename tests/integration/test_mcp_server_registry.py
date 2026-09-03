from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import pytest

from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.config import get_settings
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.mcp_registry import (
    McpDesiredState,
    McpRegistryOperationKind,
    McpRegistryOperationStatus,
    McpServerConfig,
    McpServerLifecycleCommand,
    McpServerOperationRecord,
    McpServerRuntimeRecord,
    McpServerWriteCommand,
)
from auraclaw.infrastructure.persistence.migration_runner import discover_migrations
from auraclaw.infrastructure.persistence.postgres_common import LazyPool
from auraclaw.infrastructure.persistence.postgres_mcp_registry import (
    PostgresMcpServerRegistryStore,
)

ROOT = Path(__file__).resolve().parents[2]


def _sql_url() -> str | None:
    settings = get_settings()
    if settings.sql_storage_enabled:
        return settings.resolved_database_url
    host = os.environ.get("DB_HOST")
    if not host:
        return None
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PWD")
    port = os.environ.get("DB_PORT") or "5432"
    database = os.environ.get("DB_NAME") or "auraclaw_dev"
    if not user or password is None:
        return None
    return (
        f"postgresql+asyncpg://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{quote(database, safe='')}"
    )


SQL_URL = _sql_url()
pytestmark = pytest.mark.skipif(SQL_URL is None, reason="SQL registry test URL not configured")


async def _apply_registry_migration(database_url: str) -> None:
    pool_holder = LazyPool(database_url)
    pool = await pool_holder.pool()
    try:
        async with pool.acquire() as connection:
            existing = await connection.fetchval(
                """SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema='hands' AND table_name='mcp_server'"""
            )
            if int(existing or 0) == 0:
                sql = (ROOT / "migrations/0020_mcp_server_registry.sql").read_text()
                for statement in _split_sql(sql):
                    await connection.execute(statement)
            await connection.execute(
                (ROOT / "migrations/0060_mcp_local_applied_generation.sql").read_text()
            )
            claims = (ROOT / "migrations/0046_mcp_lifecycle_operation_claims.sql").read_text()
            for statement in _split_sql(claims):
                await connection.execute(statement)
    finally:
        await pool_holder.close()


def _split_sql(source: str) -> list[str]:
    body = source.strip()
    if body.startswith("BEGIN;"):
        body = body.removeprefix("BEGIN;").strip()
    if body.endswith("COMMIT;"):
        body = body.removesuffix("COMMIT;").strip()
    statements: list[str] = []
    current: list[str] = []
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).strip())
            current = []
    if current:
        statements.append("\n".join(current).strip())
    return [item for item in statements if item and item != ";"]


async def _cleanup(store: PostgresMcpServerRegistryStore, server_id: str) -> None:
    pool = await store.pool()
    await pool.execute("DELETE FROM hands.mcp_server_operation WHERE server_id=$1", server_id)
    await pool.execute("DELETE FROM hands.mcp_server_runtime WHERE server_id=$1", server_id)
    await pool.execute("DELETE FROM hands.mcp_server_revision WHERE server_id=$1", server_id)
    await pool.execute("DELETE FROM hands.mcp_server WHERE server_id=$1", server_id)


def test_sql_mcp_registry_revision_idempotency_and_snapshot() -> None:
    async def scenario() -> None:
        assert SQL_URL is not None
        await _apply_registry_migration(SQL_URL)
        suffix = uuid4().hex[:12]
        server_id = f"reg-{suffix}"
        tenant_id = f"tenant-{suffix}"
        store = PostgresMcpServerRegistryStore(SQL_URL)
        service = McpServerRegistryService(store, allow_private_auth_none=True)

        class OkRuntime:
            async def test(self, entry: object) -> None:
                del entry

            async def apply(self, entry: object) -> None:
                del entry

            async def revoke(self, server_id: str) -> None:
                del server_id

        service.bind_runtime(OkRuntime())
        try:
            config = McpServerConfig(
                server_id=server_id,
                tenant_id=tenant_id,
                title="SQL Registry MCP",
                endpoint="http://127.0.0.1:18080/mcp",
                network_mode="loopback",
                auth_strategy="none",
            )
            created = await service.create(
                McpServerWriteCommand(
                    command_id=f"create-{suffix}",
                    tenant_id=tenant_id,
                    actor_id="admin",
                    correlation_id="corr",
                    causation_id="cause",
                    expected_revision=0,
                    config=config,
                )
            )
            again = await service.create(
                McpServerWriteCommand(
                    command_id=f"create-{suffix}",
                    tenant_id=tenant_id,
                    actor_id="admin",
                    correlation_id="corr",
                    causation_id="cause",
                    expected_revision=0,
                    config=config,
                )
            )
            assert created.operation_id == again.operation_id
            with pytest.raises(VersionConflictError):
                await service.update(
                    McpServerWriteCommand(
                        command_id=f"update-stale-{suffix}",
                        tenant_id=tenant_id,
                        actor_id="admin",
                        correlation_id="corr",
                        causation_id="cause",
                        expected_revision=0,
                        config=config.model_copy(update={"title": "stale"}),
                    )
                )
            enabled = await service.enable(
                server_id,
                McpServerLifecycleCommand(
                    command_id=f"enable-{suffix}",
                    tenant_id=tenant_id,
                    actor_id="admin",
                    correlation_id="corr",
                    causation_id="cause",
                    expected_revision=1,
                    target_revision=1,
                ),
            )
            assert enabled.status.value == "succeeded"
            record = await service.get_server(
                tenant_id=tenant_id, server_id=server_id, actor_id="admin"
            )
            assert record.desired_state is McpDesiredState.ENABLED
            assert record.active_revision == 1
            snapshot = await service.active_snapshot()
            assert any(item.server_id == server_id for item in snapshot)
            await service.disable(
                server_id,
                McpServerLifecycleCommand(
                    command_id=f"disable-{suffix}",
                    tenant_id=tenant_id,
                    actor_id="admin",
                    correlation_id="corr",
                    causation_id="cause",
                    expected_revision=1,
                ),
            )
            snapshot = await service.active_snapshot()
            assert all(item.server_id != server_id for item in snapshot)
        finally:
            await _cleanup(store, server_id)
            await store.close()

    asyncio.run(scenario())


def test_migration_discovery_includes_mcp_registry() -> None:
    migrations = discover_migrations(ROOT / "migrations")
    versions = {item.version for item in migrations}
    assert "0020" in versions
    assert len(migrations) >= 19


def test_lifecycle_command_is_claimed_before_runtime_side_effect() -> None:
    async def scenario() -> None:
        assert SQL_URL is not None
        await _apply_registry_migration(SQL_URL)
        suffix = uuid4().hex[:12]
        server_id = f"claim-{suffix}"
        tenant_id = f"tenant-{suffix}"
        store_a = PostgresMcpServerRegistryStore(SQL_URL)
        store_b = PostgresMcpServerRegistryStore(SQL_URL)
        service_a = McpServerRegistryService(store_a, instance_id="registry-a")
        service_b = McpServerRegistryService(store_b, instance_id="registry-b")
        started = asyncio.Event()
        release = asyncio.Event()

        class SlowRuntime:
            def __init__(self) -> None:
                self.applies = 0

            async def test(self, entry: object) -> None:
                del entry

            async def apply(self, entry: object) -> None:
                del entry
                self.applies += 1
                persisted = await store_b.get_server(server_id)
                assert persisted is not None
                assert persisted.desired_state is McpDesiredState.ENABLED
                assert persisted.active_revision == 1
                started.set()
                await release.wait()

            async def revoke(self, server_id: str) -> None:
                del server_id

        runtime = SlowRuntime()
        service_a.bind_runtime(runtime)
        service_b.bind_runtime(runtime)
        config = McpServerConfig(
            server_id=server_id,
            tenant_id=tenant_id,
            title="Claimed MCP",
            endpoint="https://claimed.example/mcp",
            credential_ref=f"vault/{server_id}#client_secret",
        )
        command_id = f"enable-{suffix}"
        command = McpServerLifecycleCommand(
            command_id=command_id,
            tenant_id=tenant_id,
            actor_id="admin",
            correlation_id=f"corr-{suffix}",
            causation_id=f"cause-{suffix}",
            expected_revision=1,
            target_revision=1,
        )
        try:
            await service_a.create(
                McpServerWriteCommand(
                    command_id=f"create-{suffix}",
                    tenant_id=tenant_id,
                    actor_id="admin",
                    correlation_id=f"corr-create-{suffix}",
                    causation_id=f"cause-create-{suffix}",
                    expected_revision=0,
                    config=config,
                )
            )
            winner = asyncio.create_task(service_a.enable(server_id, command))
            await asyncio.wait_for(started.wait(), timeout=2)
            loser = await service_b.enable(server_id, command)
            assert loser.status.value == "running"
            release.set()
            completed = await asyncio.wait_for(winner, timeout=2)
            assert completed.status.value == "succeeded"
            assert runtime.applies == 1

            with pytest.raises(VersionConflictError, match="different request"):
                await service_b.disable(server_id, command)

            abandoned = McpServerOperationRecord(
                operation_id=f"operation-abandoned-{suffix}",
                server_id=server_id,
                tenant_id=tenant_id,
                target_revision=1,
                command_id=f"abandoned-{suffix}",
                actor_id="admin",
                correlation_id=f"corr-abandoned-{suffix}",
                causation_id=f"cause-abandoned-{suffix}",
                operation=McpRegistryOperationKind.RECONCILE,
                status=McpRegistryOperationStatus.ACCEPTED,
                created_at=datetime.now(UTC),
            )
            claimed = await store_a.claim_operation(
                abandoned,
                request_digest="abandoned-digest",
                claimed_by="registry-a",
                claim_token="abandoned-token",
                claim_ttl=timedelta(seconds=30),
            )
            assert claimed.acquired
            pool = await store_a.pool()
            await pool.execute(
                """UPDATE hands.mcp_server_operation
                SET claim_expires_at=now()-interval '1 second'
                WHERE operation_id=$1""",
                abandoned.operation_id,
            )
            recovered = await store_b.claim_operation(
                abandoned,
                request_digest="abandoned-digest",
                claimed_by="registry-b",
                claim_token="takeover-token",
                claim_ttl=timedelta(seconds=30),
            )
            assert not recovered.acquired
            assert recovered.operation.status is McpRegistryOperationStatus.UNKNOWN_SIDE_EFFECT
            assert recovered.operation.safe_error_code == "mcp_operation_recovery_required"
        finally:
            release.set()
            await _cleanup(store_a, server_id)
            await store_a.close()
            await store_b.close()

    asyncio.run(scenario())


def test_sql_pending_delete_survives_restart_and_revision_fences_cleanup() -> None:
    async def scenario() -> None:
        assert SQL_URL is not None
        await _apply_registry_migration(SQL_URL)
        suffix = uuid4().hex[:12]
        server_id = f"delete-{suffix}"
        tenant_id = f"tenant-{suffix}"
        store = PostgresMcpServerRegistryStore(SQL_URL)
        service = McpServerRegistryService(store, allow_private_auth_none=True)
        restarted = PostgresMcpServerRegistryStore(SQL_URL)
        try:
            config = McpServerConfig(
                server_id=server_id,
                tenant_id=tenant_id,
                title="Deletion recovery",
                endpoint="http://127.0.0.1:18080/mcp",
                network_mode="loopback",
                auth_strategy="none",
            )
            write = McpServerWriteCommand(
                command_id=f"create-{suffix}",
                tenant_id=tenant_id,
                actor_id="admin",
                correlation_id="corr",
                causation_id="cause",
                expected_revision=0,
                config=config,
            )
            await service.create(write)
            await service.record_runtime(
                McpServerRuntimeRecord(
                    server_id=server_id,
                    instance_id="cold-follower",
                    loaded_revision=1,
                    applied_generation=7,
                    updated_at=datetime.now(UTC),
                )
            )
            restored_record = await restarted.get_server(server_id)
            assert restored_record.runtimes[0].applied_generation == 7

            class FailingRuntime:
                async def revoke(self, server_id):
                    raise RuntimeError("injected close failure")

            service.bind_runtime(FailingRuntime())
            pending = await service.delete(
                server_id,
                McpServerLifecycleCommand(
                    command_id=f"delete-{suffix}",
                    tenant_id=tenant_id,
                    actor_id="admin",
                    correlation_id="corr",
                    causation_id="cause",
                    expected_revision=1,
                ),
            )
            assert pending.status is McpRegistryOperationStatus.RECONCILING
            assert any(
                op.operation_id == pending.operation_id
                for op in await restarted.list_pending_deletes()
            )
            assert not any(
                entry.server_id == server_id for entry in await service.active_snapshot()
            )
            recovered = McpServerRegistryService(restarted, allow_private_auth_none=True)
            calls = []

            class RecoveredRuntime:
                async def revoke(self, server_id):
                    calls.append(server_id)

            recovered.bind_runtime(RecoveredRuntime())
            assert await recovered.reconcile_pending_deletes() == 1
            assert calls == [server_id]
            assert await store.get_server(server_id) is None
            assert (
                await store.get_operation(pending.operation_id)
            ).status is McpRegistryOperationStatus.SUCCEEDED
            await recovered.create(write.model_copy(update={"command_id": f"new-{suffix}"}))
            with pytest.raises(VersionConflictError, match="superseded"):
                await store.delete_server(server_id, expected_revision=1)
            assert await store.get_server(server_id) is not None
        finally:
            await _cleanup(store, server_id)
            await store.close()
            await restarted.close()

    asyncio.run(scenario())
