from __future__ import annotations

import asyncio
import os
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import pytest

from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.config import get_settings
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.mcp_registry import (
    McpDesiredState,
    McpServerConfig,
    McpServerLifecycleCommand,
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
pytestmark = pytest.mark.skipif(
    SQL_URL is None, reason="SQL registry test URL not configured"
)


async def _apply_registry_migration(database_url: str) -> None:
    pool_holder = LazyPool(database_url)
    pool = await pool_holder.pool()
    try:
        async with pool.acquire() as connection:
            existing = await connection.fetchval(
                """SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema='hands' AND table_name='mcp_server'"""
            )
            if int(existing or 0) > 0:
                return
            sql = (ROOT / "migrations/0020_mcp_server_registry.sql").read_text()
            for statement in _split_sql(sql):
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
    await pool.execute(
        "DELETE FROM hands.mcp_server_operation WHERE server_id=$1", server_id
    )
    await pool.execute(
        "DELETE FROM hands.mcp_server_runtime WHERE server_id=$1", server_id
    )
    await pool.execute(
        "DELETE FROM hands.mcp_server_revision WHERE server_id=$1", server_id
    )
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
                allowed_tool_prefixes=("demo.",),
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
