import asyncio
import os
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from auraclaw.composition import cli
from auraclaw.config import Settings
from auraclaw.infrastructure.persistence.migration_runner import (
    MigrationError,
    PostgresMigrationRunner,
    discover_migrations,
)
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("state", ["ready", "missing", "pending", "drifted", "newer"])
def test_schema_check_is_read_only_and_closes_its_connection(tmp_path, monkeypatch, state):
    (tmp_path / "0001_initial.sql").write_text("SELECT 1;")
    migration, = discover_migrations(tmp_path)
    installed = {"0001": migration.checksum}
    if state == "pending":
        installed = {}
    elif state == "drifted":
        installed["0001"] = "other-checksum"
    elif state == "newer":
        installed["0002"] = "future"

    @asynccontextmanager
    async def transaction(*, readonly):
        assert readonly is True
        yield

    connection = SimpleNamespace(
        transaction=transaction,
        fetchval=AsyncMock(return_value=None if state == "missing" else "schema_migration"),
        fetch=AsyncMock(return_value=[
            {"version": version, "checksum": checksum} for version, checksum in installed.items()
        ]),
        close=AsyncMock(),
    )
    connect = AsyncMock(return_value=connection)
    monkeypatch.setattr(asyncpg, "connect", connect)
    runner = PostgresMigrationRunner("postgresql://unused/test", tmp_path)
    if state == "ready":
        asyncio.run(runner.check())
    else:
        with pytest.raises(MigrationError):
            asyncio.run(runner.check())
    connection.close.assert_awaited_once()
    connect.reset_mock()
    with pytest.raises(MigrationError, match="does not match"):
        asyncio.run(runner.check("0000"))
    connect.assert_not_called()


def test_startup_checks_database_owners_but_not_agent_runtime(monkeypatch):
    check = AsyncMock(side_effect=MigrationError("migration 0058 is pending"))
    monkeypatch.setattr(cli, "create_migration_runner", lambda *_args: SimpleNamespace(check=check))
    settings = Settings(storage_backend="postgres")
    with pytest.raises(MigrationError, match="pending"):
        cli._check_service_schema("api", settings)
    cli._check_service_schema("runtime", settings)
    check.assert_awaited_once()


@pytest.mark.parametrize("repeat", [False, True])
def test_mcp_catalog_read_renews_stale_pool_and_retries_only_once(monkeypatch, repeat):
    error = asyncpg.FeatureNotSupportedError("cached plan must not change result type")
    pool = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=[error, error if repeat else None]),
        expire_connections=AsyncMock(),
    )
    store = PostgresCapabilityCatalogStore("postgresql://unused/test")
    monkeypatch.setattr(store, "pool", AsyncMock(return_value=pool))
    if repeat:
        with pytest.raises(asyncpg.FeatureNotSupportedError):
            asyncio.run(store.get_server("mcp"))
    else:
        assert asyncio.run(store.get_server("mcp")) is None
    pool.expire_connections.assert_awaited_once()
    assert pool.fetchrow.await_count == 2


def test_mcp_catalog_does_not_retry_unrelated_database_errors(monkeypatch):
    pool = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=asyncpg.FeatureNotSupportedError("unsupported feature")),
        expire_connections=AsyncMock(),
    )
    store = PostgresCapabilityCatalogStore("postgresql://unused/test")
    monkeypatch.setattr(store, "pool", AsyncMock(return_value=pool))
    with pytest.raises(asyncpg.FeatureNotSupportedError):
        asyncio.run(store.get_server("mcp"))
    pool.expire_connections.assert_not_awaited()


@pytest.mark.parametrize("mode", ["normal", "stale_target", "migration_failure", "build_only"])
def test_deployment_orders_stop_migrate_check_recreate_and_fails_closed(tmp_path, mode):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "dev_service_deploy.sh"
    script.write_text((ROOT / "scripts/dev_service_deploy.sh").read_text())
    (tmp_path / ".host.env").write_text("DEV_SERVICE_HOST=test.invalid\nDEV_SERVICE_USER=test\n")
    log = tmp_path / "commands"
    ssh = tmp_path / "ssh"
    ssh.write_text(
        "#!/usr/bin/env python3\n"
        "import os,sys\n"
        "from pathlib import Path\n"
        "command=sys.argv[-1]\n"
        "with Path(os.environ['COMMAND_LOG']).open('a') as out: out.write(command+'\\n')\n"
        "if 'migrate latest' in command: print('0058')\n"
        "if os.environ['TEST_MODE']=='migration_failure' and command.endswith('-T migrate'): "
        "sys.exit(1)\n"
    )
    ssh.chmod(0o755)
    result = subprocess.run(
        ["bash", str(script), "--skip-sync", "--skip-build", "--skip-health",
         *(["--skip-up"] if mode == "build_only" else [])],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
             "COMMAND_LOG": str(log), "TEST_MODE": mode,
             "AURACLAW_MIGRATE_TARGET": "0055" if mode == "stale_target" else "0058"},
        capture_output=True, text=True,
    )
    commands = log.read_text().splitlines()
    stops = [i for i, value in enumerate(commands) if value.endswith(" stop")]
    starts = [i for i, value in enumerate(commands) if "up -d --force-recreate" in value]
    if mode in {"stale_target", "build_only"}:
        assert not stops and not starts
        assert result.returncode == (1 if mode == "stale_target" else 0)
    elif mode == "migration_failure":
        assert stops and not starts and result.returncode != 0
    else:
        assert result.returncode == 0, result.stderr
        migrate = next(i for i, value in enumerate(commands) if value.endswith("-T migrate"))
        check = next(i for i, value in enumerate(commands) if "migrate check" in value)
        assert stops[0] < migrate < check < starts[0]
        assert "AURACLAW_IMAGE=auraclaw:dev" in commands[starts[0]]


def test_deployment_defaults_match_the_latest_migration():
    latest = discover_migrations(ROOT / "migrations")[-1].version
    for relative in ("compose.prod.yml", "compose.test.yml", "scripts/dev_service_deploy.sh"):
        assert f"AURACLAW_MIGRATE_TARGET:-{latest}" in (ROOT / relative).read_text()
    for relative in (".env.test.example", ".env.prod.example"):
        assert f"AURACLAW_MIGRATE_TARGET={latest}" in (ROOT / relative).read_text()
    assert f'CURRENT_MIGRATION_TARGET = "{latest}"' in (
        ROOT / "scripts/sync_kingbase_env.py"
    ).read_text()


def test_hands_schema_check_uses_absolute_compose_directory(tmp_path, monkeypatch):
    import yaml

    for name in ("compose.test.yml", "compose.prod.yml"):
        compose = yaml.safe_load((ROOT / name).read_text())
        hands = compose["services"]["action-hands"]
        assert hands["working_dir"] == "/workspace"
        assert hands["environment"]["AURACLAW_MIGRATIONS_DIRECTORY"] == (
            "${AURACLAW_MIGRATIONS_DIRECTORY:-/app/migrations}"
        )
    check = AsyncMock()
    directories = []

    def runner(_url, directory):
        directories.append(directory)
        return SimpleNamespace(check=check)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AURACLAW_MIGRATIONS_DIRECTORY", "/app/migrations")
    monkeypatch.setattr(cli, "create_migration_runner", runner)
    cli._check_service_schema("hands", Settings(storage_backend="postgres"))
    assert directories == [Path("/app/migrations")]
    check.assert_awaited_once()
