from pathlib import Path

from dotenv import dotenv_values
from scripts.sync_kingbase_env import ensure_database_name, sync_environment


def test_sync_environment_replaces_postgresql_database_settings(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env.test"
    env_file.write_text(
        "AURACLAW_STORAGE_BACKEND=postgres\n"
        "AURACLAW_MIGRATIONS_DIRECTORY=/app/migrations\n"
        "AURACLAW_MIGRATE_TARGET=0040\n"
        "# --- Database ---\n"
        "AURACLAW_DB_DIALECT=postgres\n"
        "DB_HOST=postgres.internal\n"
        "DB_PORT=5432\n"
        "DB_USER=root\n"
        "DB_PWD=old-password\n"
        "DB_NAME=auraclaw_dev\n"
        "AURACLAW_DATABASE_URL=postgresql+asyncpg://old\n"
        "AURACLAW_MIGRATION_DATABASE_URL=postgresql+asyncpg://old\n"
        "# --- Workload tokens / Agent Context ---\n"
        "AURACLAW_RUNTIME_WORKLOAD_TOKEN=test-token\n",
        encoding="utf-8",
    )
    sync_environment(
        env_file,
        {
            "KINGBASE_HOST": "10.244.72.1",
            "KINGBASE_PORT": "54321",
            "KINGBASE_USER": "kb-user",
            "KINGBASE_PWD": "P@ss word",
            "KINGBASE_AURACLAW_DB": "chaintower_agent",
        },
    )

    values = dotenv_values(env_file)
    assert values["AURACLAW_STORAGE_BACKEND"] == "kingbase"
    assert values["AURACLAW_DB_DIALECT"] == "postgres"
    assert values["DB_HOST"] == "10.244.72.1"
    assert values["DB_PORT"] == "54321"
    assert values["DB_NAME"] == "chaintower_agent"
    assert values["AURACLAW_MIGRATIONS_DIRECTORY"] == "/app/migrations"
    assert values["AURACLAW_MIGRATE_TARGET"] == "0054"
    assert values["AURACLAW_DATABASE_URL"] == (
        "postgresql+asyncpg://kb-user:P%40ss%20word@"
        "10.244.72.1:54321/chaintower_agent"
    )
    assert values["AURACLAW_MIGRATION_DATABASE_URL"] == values[
        "AURACLAW_DATABASE_URL"
    ]
    assert values["AURACLAW_RUNTIME_WORKLOAD_TOKEN"] == "test-token"


def test_ensure_database_name_adds_safe_default(tmp_path: Path) -> None:
    host_env = tmp_path / ".host.env"
    host_env.write_text("KINGBASE_HOST=10.244.72.1\n", encoding="utf-8")
    values = dict(dotenv_values(host_env))

    ensure_database_name(host_env, values)
    ensure_database_name(host_env, values)

    assert values["KINGBASE_AURACLAW_DB"] == "chaintower_agent"
    assert host_env.read_text(encoding="utf-8").count("KINGBASE_AURACLAW_DB=") == 1
    assert host_env.stat().st_mode & 0o777 == 0o600
