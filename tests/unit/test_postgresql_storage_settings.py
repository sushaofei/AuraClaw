from __future__ import annotations

from pathlib import Path

import pytest

from auraclaw.config import Settings, apply_postgresql_env_aliases

_POSTGRESQL_KEYS = (
    "POSTGRESQL_HOST",
    "POSTGRESQL_PORT",
    "POSTGRESQL_DB_USER",
    "POSTGRESQL_DB_PWD",
    "POSTGRESQL_AURACLAW_DB",
)


def test_postgres_backend_resolves_local_dialect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "postgres")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_USER", "chaintower_admin")
    monkeypatch.setenv("DB_PWD", "Chain@2026")
    monkeypatch.setenv("DB_NAME", "chaintower_agent")
    settings = Settings(_env_file=None)
    assert settings.postgres_enabled is True
    assert settings.mysql_enabled is False
    assert settings.kingbase_enabled is False
    assert settings.resolved_db_dialect == "postgres"
    assert settings.storage_label == "postgres"
    assert settings.resolved_database_url.startswith("postgresql+asyncpg://")
    assert "@localhost:5432/chaintower_agent" in settings.resolved_database_url
    assert "Chain%402026" in settings.resolved_database_url


def test_postgresql_env_aliases_overwrite_db_when_backend_postgres(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    postgresql_env = tmp_path / ".postgresql.local.env"
    postgresql_env.write_text(
        "\n".join(
            [
                "POSTGRESQL_HOST=localhost",
                "POSTGRESQL_PORT=5432",
                "POSTGRESQL_DB_USER=pg_user",
                "POSTGRESQL_DB_PWD=Chain@2026",
                "POSTGRESQL_AURACLAW_DB=chaintower_agent",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AURACLAW_POSTGRESQL_ENV_FILE", str(postgresql_env))
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "postgres")
    for key in _POSTGRESQL_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_HOST", "mysql-host")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_USER", "mysql_user")
    monkeypatch.setenv("DB_PWD", "mysql_pwd")
    monkeypatch.setenv("DB_NAME", "auraclaw_dev")

    apply_postgresql_env_aliases()
    settings = Settings(_env_file=None)
    assert settings.db_host == "localhost"
    assert settings.db_port == 5432
    assert settings.db_user == "pg_user"
    assert settings.db_password == "Chain@2026"
    assert settings.db_name == "chaintower_agent"
    assert "Chain%402026" in settings.resolved_database_url


def test_postgresql_aliases_do_not_override_mysql_backend(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    postgresql_env = tmp_path / ".postgresql.local.env"
    postgresql_env.write_text(
        "POSTGRESQL_HOST=localhost\nPOSTGRESQL_PORT=5432\n"
        "POSTGRESQL_DB_USER=pg\nPOSTGRESQL_DB_PWD=p\nPOSTGRESQL_AURACLAW_DB=pgdb\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AURACLAW_POSTGRESQL_ENV_FILE", str(postgresql_env))
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "mysql")
    monkeypatch.setenv("DB_HOST", "mysql-host")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_USER", "mysql_user")
    monkeypatch.setenv("DB_PWD", "mysql_pwd")
    monkeypatch.setenv("DB_NAME", "auraclaw_dev")

    apply_postgresql_env_aliases()
    settings = Settings(_env_file=None)
    assert settings.mysql_enabled is True
    assert settings.db_host == "mysql-host"
    assert settings.resolved_database_url.startswith("mysql+aiomysql://")
