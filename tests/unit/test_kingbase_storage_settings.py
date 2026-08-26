from __future__ import annotations

from pathlib import Path

import pytest

from auraclaw.config import Settings, apply_kingbase_env_aliases
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.sql_dialect import detect_dialect, normalize_database_url

_KINGBASE_KEYS = (
    "KINGBASE_HOST",
    "KINGBASE_PORT",
    "KINGBASE_DB_USER",
    "KINGBASE_DB_PWD",
    "KINGBASE_AURACLAW_DB",
)


def test_kingbase_backend_resolves_postgres_dialect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "kingbase")
    monkeypatch.setenv("DB_HOST", "10.0.0.1")
    monkeypatch.setenv("DB_PORT", "54321")
    monkeypatch.setenv("DB_USER", "root")
    monkeypatch.setenv("DB_PWD", "secret")
    monkeypatch.setenv("DB_NAME", "auraclaw")
    settings = Settings(_env_file=None)
    assert settings.kingbase_enabled is True
    assert settings.postgres_enabled is True
    assert settings.mysql_enabled is False
    assert settings.resolved_db_dialect == "postgres"
    assert settings.storage_label == "kingbase"
    assert settings.resolved_database_url.startswith("postgresql+asyncpg://")
    assert "@10.0.0.1:54321/auraclaw" in settings.resolved_database_url


def test_kingbase_env_aliases_overwrite_db_when_backend_kingbase(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_file = tmp_path / ".env.test"
    env_file.write_text(
        "\n".join(
            [
                "AURACLAW_STORAGE_BACKEND=kingbase",
                "KINGBASE_HOST=10.244.72.1",
                "KINGBASE_PORT=54321",
                "KINGBASE_DB_USER=kb_user",
                "KINGBASE_DB_PWD=Chain@2026",
                "KINGBASE_AURACLAW_DB=chaintower_agent",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("AURACLAW_KINGBASE_ENV_FILE", raising=False)
    for key in _KINGBASE_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_HOST", "mysql-host")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_USER", "mysql_user")
    monkeypatch.setenv("DB_PWD", "mysql_pwd")
    monkeypatch.setenv("DB_NAME", "auraclaw_dev")

    apply_kingbase_env_aliases(settings_env_file=env_file)
    settings = Settings(_env_file=None)
    assert settings.db_host == "10.244.72.1"
    assert settings.db_port == 54321
    assert settings.db_user == "kb_user"
    assert settings.db_password == "Chain@2026"
    assert settings.db_name == "chaintower_agent"
    assert "Chain%402026" in settings.resolved_database_url


def test_kingbase_inline_db_credentials_in_settings_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_file = tmp_path / ".env.test"
    env_file.write_text(
        "\n".join(
            [
                "AURACLAW_STORAGE_BACKEND=kingbase",
                "DB_HOST=10.244.72.1",
                "DB_PORT=54321",
                "DB_USER=kb_user",
                "DB_PWD=Chain@2026",
                "DB_NAME=chaintower_agent",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    for key in (*_KINGBASE_KEYS, "DB_HOST", "DB_PORT", "DB_USER", "DB_PWD", "DB_NAME"):
        monkeypatch.delenv(key, raising=False)

    apply_kingbase_env_aliases(settings_env_file=env_file)
    settings = Settings(_env_file=env_file)
    assert settings.db_host == "10.244.72.1"
    assert settings.kingbase_enabled is True


def test_kingbase_aliases_do_not_override_mysql_backend(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_file = tmp_path / ".env.dev"
    env_file.write_text(
        "AURACLAW_STORAGE_BACKEND=mysql\n"
        "KINGBASE_HOST=10.244.72.1\nKINGBASE_PORT=54321\n"
        "KINGBASE_DB_USER=kb\nKINGBASE_DB_PWD=p\nKINGBASE_AURACLAW_DB=kbdb\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "mysql")
    monkeypatch.setenv("DB_HOST", "mysql-host")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_USER", "mysql_user")
    monkeypatch.setenv("DB_PWD", "mysql_pwd")
    monkeypatch.setenv("DB_NAME", "auraclaw_dev")

    apply_kingbase_env_aliases(settings_env_file=env_file)
    settings = Settings(_env_file=None)
    assert settings.mysql_enabled is True
    assert settings.db_host == "mysql-host"
    assert settings.resolved_database_url.startswith("mysql+aiomysql://")


def test_detect_and_normalize_kingbase_urls() -> None:
    assert detect_dialect("kingbase://u:p@h:54321/db") == "postgres"
    assert detect_dialect("kingbase+asyncpg://u:p@h:54321/db") == "postgres"
    assert (
        normalize_database_url("kingbase+asyncpg://u:p@h/db") == "postgresql://u:p@h/db"
    )
    assert asyncpg_url("kingbase://u:p@h/db") == "postgresql://u:p@h/db"


def test_settings_rewrites_kingbase_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "kingbase")
    monkeypatch.setenv(
        "AURACLAW_DATABASE_URL",
        "kingbase+asyncpg://root:secret@10.244.72.1:54321/chaintower_agent",
    )
    for key in ("DB_HOST", "DB_USER", "DB_PWD", "DB_NAME"):
        monkeypatch.delenv(key, raising=False)
    settings = Settings(_env_file=None)
    assert settings.resolved_database_url.startswith("postgresql+asyncpg://")
    assert "10.244.72.1:54321/chaintower_agent" in settings.resolved_database_url
