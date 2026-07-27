from __future__ import annotations

import pytest

from auraclaw.config import Settings
from auraclaw.infrastructure.persistence.mysql_pool import _prepare_mysql_sql
from auraclaw.infrastructure.persistence.sql_dialect import detect_dialect, table


def test_default_dialect_is_mysql() -> None:
    settings = Settings(
        _env_file=None,
        storage_backend="memory",
        database_url="mysql+aiomysql://auraclaw:auraclaw@localhost:3306/auraclaw",
    )
    assert settings.db_dialect == "mysql"
    assert settings.database_url.startswith("mysql+")


def test_auto_with_db_parts_enables_mysql_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "auto")
    monkeypatch.setenv("AURACLAW_DB_DIALECT", "mysql")
    monkeypatch.setenv("AURACLAW_DATABASE_URL", "mysql+aiomysql://unused:unused@localhost:3306/unused")
    monkeypatch.setenv("DB_HOST", "127.0.0.1")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PWD", "p")
    monkeypatch.setenv("DB_NAME", "auraclaw")
    settings = Settings(_env_file=None)
    assert settings.sql_storage_enabled is True
    assert settings.mysql_enabled is True
    assert settings.postgres_enabled is False
    assert settings.storage_label == "mysql"
    assert settings.resolved_database_url.startswith("mysql+aiomysql://")


def test_explicit_postgres_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURACLAW_STORAGE_BACKEND", "postgres")
    monkeypatch.setenv("DB_HOST", "127.0.0.1")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PWD", "p")
    monkeypatch.setenv("DB_NAME", "auraclaw")
    settings = Settings(_env_file=None)
    assert settings.postgres_enabled is True
    assert settings.mysql_enabled is False
    assert settings.storage_label == "postgres"
    assert "postgresql+asyncpg://" in settings.resolved_database_url


def test_detect_dialect_from_url() -> None:
    assert detect_dialect("mysql+aiomysql://a:b@h/db") == "mysql"
    assert detect_dialect("postgresql+asyncpg://a:b@h/db") == "postgres"


def test_table_helper() -> None:
    assert table("session_core", "outbox", "postgres") == "session_core.outbox"
    assert table("session_core", "outbox", "mysql") == "`session_core_outbox`"


def test_mysql_sql_adapts_conflict_and_schema() -> None:
    sql = _prepare_mysql_sql(
        "INSERT INTO session_core.snapshot "
        "(tenant_id, session_id, aggregate_version, schema_version, state) "
        "VALUES ($1, $2, $3, $4, $5::jsonb) "
        "ON CONFLICT (tenant_id, session_id, aggregate_version) DO NOTHING"
    )
    assert "`session_core_snapshot`" in sql
    assert "INSERT IGNORE" in sql.upper()
    assert "ON CONFLICT" not in sql.upper()
    assert "::jsonb" not in sql


def test_mysql_sql_uses_row_alias_not_values_fn() -> None:
    sql = _prepare_mysql_sql(
        "INSERT INTO control.runtime_instance "
        "(runtime_id,runtime_type,role,node_id,capabilities,status,capacity) "
        "VALUES ($1,$2,$3,$4,$5::jsonb,'ready',$6) "
        "ON CONFLICT (runtime_id) DO UPDATE SET status='ready', "
        "capabilities=EXCLUDED.capabilities, capacity=EXCLUDED.capacity, "
        "last_heartbeat_at=now()"
    )
    assert "AS excluded ON DUPLICATE KEY UPDATE" in sql
    assert "excluded.capabilities" in sql
    assert "excluded.capacity" in sql
    assert "VALUES(" not in sql.upper().split("ON DUPLICATE")[-1]
    assert "ON CONFLICT" not in sql.upper()


def test_mysql_sql_moves_limit_before_for_update() -> None:
    sql = _prepare_mysql_sql(
        "SELECT task_id FROM control.runnable_item "
        "ORDER BY priority DESC FOR UPDATE SKIP LOCKED LIMIT $1"
    )
    assert "LIMIT $1 FOR UPDATE SKIP LOCKED" in sql
    assert "FOR UPDATE SKIP LOCKED LIMIT" not in sql.upper()


def test_mysql_sql_preserves_interval_cast_as_date_add() -> None:
    sql = _prepare_mysql_sql(
        "INSERT INTO streaming.connection_registry "
        "(connection_id, tenant_id, session_id, owner_id, cursor_sequence, expires_at) "
        "VALUES ($1, $2, $3, $4, $5, now() + $6::interval)"
    )
    assert "DATE_ADD(UTC_TIMESTAMP(6), INTERVAL $6 MICROSECOND)" in sql
    assert "erval" not in sql
    assert "::interval" not in sql.lower()


def test_mysql_sql_cast_word_boundary_keeps_integer_and_jsonb() -> None:
    assert "::integer" not in _prepare_mysql_sql("SELECT $1::integer").lower()
    assert "erval" not in _prepare_mysql_sql("SELECT now() + $1::interval")
    assert "jsonb" not in _prepare_mysql_sql("SELECT $1::jsonb").lower()
    prepared_json = _prepare_mysql_sql("SELECT $1::json")
    assert "::json" not in prepared_json.lower()


def test_mysql_sql_adapts_json_arrow_operators() -> None:
    sql = _prepare_mysql_sql(
        "SELECT count(*) FROM session_core.outbox "
        "WHERE $1::text IS NULL OR payload->>'tenant_id'=$1"
    )
    assert "JSON_UNQUOTE(JSON_EXTRACT(payload, '$.tenant_id'))" in sql
    assert "->>" not in sql
    assert "::text" not in sql.lower()


def test_mysql_any_expand_keeps_space_before_in() -> None:
    from auraclaw.infrastructure.persistence.mysql_pool import _compile

    sql, params = _compile(
        "SELECT 1 FROM t WHERE capability_id=ANY($1::text[]) AND server_id<>$2",
        (["a", "b"], "srv"),
    )
    assert "capability_id IN (%s, %s)" in sql
    assert "capability_idIN" not in sql
    assert params == ("a", "b", "srv")


def test_mysql_quotes_reserved_usage_column() -> None:
    from auraclaw.infrastructure.persistence.mysql_pool import _compile

    prepared = _prepare_mysql_sql(
        "UPDATE model_gateway.model_call SET status='completed',"
        "provider=$3,model=$4,usage=$5::jsonb,response=$6::jsonb,updated_at=now() "
        "WHERE tenant_id=$1 AND model_call_id=$2"
    )
    assert "`usage`=$5" in prepared
    assert "`model_gateway_model_call`" in prepared
    # Table containing "usage" in the name must stay intact.
    budget = _prepare_mysql_sql(
        "UPDATE model_gateway.usage_budget SET tokens_used=tokens_used+$2 WHERE tenant_id=$1"
    )
    assert "`model_gateway_usage_budget`" in budget
    assert "`usage`" not in budget

    sql, params = _compile(
        "UPDATE model_gateway.model_call SET usage=$3::jsonb "
        "WHERE tenant_id=$1 AND model_call_id=$2",
        ("t", "c", '{"input_tokens":1}'),
    )
    assert "`usage`=%s" in sql
    assert params == ('{"input_tokens":1}', "t", "c")


def test_mysql_roles_sql_render_substitutes_database() -> None:
    from auraclaw.infrastructure.persistence.mysql_roles import render_roles_sql

    rendered = render_roles_sql("auraclaw_dev", "s3cret")
    assert "`auraclaw_dev`.`session_core_%`" in rendered
    assert "'s3cret'" in rendered
    assert "`auraclaw`.`" not in rendered


def test_outbox_nack_backoff_caps_exponent_before_power() -> None:
    """MySQL evaluates POWER before LEAST; cap the exponent to avoid DOUBLE overflow."""
    import inspect

    from auraclaw.infrastructure.persistence.postgres_event_store import (
        PostgresEventStore,
    )

    source = inspect.getsource(PostgresEventStore.mark_outbox_failed)
    assert "LEAST(publish_attempt, 6)" in source
    normalized = "".join(source.upper().split())
    assert "POWER(2,LEAST(PUBLISH_ATTEMPT,6))" in normalized
    for attempt in (0, 1, 6, 7, 1024, 10_000):
        delay = min(60, 2 ** min(attempt, 6))
        assert delay <= 60
        if attempt >= 6:
            assert delay == 60
