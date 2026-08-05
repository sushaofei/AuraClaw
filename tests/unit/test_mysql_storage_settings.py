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
    """mark_outbox_failed must emit capped POWER SQL even for huge publish_attempt."""
    import asyncio

    from auraclaw.infrastructure.persistence.postgres_event_store import (
        PostgresEventStore,
    )

    class _RecordingPool:
        def __init__(self) -> None:
            self.statements: list[tuple[str, tuple[object, ...]]] = []

        async def execute(self, sql: str, *args: object) -> str:
            self.statements.append((sql, args))
            return "UPDATE 1"

    async def exercise(dialect: str) -> None:
        store = PostgresEventStore.__new__(PostgresEventStore)
        store._dialect = dialect  # type: ignore[attr-defined]
        pool = _RecordingPool()

        async def _pool() -> _RecordingPool:
            return pool

        store.pool = _pool  # type: ignore[method-assign]
        # High attempt used to overflow MySQL DOUBLE via POWER(2, publish_attempt).
        await store.mark_outbox_failed(outbox_id=42)
        assert len(pool.statements) == 1
        sql, args = pool.statements[0]
        assert args == (42,)
        normalized = "".join(sql.upper().split())
        assert "POWER(2,LEAST(PUBLISH_ATTEMPT,6))" in normalized
        assert "POWER(2,PUBLISH_ATTEMPT)" not in normalized
        for attempt in (0, 1, 6, 7, 1024, 10_000):
            delay = min(60, 2 ** min(attempt, 6))
            assert delay <= 60
            if attempt >= 6:
                assert delay == 60

    asyncio.run(exercise("mysql"))
    asyncio.run(exercise("postgres"))


_NEXT_SEQUENCE_SQL = """
INSERT INTO streaming.session_sequence
       (tenant_id, session_id, last_sequence)
   VALUES ($1, $2, 1)
   ON CONFLICT (tenant_id, session_id) DO UPDATE SET
       last_sequence = streaming.session_sequence.last_sequence + 1,
       updated_at = now()
   RETURNING last_sequence
""".strip()


def test_insert_returning_followup_for_session_sequence() -> None:
    from auraclaw.infrastructure.persistence.mysql_pool import _insert_returning_followup

    followup = _insert_returning_followup(_NEXT_SEQUENCE_SQL, ("local", "ses_1"))
    assert followup is not None
    select_sql, select_args = followup
    assert select_sql == (
        "SELECT last_sequence FROM streaming.session_sequence "
        "WHERE tenant_id=$1 AND session_id=$2"
    )
    assert select_args == ("local", "ses_1")


def test_insert_returning_followup_for_conflict_do_nothing() -> None:
    from auraclaw.infrastructure.persistence.mysql_pool import _insert_returning_followup

    sql = (
        "INSERT INTO projection.processed_event (projector_id, event_id) "
        "VALUES ('task', $1) ON CONFLICT DO NOTHING RETURNING event_id"
    )
    followup = _insert_returning_followup(sql, ("evt_1",))
    assert followup is not None
    select_sql, select_args = followup
    assert "projector_id='task'" in select_sql
    assert "event_id=$1" in select_sql
    assert select_args == ("evt_1",)


def test_mysql_fetchval_insert_returning_reads_real_sequence() -> None:
    """Regression for #40: INSERT RETURNING must not hardcode '1'."""
    import asyncio

    from auraclaw.infrastructure.persistence.mysql_pool import MysqlConnection

    class _FakeCursor:
        def __init__(self, owner: _FakeRaw) -> None:
            self._owner = owner
            self.rowcount = 0
            self._rows: list[dict[str, object]] = []

        async def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
            self._owner.statements.append((sql, params))
            normalized = " ".join(sql.upper().split())
            if normalized.startswith("INSERT"):
                self._owner.sequence += 1
                self.rowcount = 1 if self._owner.sequence == 1 else 2
                self._rows = []
                return
            if "SELECT LAST_SEQUENCE FROM" in normalized:
                self.rowcount = 1
                self._rows = [{"last_sequence": self._owner.sequence}]
                return
            raise AssertionError(f"unexpected SQL: {sql}")

        async def fetchall(self) -> list[dict[str, object]]:
            return list(self._rows)

        async def __aenter__(self) -> _FakeCursor:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

    class _FakeRaw:
        def __init__(self) -> None:
            self.sequence = 0
            self.statements: list[tuple[str, tuple[object, ...]]] = []

        def cursor(self, *_args: object, **_kwargs: object) -> _FakeCursor:
            return _FakeCursor(self)

    async def exercise() -> None:
        raw = _FakeRaw()
        connection = MysqlConnection(raw)  # type: ignore[arg-type]
        values = [
            await connection.fetchval(_NEXT_SEQUENCE_SQL, "local", "ses_seq")
            for _ in range(3)
        ]
        assert values == [1, 2, 3]
        assert raw.sequence == 3
        assert any("SELECT" in sql.upper() for sql, _ in raw.statements)

    asyncio.run(exercise())


def test_mysql_fetchval_insert_returning_do_nothing_miss_returns_none() -> None:
    import asyncio

    from auraclaw.infrastructure.persistence.mysql_pool import MysqlConnection

    class _FakeCursor:
        rowcount = 0

        async def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
            del sql, params
            self.rowcount = 0

        async def fetchall(self) -> list[dict[str, object]]:
            return []

        async def __aenter__(self) -> _FakeCursor:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

    class _FakeRaw:
        def cursor(self, *_args: object, **_kwargs: object) -> _FakeCursor:
            return _FakeCursor()

    async def exercise() -> None:
        connection = MysqlConnection(_FakeRaw())  # type: ignore[arg-type]
        sql = (
            "INSERT INTO projection.processed_event (projector_id, event_id) "
            "VALUES ('task', $1) ON CONFLICT DO NOTHING RETURNING event_id"
        )
        assert await connection.fetchval(sql, "evt_dup") is None

    asyncio.run(exercise())
