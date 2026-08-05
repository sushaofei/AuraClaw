from __future__ import annotations

import asyncio
import re
import warnings
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import aiomysql  # type: ignore[import-untyped]

from auraclaw.infrastructure.persistence.sql_dialect import (
    mysql_insert_ignore,
    normalize_database_url,
    parse_mysql_url,
)

_PLACEHOLDER = re.compile(r"\$(\d+)")
_ANY_OR_PLACEHOLDER = re.compile(
    r"ANY\(\$(\d+)(?:::[a-zA-Z0-9_\[\]]+)?\)|\$(\d+)",
    re.IGNORECASE,
)
_SCHEMA_TABLE = re.compile(
    r"\b("
    r"session_core|projection|control|delivery|artifact|security|observability|"
    r"streaming|model_gateway|hands|policy|credential|auraclaw_meta"
    r")\.([a-zA-Z_][a-zA-Z0-9_]*)\b"
)
_PG_CAST = re.compile(
    r"::(?:jsonb|json|text\[\]|timestamptz|interval|integer|bigint|boolean|int|text)\b"
)
_INTERVAL_LITERAL = re.compile(r"interval\s+'([^']+)'", re.IGNORECASE)
_UPDATE_RETURNING = re.compile(
    r"^\s*UPDATE\s+(\S+)(?:\s+(?!SET\b)\w+)?\s+SET\s+.+\s+WHERE\s+(.+?)\s+RETURNING\s+(.+?)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_DELETE_RETURNING = re.compile(
    r"^\s*DELETE\s+FROM\s+(\S+)(?:\s+AS\s+(\w+))?\s+WHERE\s+(.+?)\s+RETURNING\s+(.+?)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_INSERT_RETURNING_HEAD = re.compile(
    r"^\s*INSERT\s+INTO\s+(\S+)\s*"
    r"\(([^)]+)\)\s*"
    r"VALUES\s*\(",
    re.IGNORECASE | re.DOTALL,
)
_INSERT_RETURNING_TAIL = re.compile(
    r"^\s*(?:ON\s+CONFLICT\s*(?:\(([^)]*)\))?\s*DO\s+(?:UPDATE\s+SET\s+.+?|NOTHING)\s*)?"
    r"RETURNING\s+(.+?)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_CONCAT_FOUR = re.compile(
    r"\('([^']*)'\s*\|\|\s*([a-zA-Z_][\w.]*)\s*\|\|\s*'([^']*)'\s*\|\|\s*([a-zA-Z_][\w.]*)\)"
)
_CONCAT_TWO = re.compile(r"'([^']*)'\s*\|\|\s*([a-zA-Z_][\w.]*)")
_JSON_TEXT = re.compile(r"(\w+(?:\.\w+)?)\s*->>\s*'([^']+)'")
_JSON_PATH = re.compile(r"(\w+(?:\.\w+)?)\s*->\s*'([^']+)'")


def _split_sql_csv(expr: str) -> list[str]:
    """Split a comma-separated SQL list, ignoring commas inside parentheses."""
    parts: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(expr):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(expr[start:index].strip())
            start = index + 1
    parts.append(expr[start:].strip())
    return [part for part in parts if part]


def _placeholder_index(expr: str) -> int | None:
    match = re.fullmatch(r"\$(\d+)(?:::[a-zA-Z0-9_\[\]]+)?", expr.strip())
    if match is None:
        return None
    return int(match.group(1))


def _split_values_list(sql: str, start: int) -> tuple[str, int] | None:
    """Return (values_csv, index_after_closing_paren) for VALUES ( ... )."""
    depth = 1
    index = start
    while index < len(sql):
        char = sql[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return sql[start:index], index + 1
        index += 1
    return None


def _insert_returning_followup(
    query: str, args: Sequence[Any]
) -> tuple[str, tuple[Any, ...]] | None:
    """Build SELECT that reads INSERT ... RETURNING columns after a MySQL upsert.

    MySQL has no RETURNING; callers execute the INSERT first, then this SELECT.
    """
    stripped = query.strip()
    head = _INSERT_RETURNING_HEAD.match(stripped)
    if head is None:
        return None
    values_span = _split_values_list(stripped, head.end())
    if values_span is None:
        return None
    values_csv, after_values = values_span
    tail = _INSERT_RETURNING_TAIL.match(stripped[after_values:])
    if tail is None:
        return None
    table = head.group(1)
    columns = _split_sql_csv(head.group(2))
    value_exprs = _split_sql_csv(values_csv)
    if len(columns) != len(value_exprs):
        return None
    conflict_raw = tail.group(1)
    returning = tail.group(2).strip()
    if conflict_raw is not None and conflict_raw.strip():
        key_columns = _split_sql_csv(conflict_raw)
    else:
        key_columns = list(columns)
    column_to_expr = {
        column.split(".")[-1].strip().strip("`"): expr
        for column, expr in zip(columns, value_exprs, strict=True)
    }
    where_parts: list[str] = []
    select_args: list[Any] = []
    for key in key_columns:
        bare = key.split(".")[-1].strip().strip("`")
        expr = column_to_expr.get(bare)
        if expr is None:
            return None
        placeholder = _placeholder_index(expr)
        if placeholder is None:
            where_parts.append(f"{bare}={expr}")
            continue
        if placeholder < 1 or placeholder > len(args):
            return None
        select_args.append(args[placeholder - 1])
        where_parts.append(f"{bare}=${len(select_args)}")
    if not where_parts:
        return None
    if returning == "*" or returning.endswith(".*"):
        select_list = "*"
    elif "." in returning:
        select_list = ", ".join(
            part.strip().split(".")[-1] for part in _split_sql_csv(returning)
        )
    else:
        select_list = returning
    select_sql = f"SELECT {select_list} FROM {table} WHERE {' AND '.join(where_parts)}"
    return select_sql, tuple(select_args)


# Column names that are MySQL reserved words (must be backtick-quoted).
# Applied after schema.table → `schema_table` rewrite so table names like
# `model_gateway_usage_budget` are not mangled.
_MYSQL_RESERVED_COLUMNS = ("usage",)
_RESERVED_COLUMN = re.compile(
    r"(?<![`\w])(?:" + "|".join(_MYSQL_RESERVED_COLUMNS) + r")(?![`\w])",
    re.IGNORECASE,
)


def _quote_mysql_reserved_columns(sql: str) -> str:
    return _RESERVED_COLUMN.sub(lambda match: f"`{match.group(0).lower()}`", sql)


def _convert_arg(value: Any) -> Any:
    if isinstance(value, timedelta):
        return int(value.total_seconds() * 1_000_000)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (datetime, date)):
        return value
    return value


def _mysql_interval_literal(raw: str) -> str:
    parts = raw.strip().split()
    if len(parts) != 2:
        return f"INTERVAL {raw}"
    amount, unit = parts[0], parts[1].upper().rstrip("S")
    aliases = {
        "SECOND": "SECOND",
        "MINUTE": "MINUTE",
        "HOUR": "HOUR",
        "DAY": "DAY",
        "WEEK": "WEEK",
        "MONTH": "MONTH",
        "YEAR": "YEAR",
    }
    return f"INTERVAL {amount} {aliases.get(unit, unit)}"


def _expand_any(query: str, args: Sequence[Any]) -> tuple[str, tuple[Any, ...]]:
    """Expand ANY($n::text[]) / ANY($n) list args into IN ($i, $j, ...)."""
    if "ANY($" not in query.upper().replace(" ", ""):
        # cheap check; still handle spaced forms below
        if not re.search(r"ANY\s*\(\s*\$", query, re.I):
            return query, tuple(args)

    new_args: list[Any] = []
    pieces: list[str] = []
    last = 0
    for match in _ANY_OR_PLACEHOLDER.finditer(query):
        pieces.append(query[last : match.start()])
        token = match.group(0)
        if token.upper().startswith("ANY"):
            idx = int(match.group(1))
            values = args[idx - 1]
            if isinstance(values, (list, tuple)):
                if not values:
                    pieces.append("IN (SELECT NULL WHERE FALSE)")
                else:
                    placeholders: list[str] = []
                    for value in values:
                        new_args.append(value)
                        placeholders.append(f"${len(new_args)}")
                    pieces.append(f"IN ({', '.join(placeholders)})")
            else:
                new_args.append(values)
                pieces.append(f"= ${len(new_args)}")
        else:
            idx = int(match.group(2))
            new_args.append(args[idx - 1])
            pieces.append(f"${len(new_args)}")
        last = match.end()
    pieces.append(query[last:])
    sql = "".join(pieces)
    # `col=ANY($1)` becomes `col=IN (...)`; normalize to `col IN (...)`.
    sql = re.sub(r"=\s*IN\s*\(", " IN (", sql, flags=re.IGNORECASE)
    return sql, tuple(new_args)


def _prepare_mysql_sql(query: str) -> str:
    sql = query
    upper = sql.upper()
    if "ON CONFLICT" in upper and "DO UPDATE SET" in upper:
        # Conditional upsert WHERE clauses are not portable; callers use dialect SQL.
        # Still translate unconditional ON CONFLICT DO UPDATE for common paths.
        sql = re.sub(
            r"ON CONFLICT\s*\([^)]*\)\s*DO UPDATE SET",
            "ON DUPLICATE KEY UPDATE",
            sql,
            flags=re.IGNORECASE,
        )
        # MySQL 8.0.19+: INSERT ... VALUES (...) AS excluded ON DUPLICATE KEY UPDATE ...
        sql = re.sub(
            r"(VALUES\s*\((?:[^()]|\([^()]*\))*\))\s*ON DUPLICATE KEY UPDATE",
            r"\1 AS excluded ON DUPLICATE KEY UPDATE",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        # INSERT ... SELECT ... ON DUPLICATE (no VALUES clause): still rewrite EXCLUDED.
        sql = re.sub(r"\bEXCLUDED\.(\w+)", r"excluded.\1", sql, flags=re.IGNORECASE)
    elif "ON CONFLICT" in upper and "DO NOTHING" in upper:
        sql = mysql_insert_ignore(sql)
    sql = re.sub(r"\s+RETURNING\s+[\w.\s,*]+$", "", sql, flags=re.IGNORECASE)
    sql = _SCHEMA_TABLE.sub(r"`\1_\2`", sql)
    sql = _quote_mysql_reserved_columns(sql)
    sql = _PG_CAST.sub("", sql)
    sql = _INTERVAL_LITERAL.sub(lambda match: _mysql_interval_literal(match.group(1)), sql)
    # Postgres: make_interval(secs => col) → MySQL: INTERVAL col SECOND
    sql = re.sub(
        r"make_interval\(\s*secs\s*=>\s*([a-zA-Z_][\w.]*)\s*\)",
        r"INTERVAL \1 SECOND",
        sql,
        flags=re.IGNORECASE,
    )
    sql = sql.replace("now()", "UTC_TIMESTAMP(6)")
    sql = re.sub(
        r"UTC_TIMESTAMP\(6\)\s*\+\s*\$(\d+)",
        r"DATE_ADD(UTC_TIMESTAMP(6), INTERVAL $\1 MICROSECOND)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"UTC_TIMESTAMP\(6\)\s*-\s*INTERVAL\s+(\d+)\s+(\w+)",
        r"DATE_SUB(UTC_TIMESTAMP(6), INTERVAL \1 \2)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(\w+(?:\.\w+)?)\s*@>\s*\$(\d+)",
        r"JSON_CONTAINS(\1, $\2)",
        sql,
    )
    sql = _JSON_TEXT.sub(
        r"JSON_UNQUOTE(JSON_EXTRACT(\1, '$.\2'))",
        sql,
    )
    sql = _JSON_PATH.sub(r"JSON_EXTRACT(\1, '$.\2')", sql)
    sql = _CONCAT_FOUR.sub(r"CONCAT('\1', \2, '\3', \4)", sql)
    sql = _CONCAT_TWO.sub(r"CONCAT('\1', \2)", sql)
    # MySQL: LIMIT must precede FOR UPDATE [SKIP LOCKED]; Postgres allows either order.
    sql = re.sub(
        r"FOR UPDATE(?:\s+OF\s+\w+)?\s+SKIP LOCKED\s+LIMIT\s+(\$\d+|\d+)",
        r"LIMIT \1 FOR UPDATE SKIP LOCKED",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"FOR UPDATE(?:\s+OF\s+\w+)?\s+LIMIT\s+(\$\d+|\d+)",
        r"LIMIT \1 FOR UPDATE",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _bind(query: str, args: Sequence[Any]) -> tuple[str, tuple[Any, ...]]:
    matches = list(_PLACEHOLDER.finditer(query))
    if not matches:
        return query, tuple(_convert_arg(arg) for arg in args)
    ordered: list[Any] = []
    pieces: list[str] = []
    last = 0
    for match in matches:
        pieces.append(query[last : match.start()])
        pieces.append("%s")
        ordered.append(_convert_arg(args[int(match.group(1)) - 1]))
        last = match.end()
    pieces.append(query[last:])
    return "".join(pieces), tuple(ordered)


def _compile(query: str, args: Sequence[Any]) -> tuple[str, tuple[Any, ...]]:
    expanded_sql, expanded_args = _expand_any(query, args)
    return _bind(_prepare_mysql_sql(expanded_sql), expanded_args)


class MysqlRecord(dict[str, Any]):
    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class MysqlConnection:
    def __init__(self, connection: aiomysql.Connection) -> None:
        self._connection = connection
        self._in_transaction = False

    async def execute(self, query: str, *args: Any) -> str:
        sql, params = _compile(query, args)
        async with self._connection.cursor(aiomysql.DictCursor) as cursor:
            if sql.lstrip().upper().startswith("INSERT IGNORE"):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", Warning)
                    await cursor.execute(sql, params)
            else:
                await cursor.execute(sql, params)
            if sql.lstrip().upper().startswith("UPDATE"):
                return f"UPDATE {cursor.rowcount}"
            return f"OK {cursor.rowcount}"

    async def fetch(self, query: str, *args: Any) -> list[MysqlRecord]:
        stripped = query.strip()
        upper = stripped.upper()
        if "RETURNING" in upper and upper.startswith("INSERT"):
            followup = _insert_returning_followup(query, args)
            result = await self.execute(query, *args)
            count = int(str(result).rsplit(" ", 1)[-1])
            if count == 0:
                return []
            if followup is None:
                raise RuntimeError(
                    "MySQL adapter could not emulate INSERT ... RETURNING"
                )
            select_sql, select_args = followup
            return await self.fetch(select_sql, *select_args)
        if "RETURNING" in upper and upper.startswith("UPDATE"):
            matched = _UPDATE_RETURNING.match(stripped)
            if matched is not None:
                await self.execute(query, *args)
                table, where, returning = matched.group(1), matched.group(2), matched.group(3)
                ret = returning.strip()
                if ret == "*" or ret.endswith(".*"):
                    select = f"SELECT * FROM {table} WHERE {where}"
                elif "." in ret:
                    cols = ", ".join(
                        part.strip().split(".")[-1] for part in ret.split(",")
                    )
                    select = f"SELECT {cols} FROM {table} WHERE {where}"
                else:
                    select = f"SELECT {ret} FROM {table} WHERE {where}"
                return await self.fetch(select, *args)
        if "RETURNING" in upper and upper.startswith("DELETE"):
            matched = _DELETE_RETURNING.match(stripped)
            if matched is not None:
                table = matched.group(1)
                where = matched.group(3)
                returning = matched.group(4).strip()
                if returning == "*" or returning.endswith(".*"):
                    select = f"SELECT * FROM {table} WHERE {where}"
                elif "." in returning:
                    cols = ", ".join(
                        part.strip().split(".")[-1] for part in returning.split(",")
                    )
                    select = f"SELECT {cols} FROM {table} WHERE {where}"
                else:
                    select = f"SELECT {returning} FROM {table} WHERE {where}"
                rows = await self.fetch(select, *args)
                await self.execute(query, *args)
                return rows
        sql, params = _compile(query, args)
        async with self._connection.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
        return [MysqlRecord(row) for row in rows]

    async def fetchrow(self, query: str, *args: Any) -> MysqlRecord | None:
        rows = await self.fetch(query, *args)
        return rows[0] if rows else None

    async def fetchval(self, query: str, *args: Any) -> Any:
        upper = query.lstrip().upper()
        if "RETURNING" in query.upper() and (
            upper.startswith("INSERT")
            or upper.startswith("UPDATE")
            or upper.startswith("DELETE")
        ):
            row = await self.fetchrow(query, *args)
            if row is None:
                return None
            return next(iter(row.values()))
        row = await self.fetchrow(query, *args)
        if row is None:
            return None
        return next(iter(row.values()))

    async def executemany(self, query: str, args_seq: Sequence[Sequence[Any]]) -> str:
        if not args_seq:
            return "OK 0"
        compiled = [_compile(query, tuple(args)) for args in args_seq]
        sql = compiled[0][0]
        params_list = [params for _, params in compiled]
        async with self._connection.cursor(aiomysql.DictCursor) as cursor:
            await cursor.executemany(sql, params_list)
            return f"OK {cursor.rowcount}"

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[MysqlConnection]:
        if self._in_transaction:
            yield self
            return
        await self._connection.begin()
        self._in_transaction = True
        try:
            yield self
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise
        finally:
            self._in_transaction = False


class MysqlPool:
    def __init__(self, pool: aiomysql.Pool) -> None:
        self._pool = pool

    async def execute(self, query: str, *args: Any) -> str:
        async with self.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetch(self, query: str, *args: Any) -> list[MysqlRecord]:
        async with self.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> MysqlRecord | None:
        async with self.acquire() as connection:
            return await connection.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        async with self.acquire() as connection:
            return await connection.fetchval(query, *args)

    async def executemany(self, query: str, args_seq: Sequence[Sequence[Any]]) -> str:
        async with self.acquire() as connection:
            return await connection.executemany(query, args_seq)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[MysqlConnection]:
        async with self._pool.acquire() as raw:
            connection = MysqlConnection(raw)
            if not raw.get_autocommit():
                await raw.autocommit(True)
            yield connection

    async def close(self) -> None:
        self._pool.close()
        await self._pool.wait_closed()


class MysqlLazyPool:
    def __init__(self, database_url: str) -> None:
        self._database_url = normalize_database_url(database_url, "mysql")
        self._pool: MysqlPool | None = None
        self._pool_lock = asyncio.Lock()

    async def pool(self) -> MysqlPool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    params = parse_mysql_url(self._database_url)
                    raw = await aiomysql.create_pool(
                        minsize=1,
                        maxsize=5,
                        connect_timeout=30,
                        init_command="SET time_zone = '+00:00'",
                        **params,
                    )
                    self._pool = MysqlPool(raw)
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
