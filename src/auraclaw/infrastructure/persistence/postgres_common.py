from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from functools import wraps
from typing import Any, Concatenate, ParamSpec, Protocol, TypeVar, cast

import asyncpg  # type: ignore[import-untyped]

from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.state import Visibility
from auraclaw.infrastructure.persistence.sql_dialect import Dialect, detect_dialect

_Result = TypeVar("_Result")
_Store = TypeVar("_Store", bound="LazyPool")
_Params = ParamSpec("_Params")
_RETRYABLE_TRANSACTION_STATES = frozenset({"40001", "40P01"})


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


def retry_serializable_transaction(
    operation: str,
) -> Callable[
    [Callable[Concatenate[_Store, _Params], Awaitable[_Result]]],
    Callable[Concatenate[_Store, _Params], Coroutine[Any, Any, _Result]],
]:
    """Retry an entire idempotent transaction on serialization/deadlock abort."""

    def decorator(
        method: Callable[Concatenate[_Store, _Params], Awaitable[_Result]],
    ) -> Callable[Concatenate[_Store, _Params], Coroutine[Any, Any, _Result]]:
        @wraps(method)
        async def wrapped(
            self: _Store, *args: _Params.args, **kwargs: _Params.kwargs
        ) -> _Result:
            return await self.run_transaction_with_retry(
                operation, lambda: method(self, *args, **kwargs)
            )

        return cast(
            Callable[
                Concatenate[_Store, _Params], Coroutine[Any, Any, _Result]
            ],
            wrapped,
        )

    return decorator


def asyncpg_url(database_url: str) -> str:
    return (
        database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        .replace("kingbase+asyncpg://", "postgresql://", 1)
        .replace("kingbase://", "postgresql://", 1)
    )


def json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def json_loads(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        return json.loads(value.decode())
    if isinstance(value, str):
        return json.loads(value)
    return value


def event_from_record(row: Any) -> CanonicalEvent:
    actor = json_loads(row["actor"])
    occurred_at = row["occurred_at"]
    return CanonicalEvent(
        event_id=str(row["event_id"]),
        tenant_id=str(row["tenant_id"]),
        root_session_id=str(row["root_session_id"]),
        session_id=str(row["session_id"]),
        run_id=str(row["run_id"]) if row["run_id"] is not None else None,
        aggregate_version=int(row["aggregate_version"]),
        type=str(row["event_type"]),
        occurred_at=occurred_at,
        actor=Actor(type=str(actor["type"]), id=str(actor["id"])),
        correlation_id=str(row["correlation_id"]),
        causation_id=str(row["causation_id"]),
        visibility=Visibility(str(row["visibility"])),
        schema_version=int(row["schema_version"]),
        payload=dict(json_loads(row["payload"])),
    )


class LazyPool:
    """Lazy asyncpg pool for PostgreSQL and Kingbase compatibility mode."""

    def __init__(
        self,
        database_url: str,
        dialect: Dialect | None = None,
        *,
        transaction_retry_attempts: int = 3,
        transaction_retry_base_delay: float = 0.01,
        metric_writer: MetricWriter | None = None,
    ) -> None:
        if transaction_retry_attempts < 1 or transaction_retry_base_delay < 0:
            raise ValueError("Transaction retry policy is invalid")
        self._dialect: Dialect = dialect or detect_dialect(database_url)
        self._database_url = database_url
        self._postgres_url = asyncpg_url(database_url)
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()
        self._transaction_retry_attempts = transaction_retry_attempts
        self._transaction_retry_base_delay = transaction_retry_base_delay
        self._transaction_metric_writer = metric_writer

    @property
    def dialect(self) -> Dialect:
        return self._dialect

    async def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    self._pool = await asyncpg.create_pool(
                        self._postgres_url,
                        min_size=1,
                        max_size=5,
                        command_timeout=30,
                    )
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def run_transaction_with_retry(
        self, operation: str, callback: Callable[[], Awaitable[_Result]]
    ) -> _Result:
        for attempt in range(1, self._transaction_retry_attempts + 1):
            try:
                return await callback()
            except asyncpg.PostgresError as exc:
                sqlstate = getattr(exc, "sqlstate", None)
                if sqlstate not in _RETRYABLE_TRANSACTION_STATES:
                    raise
                await self._emit_transaction_metric(
                    "postgres.transaction.retry",
                    operation,
                    sqlstate,
                )
                if attempt >= self._transaction_retry_attempts:
                    await self._emit_transaction_metric(
                        "postgres.transaction.retry_exhausted",
                        operation,
                        sqlstate,
                    )
                    raise VersionConflictError(
                        "Database transaction retry budget was exhausted",
                        detail=operation,
                        retry_after=1,
                    ) from exc
                delay = self._transaction_retry_base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay * (0.5 + random.random()))
        raise AssertionError("unreachable transaction retry state")

    async def _emit_transaction_metric(
        self, name: str, operation: str, sqlstate: str
    ) -> None:
        if self._transaction_metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._transaction_metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=1.0,
                        observed_at=datetime.now(UTC),
                        labels={"operation": operation, "sqlstate": sqlstate},
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return
