from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.state import Visibility


def asyncpg_url(database_url: str) -> str:
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


def json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def json_loads(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def event_from_record(row: asyncpg.Record) -> CanonicalEvent:
    actor = json_loads(row["actor"])
    return CanonicalEvent(
        event_id=str(row["event_id"]),
        tenant_id=str(row["tenant_id"]),
        root_session_id=str(row["root_session_id"]),
        session_id=str(row["session_id"]),
        run_id=str(row["run_id"]) if row["run_id"] is not None else None,
        aggregate_version=int(row["aggregate_version"]),
        type=str(row["event_type"]),
        occurred_at=row["occurred_at"],
        actor=Actor(type=str(actor["type"]), id=str(actor["id"])),
        correlation_id=str(row["correlation_id"]),
        causation_id=str(row["causation_id"]),
        visibility=Visibility(str(row["visibility"])),
        schema_version=int(row["schema_version"]),
        payload=dict(json_loads(row["payload"])),
    )


class LazyPool:
    def __init__(self, database_url: str) -> None:
        self._database_url = asyncpg_url(database_url)
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    self._pool = await asyncpg.create_pool(
                        self._database_url,
                        min_size=1,
                        max_size=5,
                        command_timeout=30,
                    )
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
