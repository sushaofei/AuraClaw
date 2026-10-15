from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass

from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.capabilities.state import run_key
from auraclaw.runtime.ports import CapabilityClient


@dataclass(frozen=True)
class _SkillContentEntry:
    text: str
    size: int
    expires_at: float


class SkillContentCache:
    """Own content bytes, per-run in-flight loads and cancellation."""

    def __init__(
        self,
        client: CapabilityClient,
        *,
        max_bytes: int,
        max_entries: int,
        ttl_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_bytes < 1 or max_entries < 1 or ttl_seconds <= 0:
            raise ValueError("Runtime Skill content cache limits must be positive")
        self._client = client
        self._clock = clock
        self._skill_content_cache_max_bytes = max_bytes
        self._skill_content_cache_max_entries = max_entries
        self._skill_content_cache_ttl_seconds = ttl_seconds
        self._skill_content_cache: OrderedDict[
            tuple[str, str, str, str, str, str], _SkillContentEntry
        ] = OrderedDict()
        self._skill_content_loads: dict[
            tuple[str, str, str, str, str, str, str, str, str], asyncio.Task[str]
        ] = {}
        self._skill_content_cache_bytes = 0
        self._skill_content_lock = asyncio.Lock()

    async def load(
        self,
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
        package_digest: str,
        path: str,
    ) -> tuple[str, bool]:
        cache_key = (
            assignment.tenant_id,
            publisher,
            name,
            version,
            package_digest,
            path,
        )
        load_key = (*run_key(assignment), *cache_key)
        now = self._clock()
        async with self._skill_content_lock:
            entry = self._skill_content_cache.get(cache_key)
            if entry is not None and entry.expires_at > now:
                self._skill_content_cache.move_to_end(cache_key)
                return entry.text, True
            if entry is not None:
                self._skill_content_cache.pop(cache_key)
                self._skill_content_cache_bytes -= entry.size
            task = self._skill_content_loads.get(load_key)
            if task is None:
                task = asyncio.create_task(
                    self._fetch_skill_text(
                        cache_key,
                        load_key,
                        assignment,
                        publisher=publisher,
                        name=name,
                        version=version,
                        path=path,
                    )
                )
                self._skill_content_loads[load_key] = task
        return await asyncio.shield(task), False

    async def _fetch_skill_text(
        self,
        cache_key: tuple[str, str, str, str, str, str],
        load_key: tuple[str, str, str, str, str, str, str, str, str],
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
        path: str,
    ) -> str:
        try:
            parts = await self._client.load_skill_part(
                assignment,
                publisher=publisher,
                name=name,
                version=version,
                path=path,
            )
            text = next(
                (
                    str(part["text"])
                    for part in parts
                    if isinstance(part, dict) and isinstance(part.get("text"), str)
                ),
                "",
            )
            size = len(text.encode())
            async with self._skill_content_lock:
                if size <= self._skill_content_cache_max_bytes:
                    # Different runs use independent authenticated loads. If both
                    # finish cold, replacing one immutable body must count it once.
                    previous = self._skill_content_cache.pop(cache_key, None)
                    if previous is not None:
                        self._skill_content_cache_bytes -= previous.size
                    self._skill_content_cache[cache_key] = _SkillContentEntry(
                        text=text,
                        size=size,
                        expires_at=(self._clock() + self._skill_content_cache_ttl_seconds),
                    )
                    self._skill_content_cache.move_to_end(cache_key)
                    self._skill_content_cache_bytes += size
                    while self._skill_content_cache and (
                        len(self._skill_content_cache) > self._skill_content_cache_max_entries
                        or self._skill_content_cache_bytes > self._skill_content_cache_max_bytes
                    ):
                        _evicted_key, evicted = self._skill_content_cache.popitem(last=False)
                        self._skill_content_cache_bytes -= evicted.size
            return text
        finally:
            async with self._skill_content_lock:
                current = asyncio.current_task()
                if self._skill_content_loads.get(load_key) is current:
                    self._skill_content_loads.pop(load_key, None)

    async def release_run(self, assignment: RuntimeAssignment) -> None:
        run_prefix = run_key(assignment)
        async with self._skill_content_lock:
            tasks = [
                task for key, task in self._skill_content_loads.items() if key[:3] == run_prefix
            ]
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
