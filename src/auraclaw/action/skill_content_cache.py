from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from auraclaw.action.ports import ArtifactContentReader
from auraclaw.action.skill_packages import (
    SkillPackage,
    skill_package_digest,
    skill_package_from_archive,
)
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.tools import ArtifactRef


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


@dataclass(frozen=True)
class _CacheEntry:
    package: SkillPackage
    size: int
    expires_at: float


class SkillPackageContentCache:
    """Bounded, digest-keyed L1 cache for immutable persisted Skill packages."""

    def __init__(
        self,
        artifacts: ArtifactContentReader,
        *,
        max_bytes: int = 64 * 1024 * 1024,
        max_entries: int = 1024,
        ttl_seconds: float = 3600.0,
        metric_writer: MetricWriter | None = None,
    ) -> None:
        if max_bytes < 1 or max_entries < 1 or ttl_seconds <= 0:
            raise ValueError("Skill package cache limits must be positive")
        self._artifacts = artifacts
        self._max_bytes = max_bytes
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._metric_writer = metric_writer
        self._entries: OrderedDict[tuple[str, str], _CacheEntry] = OrderedDict()
        self._loads: dict[tuple[str, str], asyncio.Task[SkillPackage]] = {}
        self._resident_bytes = 0
        self._lock = asyncio.Lock()

    async def load(
        self,
        *,
        tenant_id: str,
        package_digest: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        correlation_id: str,
    ) -> SkillPackage:
        key = (tenant_id, package_digest)
        now = time.monotonic()
        created = False
        async with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry.expires_at > now:
                self._entries.move_to_end(key)
                package = entry.package
            else:
                if entry is not None:
                    self._remove_locked(key)
                package = None
            task = self._loads.get(key)
            if package is None and task is None:
                task = asyncio.create_task(
                    self._load(
                        key,
                        tenant_id=tenant_id,
                        package_digest=package_digest,
                        artifact_ref=artifact_ref,
                        actor_id=actor_id,
                        correlation_id=correlation_id,
                    )
                )
                self._loads[key] = task
                created = True
        if package is not None:
            await self._emit("skill.cache.hit.count", 1.0, tenant_id, layer="l1")
            return package
        assert task is not None
        if not created:
            await self._emit("skill.load.singleflight.waiters", 1.0, tenant_id)
        await self._emit("skill.cache.miss.count", 1.0, tenant_id, layer="l1")
        return await asyncio.shield(task)

    async def prune_tenant(
        self, tenant_id: str, *, retained_digests: frozenset[str]
    ) -> int:
        async with self._lock:
            keys = [
                key
                for key in self._entries
                if key[0] == tenant_id and key[1] not in retained_digests
            ]
            for key in keys:
                self._remove_locked(key)
            resident = self._resident_bytes
        if keys:
            await self._emit(
                "skill.cache.eviction.count",
                float(len(keys)),
                tenant_id,
                reason="lifecycle_prune",
            )
            await self._emit("skill.cache.resident.bytes", float(resident), tenant_id)
        return len(keys)

    async def _load(
        self,
        key: tuple[str, str],
        *,
        tenant_id: str,
        package_digest: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        correlation_id: str,
    ) -> SkillPackage:
        started = time.monotonic()
        try:
            content = await self._artifacts.read(
                tenant_id=tenant_id,
                artifact_ref=artifact_ref,
                actor_id=actor_id,
                correlation_id=correlation_id,
            )
            package = skill_package_from_archive(content)
            if skill_package_digest(package) != package_digest:
                raise VersionConflictError("Persisted Skill package digest does not match")
            await self._emit("skill.package.download.count", 1.0, tenant_id)
            await self._emit(
                "skill.package.download.bytes", float(len(content)), tenant_id
            )
            await self._emit(
                "skill.package.download.latency.seconds",
                time.monotonic() - started,
                tenant_id,
            )
            async with self._lock:
                if len(content) <= self._max_bytes:
                    self._entries[key] = _CacheEntry(
                        package=package,
                        size=len(content),
                        expires_at=time.monotonic() + self._ttl_seconds,
                    )
                    self._entries.move_to_end(key)
                    self._resident_bytes += len(content)
                    evicted = self._evict_locked()
                else:
                    evicted = 0
                resident = self._resident_bytes
            if evicted:
                await self._emit(
                    "skill.cache.eviction.count",
                    float(evicted),
                    tenant_id,
                    reason="capacity",
                )
            await self._emit("skill.cache.resident.bytes", float(resident), tenant_id)
            return package
        finally:
            async with self._lock:
                current = asyncio.current_task()
                if self._loads.get(key) is current:
                    self._loads.pop(key, None)

    def _evict_locked(self) -> int:
        evicted = 0
        while self._entries and (
            len(self._entries) > self._max_entries
            or self._resident_bytes > self._max_bytes
        ):
            _key, entry = self._entries.popitem(last=False)
            self._resident_bytes -= entry.size
            evicted += 1
        return evicted

    def _remove_locked(self, key: tuple[str, str]) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._resident_bytes -= entry.size

    async def _emit(
        self,
        name: str,
        value: float,
        tenant_id: str,
        **labels: str,
    ) -> None:
        if self._metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=value,
                        observed_at=datetime.now(UTC),
                        tenant_id=tenant_id,
                        labels=labels,
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return
