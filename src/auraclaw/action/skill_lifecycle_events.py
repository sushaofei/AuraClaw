from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable
from uuid import uuid4

from auraclaw.contracts.observability import MetricPoint


@dataclass(frozen=True)
class SkillLifecycleSignal:
    event_id: str
    tenant_id: str
    revision: int
    change_type: str
    snapshot_digest: str | None
    origin_replica: str
    occurred_at: datetime


@dataclass(frozen=True)
class SkillLifecycleSignalRecord:
    outbox_id: str
    signal: SkillLifecycleSignal
    attempt: int = 0


class SkillLifecycleSignalStore(Protocol):
    async def enqueue(
        self,
        *,
        tenant_id: str,
        change_type: str,
        snapshot_digest: str | None,
        origin_replica: str,
    ) -> SkillLifecycleSignal: ...

    async def claim(
        self,
        *,
        owner: str,
        limit: int,
        claim_ttl: timedelta,
    ) -> tuple[SkillLifecycleSignalRecord, ...]: ...

    async def complete(self, *, outbox_id: str, owner: str) -> bool: ...

    async def fail(
        self, *, outbox_id: str, owner: str, safe_error_code: str
    ) -> bool: ...


class SkillLifecycleSignalPublisher(Protocol):
    async def publish(self, signal: SkillLifecycleSignal) -> None: ...


class SkillTenantRebuilder(Protocol):
    async def rebuild_tenant(self, tenant_id: str) -> object: ...


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


@runtime_checkable
class SkillSnapshotDigestReader(Protocol):
    def snapshot_digest(self, tenant_id: str) -> str | None: ...


class BroadcastingSkillStateProjector:
    """Rebuild locally, then durably announce the authoritative tenant snapshot."""

    def __init__(
        self,
        *,
        rebuilder: SkillTenantRebuilder,
        signals: SkillLifecycleSignalStore,
        replica_id: str,
    ) -> None:
        self._rebuilder = rebuilder
        self._signals = signals
        self._replica_id = replica_id

    async def rebuild_tenant(self, tenant_id: str) -> object:
        result = await self._rebuilder.rebuild_tenant(tenant_id)
        digest_reader = self._rebuilder
        snapshot_digest = (
            digest_reader.snapshot_digest(tenant_id)
            if isinstance(digest_reader, SkillSnapshotDigestReader)
            else None
        )
        await self._signals.enqueue(
            tenant_id=tenant_id,
            change_type="skill.lifecycle.snapshot_changed",
            snapshot_digest=snapshot_digest,
            origin_replica=self._replica_id,
        )
        return result


class SkillLifecycleSignalRelay:
    """Competing relay publishes one durable outbox row to the broadcast topic."""

    def __init__(
        self,
        *,
        signals: SkillLifecycleSignalStore,
        publisher: SkillLifecycleSignalPublisher,
        owner: str,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> None:
        self._signals = signals
        self._publisher = publisher
        self._owner = owner
        self._claim_ttl = claim_ttl

    async def run_once(self, *, limit: int = 100) -> int:
        records = await self._signals.claim(
            owner=self._owner,
            limit=limit,
            claim_ttl=self._claim_ttl,
        )
        completed = 0
        for record in records:
            try:
                await self._publisher.publish(record.signal)
                if await self._signals.complete(
                    outbox_id=record.outbox_id, owner=self._owner
                ):
                    completed += 1
            except Exception as exc:
                await self._signals.fail(
                    outbox_id=record.outbox_id,
                    owner=self._owner,
                    safe_error_code=type(exc).__name__,
                )
        return completed


class SkillLifecycleSignalApplier:
    """Per-replica revision fence followed by authoritative PostgreSQL rebuild."""

    def __init__(
        self,
        *,
        rebuilder: SkillTenantRebuilder,
        metric_writer: MetricWriter | None = None,
    ) -> None:
        self._rebuilder = rebuilder
        self._metric_writer = metric_writer
        self._applied_revisions: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def apply(self, signal: SkillLifecycleSignal) -> bool:
        lock = self._locks.setdefault(signal.tenant_id, asyncio.Lock())
        async with lock:
            if signal.revision <= self._applied_revisions.get(signal.tenant_id, 0):
                await self._emit(
                    "skill.lifecycle.signal.stale.count", 1.0, signal.tenant_id
                )
                return False
            await self._rebuilder.rebuild_tenant(signal.tenant_id)
            self._applied_revisions[signal.tenant_id] = signal.revision
            await self._emit(
                "skill.lifecycle.signal.applied.count", 1.0, signal.tenant_id
            )
            await self._emit(
                "skill.trusted_messages.latency.seconds",
                max(0.0, (datetime.now(UTC) - signal.occurred_at).total_seconds()),
                signal.tenant_id,
            )
            return True

    def applied_revision(self, tenant_id: str) -> int:
        return self._applied_revisions.get(tenant_id, 0)

    async def _emit(self, name: str, value: float, tenant_id: str) -> None:
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
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return


class InMemorySkillLifecycleSignalStore:
    def __init__(self) -> None:
        self._revisions: dict[str, int] = {}
        self._records: list[SkillLifecycleSignalRecord] = []
        self._claims: dict[str, tuple[str, datetime]] = {}
        self._completed: set[str] = set()
        self._lock = asyncio.Lock()

    async def enqueue(
        self,
        *,
        tenant_id: str,
        change_type: str,
        snapshot_digest: str | None,
        origin_replica: str,
    ) -> SkillLifecycleSignal:
        async with self._lock:
            revision = self._revisions.get(tenant_id, 0) + 1
            self._revisions[tenant_id] = revision
            signal = SkillLifecycleSignal(
                event_id=f"sle_{uuid4().hex}",
                tenant_id=tenant_id,
                revision=revision,
                change_type=change_type,
                snapshot_digest=snapshot_digest,
                origin_replica=origin_replica,
                occurred_at=datetime.now(UTC),
            )
            self._records.append(
                SkillLifecycleSignalRecord(
                    outbox_id=str(len(self._records) + 1), signal=signal
                )
            )
            return signal

    async def claim(
        self,
        *,
        owner: str,
        limit: int,
        claim_ttl: timedelta,
    ) -> tuple[SkillLifecycleSignalRecord, ...]:
        now = datetime.now(UTC)
        async with self._lock:
            claimed: list[SkillLifecycleSignalRecord] = []
            for record in self._records:
                if record.outbox_id in self._completed:
                    continue
                current = self._claims.get(record.outbox_id)
                if current is not None and current[1] > now:
                    continue
                self._claims[record.outbox_id] = (owner, now + claim_ttl)
                claimed.append(record)
                if len(claimed) >= limit:
                    break
            return tuple(claimed)

    async def complete(self, *, outbox_id: str, owner: str) -> bool:
        async with self._lock:
            current = self._claims.get(outbox_id)
            if current is None or current[0] != owner:
                return False
            self._completed.add(outbox_id)
            self._claims.pop(outbox_id, None)
            return True

    async def fail(
        self, *, outbox_id: str, owner: str, safe_error_code: str
    ) -> bool:
        del safe_error_code
        async with self._lock:
            current = self._claims.get(outbox_id)
            if current is None or current[0] != owner:
                return False
            self._claims.pop(outbox_id, None)
            return True
