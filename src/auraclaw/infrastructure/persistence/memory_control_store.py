from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from auraclaw.contracts.errors import FencingTokenError, LeaseConflictError
from auraclaw.control.ports import (
    ClaimedRunnable,
    RunnableItem,
    RuntimeAssignment,
    RuntimeCheckpoint,
    RuntimeInstance,
    RuntimeLease,
)


def _now() -> datetime:
    return datetime.now(UTC)


class InMemoryControlStateStore:
    """Strongly consistent development control store.

    Every competing operation is performed under one lock. The store deliberately
    contains no Session business state; it only owns replaceable runtime control data.
    """

    def __init__(self) -> None:
        self._queue: dict[str, tuple[RunnableItem, str, str | None]] = {}
        self._leases: dict[str, RuntimeLease] = {}
        self._lease_counters: dict[str, int] = {}
        self._assignments: dict[str, tuple[RuntimeAssignment, str]] = {}
        self._runtimes: dict[str, tuple[RuntimeInstance, datetime]] = {}
        self._capacity: dict[str, int] = {}
        self._checkpoints: dict[tuple[str, str, str], RuntimeCheckpoint] = {}
        self._cancellations: set[tuple[str, str, str]] = set()
        self._lock = asyncio.Lock()

    async def enqueue(self, item: RunnableItem) -> bool:
        async with self._lock:
            existing = self._queue.get(item.task_id)
            if existing is not None:
                return False
            self._queue[item.task_id] = (item, "queued", None)
            return True

    async def claim(self, worker_id: str, *, limit: int = 1) -> list[ClaimedRunnable]:
        async with self._lock:
            candidates = [
                item
                for item, status, _ in self._queue.values()
                if status == "queued"
            ]
            candidates.sort(key=lambda item: (-item.priority, item.task_id))
            claimed: list[ClaimedRunnable] = []
            for item in candidates[:limit]:
                self._queue[item.task_id] = (item, "claimed", worker_id)
                claimed.append(ClaimedRunnable(item=item, claimed_by=worker_id))
            return claimed

    async def reschedule(self, task_id: str) -> None:
        async with self._lock:
            queued = self._queue.get(task_id)
            if queued is not None:
                self._queue[task_id] = (queued[0], "queued", None)
            assignment = self._assignments.get(task_id)
            if assignment is not None:
                self._assignments[task_id] = (assignment[0], "failed")

    async def acquire_lease(
        self, resource_id: str, owner: str, *, ttl: timedelta
    ) -> RuntimeLease | None:
        async with self._lock:
            now = _now()
            current = self._leases.get(resource_id)
            if current is not None and current.expires_at > now:
                return None
            token = self._lease_counters.get(resource_id, 0) + 1
            self._lease_counters[resource_id] = token
            lease = RuntimeLease(
                resource_id=resource_id,
                lease_id=f"lea_{uuid4().hex}",
                owner=owner,
                fencing_token=token,
                expires_at=now + ttl,
            )
            self._leases[resource_id] = lease
            return lease

    async def renew_lease(self, lease: RuntimeLease, *, ttl: timedelta) -> RuntimeLease:
        async with self._lock:
            current = self._leases.get(lease.resource_id)
            if (
                current is None
                or current.lease_id != lease.lease_id
                or current.owner != lease.owner
                or current.fencing_token != lease.fencing_token
                or current.expires_at <= _now()
            ):
                raise LeaseConflictError(f"lease is no longer owned: {lease.resource_id}")
            renewed = replace(current, expires_at=_now() + ttl)
            self._leases[lease.resource_id] = renewed
            return renewed

    async def release_lease(self, lease: RuntimeLease) -> None:
        async with self._lock:
            current = self._leases.get(lease.resource_id)
            if current is not None and current.lease_id == lease.lease_id:
                del self._leases[lease.resource_id]

    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None:
        async with self._lock:
            current = self._leases.get(resource_id)
            if (
                current is None
                or current.expires_at <= _now()
                or current.fencing_token != fencing_token
            ):
                raise FencingTokenError(
                    f"stale fencing token {fencing_token} for {resource_id}"
                )

    async def assign(self, task_id: str, assignment: RuntimeAssignment) -> bool:
        async with self._lock:
            current = self._assignments.get(task_id)
            if current is not None and current[1] not in {"expired", "completed", "failed"}:
                return False
            self._assignments[task_id] = (assignment, "assigned")
            if task_id in self._queue:
                item, _, owner = self._queue[task_id]
                self._queue[task_id] = (item, "assigned", owner)
            return True

    async def get_assignment(self, task_id: str) -> RuntimeAssignment | None:
        async with self._lock:
            entry = self._assignments.get(task_id)
            return entry[0] if entry is not None else None

    async def finish_assignment(self, task_id: str, outcome: str) -> None:
        async with self._lock:
            entry = self._assignments.get(task_id)
            if entry is not None:
                self._assignments[task_id] = (entry[0], outcome)
            queued = self._queue.get(task_id)
            if queued is not None:
                self._queue[task_id] = (queued[0], "acked", queued[2])

    async def register_runtime(self, instance: RuntimeInstance) -> None:
        async with self._lock:
            self._runtimes[instance.runtime_id] = (instance, _now())

    async def heartbeat(self, runtime_id: str, fencing_token: int | None = None) -> None:
        async with self._lock:
            entry = self._runtimes.get(runtime_id)
            if entry is None:
                raise LeaseConflictError(f"unknown runtime: {runtime_id}")
            if fencing_token is not None:
                assignment = next(
                    (
                        value[0]
                        for value in self._assignments.values()
                        if value[0].runtime_id == runtime_id
                    ),
                    None,
                )
                if assignment is None or assignment.fencing_token != fencing_token:
                    raise FencingTokenError(f"stale runtime heartbeat: {runtime_id}")
            self._runtimes[runtime_id] = (entry[0], _now())

    async def reserve_capacity(self, scope: str, amount: int, *, limit: int) -> bool:
        async with self._lock:
            current = self._capacity.get(scope, 0)
            if amount < 0 or current + amount > limit:
                return False
            self._capacity[scope] = current + amount
            return True

    async def release_capacity(self, scope: str, amount: int) -> None:
        async with self._lock:
            self._capacity[scope] = max(0, self._capacity.get(scope, 0) - amount)

    async def save_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None:
        async with self._lock:
            resource_id = f"session:{checkpoint.tenant_id}:{checkpoint.session_id}"
            current = self._leases.get(resource_id)
            if (
                current is None
                or current.expires_at <= _now()
                or current.fencing_token != checkpoint.fencing_token
            ):
                raise FencingTokenError("checkpoint rejected for stale Runtime")
            key = (checkpoint.tenant_id, checkpoint.session_id, checkpoint.run_id)
            previous = self._checkpoints.get(key)
            if previous is None or previous.fencing_token <= checkpoint.fencing_token:
                self._checkpoints[key] = checkpoint

    async def load_checkpoint(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> RuntimeCheckpoint | None:
        async with self._lock:
            return self._checkpoints.get((tenant_id, session_id, run_id))

    async def request_cancel(self, tenant_id: str, session_id: str, run_id: str) -> None:
        async with self._lock:
            self._cancellations.add((tenant_id, session_id, run_id))

    async def is_cancelled(self, tenant_id: str, session_id: str, run_id: str) -> bool:
        async with self._lock:
            return (tenant_id, session_id, run_id) in self._cancellations

    async def recover_expired(self) -> int:
        async with self._lock:
            now = _now()
            expired_resources = {
                resource_id
                for resource_id, lease in self._leases.items()
                if lease.expires_at <= now
            }
            for resource_id in expired_resources:
                del self._leases[resource_id]
            repaired = 0
            for task_id, (assignment, status) in list(self._assignments.items()):
                resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
                if resource_id in expired_resources and status in {"assigned", "running"}:
                    self._assignments[task_id] = (assignment, "expired")
                    queued = self._queue.get(task_id)
                    if queued is not None:
                        self._queue[task_id] = (queued[0], "queued", None)
                    repaired += 1
            return repaired
