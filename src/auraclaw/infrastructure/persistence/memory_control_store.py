from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from auraclaw.contracts.errors import FencingTokenError, LeaseConflictError
from auraclaw.control.ports import (
    AGENT_RUNTIME_POOL,
    ClaimedAssignment,
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
        self._queue_claims: dict[str, tuple[str, datetime]] = {}
        self._queue_available_at: dict[str, datetime] = {}
        self._leases: dict[str, RuntimeLease] = {}
        self._lease_counters: dict[str, int] = {}
        self._assignments: dict[str, tuple[RuntimeAssignment, str]] = {}
        self._assignment_started_at: dict[str, datetime] = {}
        self._runtimes: dict[str, tuple[RuntimeInstance, datetime]] = {}
        self._capacity: dict[str, int] = {}
        self._checkpoints: dict[tuple[str, str, str], RuntimeCheckpoint] = {}
        self._cancellations: set[tuple[str, str, str]] = set()
        self._lock = asyncio.Lock()
        # Match PostgresControlStateStore reclaim windows.
        self.orphan_running_grace = timedelta(seconds=5)
        self.stale_heartbeat_after = timedelta(seconds=30)
        self.execution_claim_ttl = timedelta(seconds=30)
        self.assignment_lease_ttl = timedelta(seconds=30)

    async def enqueue(self, item: RunnableItem) -> bool:
        async with self._lock:
            existing = self._queue.get(item.task_id)
            if existing is not None:
                return False
            self._queue[item.task_id] = (item, "queued", None)
            return True

    async def claim(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> list[ClaimedRunnable]:
        async with self._lock:
            now = _now()
            candidates = [
                item
                for item, status, _ in self._queue.values()
                if status == "queued"
                and self._queue_available_at.get(item.task_id, now) <= now
                or (
                    status == "claimed"
                    and self._queue_claims.get(
                        item.task_id, ("", datetime.min.replace(tzinfo=UTC))
                    )[1]
                    <= now
                )
            ]
            candidates.sort(key=lambda item: (-item.priority, item.task_id))
            claimed: list[ClaimedRunnable] = []
            for item in candidates[:limit]:
                token = uuid4().hex
                expires_at = now + claim_ttl
                self._queue[item.task_id] = (item, "claimed", worker_id)
                self._queue_claims[item.task_id] = (token, expires_at)
                claimed.append(
                    ClaimedRunnable(
                        item=item,
                        claimed_by=worker_id,
                        claim_token=token,
                        claim_expires_at=expires_at,
                    )
                )
            return claimed

    async def reschedule(
        self,
        task_id: str,
        *,
        worker_id: str | None = None,
        claim_token: str | None = None,
        delay: timedelta = timedelta(0),
    ) -> None:
        async with self._lock:
            queued = self._queue.get(task_id)
            if queued is not None:
                if worker_id is not None or claim_token is not None:
                    current_claim = self._queue_claims.get(task_id)
                    if (
                        queued[2] != worker_id
                        or current_claim is None
                        or current_claim[0] != claim_token
                        or current_claim[1] <= _now()
                    ):
                        return
                self._queue[task_id] = (queued[0], "queued", None)
                self._queue_available_at[task_id] = _now() + delay
                self._queue_claims.pop(task_id, None)
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

    async def assign(
        self, task_id: str, assignment: RuntimeAssignment, *, claim_token: str
    ) -> bool:
        async with self._lock:
            queued = self._queue.get(task_id)
            claim = self._queue_claims.get(task_id)
            resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
            lease = self._leases.get(resource_id)
            if (
                queued is None
                or queued[1] != "claimed"
                or claim is None
                or claim[0] != claim_token
                or claim[1] <= _now()
                or lease is None
                or lease.lease_id != assignment.lease_id
                or lease.fencing_token != assignment.fencing_token
                or lease.owner != queued[2]
                or lease.expires_at <= _now()
            ):
                return False
            current = self._assignments.get(task_id)
            if current is not None and current[1] not in {
                "expired",
                "completed",
                "failed",
                "waiting_children",
                "waiting_for_human",
                "waiting_for_tool",
            }:
                return False
            self._assignments[task_id] = (assignment, "assigned")
            if task_id in self._queue:
                item, _, owner = self._queue[task_id]
                self._queue[task_id] = (item, "assigned", owner)
                self._queue_claims.pop(task_id, None)
            return True

    async def get_assignment(self, task_id: str) -> RuntimeAssignment | None:
        async with self._lock:
            entry = self._assignments.get(task_id)
            return entry[0] if entry is not None else None

    async def select_runtime(self, item: RunnableItem) -> RuntimeInstance | None:
        async with self._lock:
            candidates: list[tuple[int, RuntimeInstance]] = []
            for runtime, heartbeat_at in self._runtimes.values():
                if (
                    runtime.role not in {AGENT_RUNTIME_POOL, item.role}
                    or heartbeat_at <= _now() - timedelta(seconds=30)
                ):
                    continue
                if any(
                    runtime.capabilities.get(key) != value
                    for key, value in item.required_capability.items()
                ):
                    continue
                active = sum(
                    1
                    for assignment, status in self._assignments.values()
                    if assignment.runtime_id == runtime.runtime_id
                    and status in {"assigned", "running"}
                )
                if active < runtime.capacity:
                    candidates.append((active, runtime))
            return min(candidates, key=lambda candidate: candidate[0])[1] if candidates else None

    async def claim_assignments(
        self,
        runtime_id: str,
        role: str,
        *,
        registration_id: str = "legacy",
        limit: int = 1,
    ) -> list[ClaimedAssignment]:
        async with self._lock:
            runtime_entry = self._runtimes.get(runtime_id)
            if (
                runtime_entry is None
                or runtime_entry[0].role != role
                or runtime_entry[0].registration_id != registration_id
            ):
                return []
            now = _now()
            claimed: list[ClaimedAssignment] = []
            for task_id, (assignment, status) in self._assignments.items():
                if assignment.runtime_id != runtime_id:
                    continue
                if status != "assigned":
                    continue
                resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
                lease = self._leases.get(resource_id)
                if (
                    lease is None
                    or lease.expires_at <= now
                    or lease.fencing_token != assignment.fencing_token
                ):
                    continue
                assignment.execution_claim_token = uuid4().hex
                assignment.execution_claim_expires_at = now + self.execution_claim_ttl
                renewed_lease = replace(
                    lease,
                    expires_at=min(
                        lease.expires_at, now + self.assignment_lease_ttl
                    ),
                )
                self._leases[resource_id] = renewed_lease
                assignment.lease_expires_at = renewed_lease.expires_at
                self._assignments[task_id] = (assignment, "running")
                self._assignment_started_at.setdefault(task_id, now)
                claimed.append(
                    ClaimedAssignment(task_id=task_id, assignment=assignment)
                )
                if len(claimed) >= limit:
                    break
            return claimed

    async def renew_assignment_claim(
        self,
        task_id: str,
        *,
        runtime_id: str,
        registration_id: str,
        execution_claim_token: str,
        lease_id: str,
        fencing_token: int,
    ) -> RuntimeAssignment:
        async with self._lock:
            now = _now()
            entry = self._assignments.get(task_id)
            runtime = self._runtimes.get(runtime_id)
            if entry is None or runtime is None:
                raise LeaseConflictError("execution claim is unavailable")
            assignment, status = entry
            resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
            lease = self._leases.get(resource_id)
            if (
                status != "running"
                or runtime[0].registration_id != registration_id
                or assignment.runtime_id != runtime_id
                or assignment.execution_claim_token != execution_claim_token
                or assignment.execution_claim_expires_at is None
                or assignment.execution_claim_expires_at <= now
                or assignment.lease_id != lease_id
                or assignment.fencing_token != fencing_token
                or lease is None
                or lease.lease_id != lease_id
                or lease.fencing_token != fencing_token
                or lease.expires_at <= now
            ):
                raise LeaseConflictError("execution claim is no longer owned")
            expires_at = now + self.assignment_lease_ttl
            self._leases[resource_id] = replace(lease, expires_at=expires_at)
            assignment.lease_expires_at = expires_at
            assignment.execution_claim_expires_at = now + self.execution_claim_ttl
            self._assignments[task_id] = (assignment, status)
            self._runtimes[runtime_id] = (runtime[0], now)
            return assignment

    async def abandon_stale_assignment(
        self,
        task_id: str,
        *,
        runtime_id: str,
        lease_id: str,
        fencing_token: int,
    ) -> bool:
        async with self._lock:
            entry = self._assignments.get(task_id)
            queued = self._queue.get(task_id)
            if entry is None:
                return False
            assignment, status = entry
            if (
                assignment.runtime_id != runtime_id
                or assignment.lease_id != lease_id
                or assignment.fencing_token != fencing_token
            ):
                return False
            if status in {
                "expired",
                "completed",
                "failed",
                "cancelled",
                "waiting_children",
                "waiting_for_human",
                "waiting_for_tool",
            }:
                return True
            resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
            lease = self._leases.get(resource_id)
            if (
                lease is not None
                and lease.expires_at > _now()
                and lease.fencing_token == fencing_token
            ):
                return False
            self._assignments[task_id] = (assignment, "expired")
            self._assignment_started_at.pop(task_id, None)
            if queued is not None:
                self._queue[task_id] = (queued[0], "queued", None)
                self._queue_claims.pop(task_id, None)
            return True

    async def finish_assignment(self, task_id: str, outcome: str) -> None:
        async with self._lock:
            entry = self._assignments.get(task_id)
            if entry is not None:
                assignment = entry[0]
                self._assignments[task_id] = (assignment, outcome)
                if outcome in {
                    "completed",
                    "failed",
                    "cancelled",
                    "waiting_children",
                    "waiting_for_human",
                    "waiting_for_tool",
                }:
                    self._assignment_started_at.pop(task_id, None)
                    resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
                    lease = self._leases.get(resource_id)
                    if lease is not None and lease.lease_id == assignment.lease_id:
                        del self._leases[resource_id]
            queued = self._queue.get(task_id)
            if queued is not None:
                self._queue[task_id] = (queued[0], "acked", queued[2])

    async def suspend_assignment(self, task_id: str, reason: str) -> None:
        if reason not in {"waiting_children", "waiting_for_human", "waiting_for_tool"}:
            raise ValueError(f"unsupported assignment suspension: {reason}")
        await self.finish_assignment(task_id, reason)

    async def suspend_with_checkpoint(
        self,
        task_id: str,
        checkpoint: RuntimeCheckpoint,
        reason: str,
    ) -> None:
        if reason not in {"waiting_children", "waiting_for_human", "waiting_for_tool"}:
            raise ValueError(f"unsupported assignment suspension: {reason}")
        async with self._lock:
            entry = self._assignments.get(task_id)
            resource_id = f"session:{checkpoint.tenant_id}:{checkpoint.session_id}"
            lease = self._leases.get(resource_id)
            if (
                entry is None
                or entry[0].run_id != checkpoint.run_id
                or lease is None
                or lease.expires_at <= _now()
                or lease.fencing_token != checkpoint.fencing_token
            ):
                raise FencingTokenError("checkpoint suspension rejected for stale Runtime")
            key = (checkpoint.tenant_id, checkpoint.session_id, checkpoint.run_id)
            previous = self._checkpoints.get(key)
            if previous is not None and previous.fencing_token > checkpoint.fencing_token:
                raise FencingTokenError("checkpoint suspension rejected for stale Runtime")
            self._checkpoints[key] = checkpoint
            assignment = entry[0]
            self._assignments[task_id] = (assignment, reason)
            self._assignment_started_at.pop(task_id, None)
            del self._leases[resource_id]
            queued = self._queue.get(task_id)
            if queued is not None:
                self._queue[task_id] = (queued[0], "acked", queued[2])

    async def wake_assignment(self, task_id: str) -> bool:
        async with self._lock:
            entry = self._assignments.get(task_id)
            queued = self._queue.get(task_id)
            if (
                entry is None
                or entry[1] not in {"waiting_children", "waiting_for_human", "waiting_for_tool"}
                or queued is None or queued[1] != "acked"
            ):
                return False
            self._queue[task_id] = (queued[0], "queued", None)
            self._queue_claims.pop(task_id, None)
            return True

    async def list_waiting_assignments(
        self, *, limit: int = 100, status: str = "waiting_children"
    ) -> tuple[RuntimeAssignment, ...]:
        async with self._lock:
            return tuple(
                assignment
                for task_id, (assignment, current_status) in self._assignments.items()
                if current_status == status
                and self._queue.get(task_id, (None, "", None))[1] == "acked"
            )[: max(0, limit)]

    async def register_runtime(self, instance: RuntimeInstance) -> None:
        async with self._lock:
            current = self._runtimes.get(instance.runtime_id)
            if (
                current is not None
                and current[0].registration_id != instance.registration_id
                and current[1] > _now() - self.stale_heartbeat_after
            ):
                raise LeaseConflictError(
                    f"runtime id is already registered: {instance.runtime_id}"
                )
            self._runtimes[instance.runtime_id] = (instance, _now())

    async def heartbeat(
        self,
        runtime_id: str,
        fencing_token: int | None = None,
        *,
        registration_id: str = "legacy",
    ) -> None:
        async with self._lock:
            entry = self._runtimes.get(runtime_id)
            if entry is None:
                raise LeaseConflictError(f"unknown runtime: {runtime_id}")
            if entry[0].registration_id != registration_id:
                raise LeaseConflictError(f"runtime registration is stale: {runtime_id}")
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
            for _task_id, (assignment, status) in list(self._assignments.items()):
                if status not in {"assigned", "running"}:
                    continue
                resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
                if (
                    status == "running"
                    and assignment.execution_claim_expires_at is not None
                    and assignment.execution_claim_expires_at <= now
                ):
                    expired_resources.add(resource_id)
                    continue
                runtime_entry = self._runtimes.get(assignment.runtime_id)
                if runtime_entry is None:
                    expired_resources.add(resource_id)
                    continue
                _, heartbeat_at = runtime_entry
                if heartbeat_at <= now - self.stale_heartbeat_after:
                    expired_resources.add(resource_id)
            for resource_id in expired_resources:
                self._leases.pop(resource_id, None)
            repaired = 0
            for task_id, (assignment, status) in list(self._assignments.items()):
                resource_id = f"session:{assignment.tenant_id}:{assignment.session_id}"
                if resource_id in expired_resources and status in {"assigned", "running"}:
                    self._assignments[task_id] = (assignment, "expired")
                    self._assignment_started_at.pop(task_id, None)
                    queued = self._queue.get(task_id)
                    if queued is not None:
                        self._queue[task_id] = (queued[0], "queued", None)
                        self._queue_claims.pop(task_id, None)
                    repaired += 1
            for task_id, (item, status, _owner) in list(self._queue.items()):
                claim = self._queue_claims.get(task_id)
                if status == "claimed" and claim is not None and claim[1] <= now:
                    self._queue[task_id] = (item, "queued", None)
                    self._queue_claims.pop(task_id, None)
                    repaired += 1
            return repaired
