from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from auraclaw.contracts.internal import LeaseAssertion

DEFAULT_RUNTIME_MAX_STEPS = 48
AGENT_RUNTIME_POOL = "agent"


@dataclass(frozen=True)
class RuntimeBudget:
    # Price Insight's governed flow uses capability discovery, Skill activation,
    # dependency hydration, and eight atomic metric calls. Follow-up turns in
    # the same Session also re-search and reload capabilities, so keep the
    # default above one complete flow plus that rediscovery tax.
    max_steps: int = DEFAULT_RUNTIME_MAX_STEPS
    max_output_tokens: int = 8192
    max_cost: float | None = None


@dataclass(frozen=True)
class RuntimeAssignment:
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    runtime_id: str
    lease_id: str
    fencing_token: int
    role: str
    resource_profile: dict[str, Any]
    deadline: datetime | None = None
    budget: RuntimeBudget = field(default_factory=RuntimeBudget)
    lease_expires_at: datetime | None = None
    lease_assertion: LeaseAssertion | None = None
    user_id: str | None = None
    dept_id: str | None = None


@dataclass(frozen=True)
class ClaimedAssignment:
    task_id: str
    assignment: RuntimeAssignment


@dataclass(frozen=True)
class RunnableItem:
    task_id: str
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    source_version: int
    priority: int = 0
    queue_partition: str = "default"
    role: str = "root"
    required_capability: dict[str, Any] = field(default_factory=dict)
    deadline: datetime | None = None
    budget: RuntimeBudget = field(default_factory=RuntimeBudget)
    user_id: str | None = None
    dept_id: str | None = None


@dataclass(frozen=True)
class ClaimedRunnable:
    item: RunnableItem
    claimed_by: str
    claim_token: str
    claim_expires_at: datetime


@dataclass(frozen=True)
class RuntimeLease:
    resource_id: str
    lease_id: str
    owner: str
    fencing_token: int
    expires_at: datetime


@dataclass(frozen=True)
class RuntimeInstance:
    runtime_id: str
    runtime_type: str
    role: str
    node_id: str
    capabilities: dict[str, Any]
    capacity: int


@dataclass(frozen=True)
class RuntimeCheckpoint:
    tenant_id: str
    session_id: str
    run_id: str
    fencing_token: int
    phase: str
    state: dict[str, Any]
    updated_at: datetime


class ControlStateStore(Protocol):
    async def enqueue(self, item: RunnableItem) -> bool: ...

    async def claim(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> list[ClaimedRunnable]: ...

    async def reschedule(
        self,
        task_id: str,
        *,
        worker_id: str | None = None,
        claim_token: str | None = None,
    ) -> None: ...

    async def acquire_lease(
        self, resource_id: str, owner: str, *, ttl: timedelta
    ) -> RuntimeLease | None: ...

    async def renew_lease(self, lease: RuntimeLease, *, ttl: timedelta) -> RuntimeLease: ...

    async def release_lease(self, lease: RuntimeLease) -> None: ...

    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None: ...

    async def assign(
        self, task_id: str, assignment: RuntimeAssignment, *, claim_token: str
    ) -> bool: ...

    async def get_assignment(self, task_id: str) -> RuntimeAssignment | None: ...

    async def select_runtime(self, item: RunnableItem) -> RuntimeInstance | None: ...

    async def claim_assignments(
        self, runtime_id: str, role: str, *, limit: int = 1
    ) -> list[ClaimedAssignment]: ...

    async def finish_assignment(self, task_id: str, outcome: str) -> None: ...

    async def suspend_assignment(self, task_id: str, reason: str) -> None: ...

    async def wake_assignment(self, task_id: str) -> bool: ...

    async def register_runtime(self, instance: RuntimeInstance) -> None: ...

    async def heartbeat(self, runtime_id: str, fencing_token: int | None = None) -> None: ...

    async def reserve_capacity(self, scope: str, amount: int, *, limit: int) -> bool: ...

    async def release_capacity(self, scope: str, amount: int) -> None: ...

    async def save_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None: ...

    async def load_checkpoint(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> RuntimeCheckpoint | None: ...

    async def request_cancel(self, tenant_id: str, session_id: str, run_id: str) -> None: ...

    async def is_cancelled(self, tenant_id: str, session_id: str, run_id: str) -> bool: ...

    async def recover_expired(self) -> int: ...


class RuntimeProvisioner(Protocol):
    async def provision(self, item: RunnableItem, lease: RuntimeLease) -> RuntimeInstance: ...

    async def cancel(self, runtime_id: str) -> None: ...


class Orchestrator(Protocol):
    async def reconcile(self) -> None: ...
