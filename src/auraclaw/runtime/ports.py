from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from auraclaw.contracts.events import CanonicalEvent, NewEvent


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
    budget: RuntimeBudget = field(default_factory=lambda: RuntimeBudget())


@dataclass(frozen=True)
class RuntimeBudget:
    max_steps: int = 16
    max_output_tokens: int = 8192
    max_cost: float | None = None


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
    budget: RuntimeBudget = field(default_factory=lambda: RuntimeBudget())


@dataclass(frozen=True)
class ClaimedRunnable:
    item: RunnableItem
    claimed_by: str


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


@dataclass(frozen=True)
class ModelPolicy:
    capability: str = "general"
    preferred_model: str | None = None
    allowed_providers: tuple[str, ...] = ()
    data_classification: str = "internal"


@dataclass(frozen=True)
class ModelRequest:
    model_call_id: str
    tenant_id: str
    run_id: str
    messages: tuple[dict[str, Any], ...]
    tools: tuple[dict[str, Any], ...] = ()
    policy: ModelPolicy = field(default_factory=ModelPolicy)
    max_output_tokens: int = 8192


@dataclass(frozen=True)
class ToolCall:
    tool_invocation_id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ModelResponse:
    model_call_id: str
    provider: str
    model: str
    completed_output: str
    deltas: tuple[str, ...] = ()
    tool_calls: tuple[ToolCall, ...] = ()
    finish_reason: str = "stop"
    usage: dict[str, int | float] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeEvent:
    event_id: str
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    sequence: int
    type: str
    timestamp: datetime
    payload: dict[str, Any]
    durable: bool = False
    visibility: str = "internal"


class ControlStateStore(Protocol):
    async def enqueue(self, item: RunnableItem) -> bool: ...

    async def claim(self, worker_id: str, *, limit: int = 1) -> list[ClaimedRunnable]: ...

    async def reschedule(self, task_id: str) -> None: ...

    async def acquire_lease(
        self, resource_id: str, owner: str, *, ttl: timedelta
    ) -> RuntimeLease | None: ...

    async def renew_lease(self, lease: RuntimeLease, *, ttl: timedelta) -> RuntimeLease: ...

    async def release_lease(self, lease: RuntimeLease) -> None: ...

    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None: ...

    async def assign(self, task_id: str, assignment: RuntimeAssignment) -> bool: ...

    async def get_assignment(self, task_id: str) -> RuntimeAssignment | None: ...

    async def finish_assignment(self, task_id: str, outcome: str) -> None: ...

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


class SessionClient(Protocol):
    async def load(self, assignment: RuntimeAssignment) -> list[CanonicalEvent]: ...

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: Sequence[NewEvent],
        *,
        command_id: str,
        operation: str,
    ) -> list[CanonicalEvent]: ...


class ModelClient(Protocol):
    async def generate(self, request: ModelRequest) -> ModelResponse: ...


class ProviderAdapter(Protocol):
    name: str

    async def generate(self, request: ModelRequest, *, credential: str) -> ModelResponse: ...


class CredentialResolver(Protocol):
    async def resolve(self, provider: str, tenant_id: str) -> str: ...


class ToolClient(Protocol):
    async def execute(
        self, assignment: RuntimeAssignment, call: ToolCall
    ) -> dict[str, Any]: ...


class RuntimeEventPublisher(Protocol):
    async def publish(self, event: RuntimeEvent) -> None: ...


class RuntimeProvisioner(Protocol):
    async def provision(self, item: RunnableItem, lease: RuntimeLease) -> RuntimeInstance: ...

    async def cancel(self, runtime_id: str) -> None: ...


class AgentRuntime(Protocol):
    async def execute(self, assignment: RuntimeAssignment) -> None: ...


class Orchestrator(Protocol):
    async def reconcile(self) -> None: ...
