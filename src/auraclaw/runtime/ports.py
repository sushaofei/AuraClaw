from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from auraclaw.contracts.events import CanonicalEvent, NewEvent
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint


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
    version: str = "1"
    expected_side_effect: str = "read"
    approval_id: str | None = None
    credential_ref: str | None = None
    idempotency_key: str | None = None


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


class RuntimeControlClient(Protocol):
    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None: ...

    async def is_cancelled(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> bool: ...

    async def save_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None: ...

    async def load_checkpoint(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> RuntimeCheckpoint | None: ...

    async def finish_assignment(self, task_id: str, outcome: str) -> None: ...


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


class CapabilityClient(ToolClient, Protocol):
    async def list_tools(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]: ...

    async def list_resources(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]: ...

    async def list_resource_templates(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]: ...

    async def read_resource(
        self,
        assignment: RuntimeAssignment,
        uri: str,
    ) -> list[dict[str, Any]]: ...

    async def list_prompts(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]: ...

    async def get_prompt(
        self,
        assignment: RuntimeAssignment,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> dict[str, Any]: ...


class RuntimeEventPublisher(Protocol):
    async def publish(self, event: RuntimeEvent) -> None: ...


class AgentRuntime(Protocol):
    async def execute(self, assignment: RuntimeAssignment) -> None: ...
