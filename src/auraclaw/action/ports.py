from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from auraclaw.contracts.capabilities import CapabilityDescriptor, McpServerDefinition
from auraclaw.contracts.tools import (
    ApprovalRecord,
    ArtifactRef,
    PolicyDecision,
    ToolCapability,
    ToolInvocation,
)

CredentialAdapter = Callable[[dict[str, Any], str], Awaitable[Any] | Any]


@dataclass(frozen=True)
class PolicyEvaluation:
    decision: PolicyDecision
    decision_id: str
    policy_version: str


class HandsExecutor(Protocol):
    async def execute(self, invocation: ToolInvocation, capability: ToolCapability) -> Any: ...


class PolicyEvaluator(Protocol):
    version: str

    def evaluate(
        self,
        capability: ToolCapability,
        invocation: ToolInvocation | None = None,
    ) -> PolicyDecision | PolicyEvaluation | Awaitable[PolicyDecision | PolicyEvaluation]: ...


@dataclass(frozen=True)
class InvocationBegin:
    conflict: bool = False
    cached_result: Any | None = None


class InvocationStore(Protocol):
    async def begin(self, invocation: ToolInvocation, argument_digest: str) -> InvocationBegin: ...

    async def set_status(
        self, invocation: ToolInvocation, status: str, *, error_code: str | None = None
    ) -> None: ...

    async def complete(self, invocation: ToolInvocation, result: Any) -> None: ...


class ApprovalController(Protocol):
    async def request_approval(self, record: ApprovalRecord) -> None: ...

    async def validate_approval(
        self,
        *,
        tenant_id: str,
        approval_id: str,
        session_id: str,
        run_id: str,
        action_digest: str,
        policy_version: str,
    ) -> bool: ...


class ArtifactWriter(Protocol):
    async def put(
        self,
        *,
        tenant_id: str,
        root_session_id: str,
        session_id: str,
        content: bytes,
        artifact_type: str,
        media_type: str,
        name: str,
        producer: str,
        lineage_refs: tuple[str, ...] = (),
        classification: str = "internal",
        acl: tuple[str, ...] = (),
        retention_until: datetime | None = None,
    ) -> ArtifactRef: ...


class CredentialInvoker(Protocol):
    async def invoke(
        self,
        *,
        tenant_id: str,
        session_id: str,
        tool_name: str,
        credential_ref: str,
        operation: str,
        request: dict[str, Any],
        adapter: CredentialAdapter | None = None,
        policy_decision_id: str | None = None,
    ) -> Any: ...

    def redact(self, value: Any) -> Any: ...


class CapabilityCatalogStore(Protocol):
    async def upsert_server(self, server: McpServerDefinition) -> None: ...

    async def get_server(self, server_id: str) -> McpServerDefinition | None: ...

    async def list_servers(self, tenant_id: str) -> tuple[McpServerDefinition, ...]: ...

    async def replace_capabilities(
        self,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
    ) -> None: ...

    async def list_capabilities(
        self, tenant_id: str
    ) -> tuple[CapabilityDescriptor, ...]: ...
