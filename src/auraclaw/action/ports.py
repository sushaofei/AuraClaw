from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from auraclaw.contracts.capabilities import CapabilityDescriptor, McpServerDefinition
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsPromptResult,
    HandsResourceContent,
    HandsToolResult,
    HandsTrustedContext,
)
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
    async def execute(
        self, invocation: ToolInvocation, capability: ToolCapability
    ) -> Any: ...


class PolicyEvaluator(Protocol):
    version: str

    def evaluate(
        self,
        capability: ToolCapability,
        invocation: ToolInvocation | None = None,
    ) -> (
        PolicyDecision | PolicyEvaluation | Awaitable[PolicyDecision | PolicyEvaluation]
    ): ...


@dataclass(frozen=True)
class InvocationBegin:
    conflict: bool = False
    cached_result: Any | None = None


class InvocationStore(Protocol):
    async def begin(
        self, invocation: ToolInvocation, argument_digest: str
    ) -> InvocationBegin: ...

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


class ArtifactContentReader(Protocol):
    async def read(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        correlation_id: str,
    ) -> bytes: ...


class ArtifactDeleter(Protocol):
    async def delete(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        reason_code: str,
        correlation_id: str,
    ) -> None: ...


class SkillBindingReferenceReader(Protocol):
    async def has_reference(
        self,
        *,
        tenant_id: str,
        package_digest: str,
        correlation_id: str,
    ) -> bool: ...


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

    async def remove_server(self, server_id: str) -> None: ...

    async def list_capabilities(
        self, tenant_id: str
    ) -> tuple[CapabilityDescriptor, ...]: ...

    async def list_server_capabilities(
        self, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]: ...

    async def get_capability(
        self, tenant_id: str, capability_id: str
    ) -> CapabilityDescriptor | None: ...


class ResourceReader(Protocol):
    async def read(
        self,
        trusted_context: HandsTrustedContext,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]: ...


McpResourceReader = ResourceReader


class CapabilityConnector(Protocol):
    connector_id: str

    async def snapshot(
        self, trusted: HandsTrustedContext
    ) -> CapabilitySnapshot: ...

    async def read_resource(
        self,
        trusted: HandsTrustedContext,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]: ...

    async def get_prompt(
        self,
        trusted: HandsTrustedContext,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> HandsPromptResult: ...

    async def call_tool(
        self,
        trusted: HandsTrustedContext,
        *,
        name: str,
        arguments: dict[str, Any],
        invocation_id: str,
    ) -> HandsToolResult: ...

    async def aclose(self) -> None: ...


class ResourcePolicyEvaluator(Protocol):
    async def evaluate_action(
        self,
        *,
        tenant_id: str,
        subject: str,
        action: str,
        resource: str,
        input_digest: str,
        correlation_id: str,
        attributes: dict[str, object],
    ) -> PolicyEvaluation: ...
