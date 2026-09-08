from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
    ToolResult,
)

CredentialAdapter = Callable[[dict[str, Any], str], Awaitable[Any] | Any]


@dataclass(frozen=True)
class PolicyEvaluation:
    decision: PolicyDecision
    decision_id: str
    policy_version: str
    constraints: dict[str, Any] = field(default_factory=dict)


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
    acquired: bool = False
    conflict: bool = False
    claim_token: str | None = None
    cached_result: Any | None = None


@dataclass(frozen=True)
class InvocationStatusRecord:
    status: str
    side_effect_status: str
    error_code: str | None = None
    cancel_requested: bool = False
    result: ToolResult | None = None
    root_session_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None


@dataclass(frozen=True)
class CatalogSyncHealth:
    consecutive_failures: int
    quarantined: bool


@dataclass(frozen=True)
class CatalogReconcileLease:
    server_id: str
    owner: str
    fencing_token: int
    config_revision: int
    previous_generation: int
    expires_at: datetime


@dataclass(frozen=True)
class CatalogCommitResult:
    generation: int
    committed: bool
    snapshot_digest: str


@dataclass(frozen=True)
class CommittedCatalogSnapshot:
    server: McpServerDefinition
    generation: int
    snapshot_digest: str
    source_revision: str | None
    capabilities: tuple[CapabilityDescriptor, ...]


class InvocationStore(Protocol):
    async def begin(
        self,
        invocation: ToolInvocation,
        argument_digest: str,
        *,
        owner: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> InvocationBegin: ...

    async def mark_executing(self, invocation: ToolInvocation, *, claim_token: str) -> bool: ...

    async def wait_for_approval(
        self, invocation: ToolInvocation, result: Any, *, claim_token: str
    ) -> bool: ...

    async def renew(
        self,
        invocation: ToolInvocation,
        *,
        owner: str,
        claim_token: str,
        claim_ttl: timedelta,
    ) -> bool: ...

    async def request_cancel(self, tenant_id: str, tool_invocation_id: str) -> bool: ...

    async def is_cancel_requested(
        self, invocation: ToolInvocation, *, claim_token: str
    ) -> bool: ...

    async def get_status(
        self, tenant_id: str, tool_invocation_id: str
    ) -> InvocationStatusRecord | None: ...

    async def complete(
        self, invocation: ToolInvocation, result: Any, *, claim_token: str
    ) -> bool: ...


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
    async def purge(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        reason_code: str,
        correlation_id: str,
    ) -> None: ...

    async def delete(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        reason_code: str,
        correlation_id: str,
    ) -> None: ...


@dataclass(frozen=True)
class SkillArtifactOrphan:
    tenant_id: str
    artifact_ref: ArtifactRef
    claim_token: str


class SkillArtifactLifecycle(Protocol):
    async def claim_publication(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        command_id: str,
        correlation_id: str,
    ) -> None: ...

    async def bind_publication(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        command_id: str,
        package_digest: str,
        correlation_id: str,
    ) -> None: ...

    async def claim_orphans(
        self, *, owner: str, limit: int = 100
    ) -> tuple[SkillArtifactOrphan, ...]: ...

    async def resolve_orphan(
        self,
        *,
        tenant_id: str,
        orphan: SkillArtifactOrphan,
        referenced: bool,
        package_digest: str | None,
        correlation_id: str,
    ) -> str: ...


class SkillBindingReferenceReader(Protocol):
    async def has_reference(
        self,
        *,
        tenant_id: str,
        package_digest: str,
        correlation_id: str,
    ) -> bool: ...

    async def has_active_skill_reference(
        self,
        *,
        tenant_id: str,
        publisher: str,
        name: str,
        correlation_id: str,
        package_digest: str | None = None,
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
        *,
        lease: CatalogReconcileLease,
        snapshot_digest: str,
        source_revision: str | None,
    ) -> CatalogCommitResult: ...

    async def claim_catalog_reconcile(
        self, *, server_id: str, owner: str, ttl: timedelta
    ) -> CatalogReconcileLease | None: ...

    async def release_catalog_reconcile(self, lease: CatalogReconcileLease) -> None: ...

    async def get_active_generation(self, server_id: str) -> int | None: ...

    async def read_committed_snapshot(
        self, tenant_id: str, server_id: str
    ) -> CommittedCatalogSnapshot | None: ...

    async def record_catalog_sync(
        self,
        server_id: str,
        *,
        succeeded: bool,
        attempted_at: datetime,
        safe_error_code: str | None,
        quarantine_after_failures: int,
    ) -> CatalogSyncHealth: ...

    async def remove_server(self, server_id: str) -> None: ...

    async def list_capabilities(self, tenant_id: str) -> tuple[CapabilityDescriptor, ...]: ...

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

    async def snapshot(self, trusted: HandsTrustedContext) -> CapabilitySnapshot: ...

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
