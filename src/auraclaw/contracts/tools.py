from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from auraclaw.contracts.capabilities import CapabilityInvocationRef


class ToolPermission(StrEnum):
    READ_ONLY = "read-only"
    SUGGEST_ONLY = "suggest-only"
    WRITE_WITH_APPROVAL = "write-with-approval"
    WRITE_AUTONOMOUS = "write-autonomous"
    DESTRUCTIVE_ADMIN = "destructive/admin"


class RiskLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class PolicyDecision(StrEnum):
    ALLOW = "allow"
    DENY = "deny"
    ALLOW_WITH_CONSTRAINTS = "allow_with_constraints"
    REQUIRE_APPROVAL = "require_approval"


class ApprovalStatus(StrEnum):
    REQUESTED = "requested"
    WAITING = "waiting"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class ToolResultStatus(StrEnum):
    SUCCESS = "success"
    ERROR = "error"
    DENIED = "denied"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ToolCapability:
    name: str
    version: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    permission: ToolPermission
    risk_level: RiskLevel
    runtime_location: str = "hands"
    timeout_seconds: float = 30.0
    owner: str = "platform"
    allowed_credential_operations: tuple[str, ...] = ()
    # Only server-registered, read-only authority queries may opt out of replay.
    # This is not an invocation option and is never populated from MCP metadata.
    cache_result: bool = True
    invocation_ref: CapabilityInvocationRef | None = None

    def __post_init__(self) -> None:
        if not self.cache_result and self.permission is not ToolPermission.READ_ONLY:
            raise ValueError("Only read-only queries may disable result replay")


@dataclass(frozen=True)
class CredentialReference:
    credential_ref: str
    provider: str
    account_scope: str
    allowed_operations: tuple[str, ...]
    expires_at: datetime


@dataclass(frozen=True)
class ToolInvocation:
    tool_invocation_id: str
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    tool_name: str
    tool_version: str
    arguments: dict[str, Any]
    expected_side_effect: str
    idempotency_key: str
    deadline: datetime | None
    fencing_token: int
    actor_id: str
    approval_id: str | None = None
    credential_ref: str | None = None
    user_id: str | None = None
    actor_role: str | None = None
    dept_id: str | None = None
    capability_ref: CapabilityInvocationRef | None = None


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: str
    version: int
    content_hash: str
    media_type: str
    size: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "version": self.version,
            "content_hash": self.content_hash,
            "media_type": self.media_type,
            "size": self.size,
        }


@dataclass(frozen=True)
class ArtifactMetadata:
    artifact_id: str
    tenant_id: str
    root_session_id: str
    session_id: str
    artifact_type: str
    media_type: str
    name: str
    version: int
    content_hash: str
    size: int
    storage_ref: str
    producer: str
    lineage_refs: tuple[str, ...]
    classification: str
    acl: tuple[str, ...]
    created_at: datetime
    retention_until: datetime | None = None

    def public_ref(self) -> ArtifactRef:
        return ArtifactRef(
            artifact_id=self.artifact_id,
            version=self.version,
            content_hash=self.content_hash,
            media_type=self.media_type,
            size=self.size,
        )


@dataclass(frozen=True)
class ApprovalRecord:
    approval_id: str
    tenant_id: str
    session_id: str
    run_id: str
    action_digest: str
    tool_name: str
    redacted_arguments: dict[str, Any]
    risk: RiskLevel
    reason: str
    expected_effect: str
    allowed_decisions: tuple[str, ...]
    assigned_approvers: tuple[str, ...]
    policy_version: str
    expires_at: datetime
    status: ApprovalStatus = ApprovalStatus.WAITING
    decision: str | None = None
    feedback: str | None = None

    def as_event_payload(self) -> dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "run_id": self.run_id,
            "action_digest": self.action_digest,
            "tool_name": self.tool_name,
            "redacted_arguments": self.redacted_arguments,
            "risk": self.risk.value,
            "reason": self.reason,
            "expected_effect": self.expected_effect,
            "allowed_decisions": list(self.allowed_decisions),
            "assigned_approvers": list(self.assigned_approvers),
            "policy_version": self.policy_version,
            "expires_at": self.expires_at.isoformat(),
            "status": self.status.value,
        }


@dataclass(frozen=True)
class ToolResult:
    status: ToolResultStatus
    content: str | dict[str, Any] | ArtifactRef | None = None
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    side_effect_status: str = "not_started"

    def as_dict(self) -> dict[str, Any]:
        content: str | dict[str, Any] | None
        if isinstance(self.content, ArtifactRef):
            content = {"artifact_ref": self.content.as_dict()}
        else:
            content = self.content
        return {
            "status": self.status.value,
            "content": content,
            "summary": self.summary,
            "metadata": self.metadata,
            "error_code": self.error_code,
            "side_effect_status": self.side_effect_status,
        }
