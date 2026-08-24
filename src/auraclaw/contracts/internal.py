from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

INTERNAL_API_VERSION = "2026-07-22"


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ServiceIdentity(StrEnum):
    TASK_API = "task-api"
    SESSION = "session"
    PROJECTION_WORKER = "projection-worker"
    ORCHESTRATOR = "orchestrator"
    AGENT_RUNTIME = "agent-runtime"
    MODEL_GATEWAY = "model-gateway"
    ACTION_HANDS = "action-hands"
    POLICY = "policy"
    CREDENTIAL_PROXY = "credential-proxy"
    ARTIFACT_SERVICE = "artifact-service"
    STREAMING_GATEWAY = "streaming-gateway"
    DELIVERY_WORKER = "delivery-worker"


class InternalErrorCode(StrEnum):
    INVALID_REQUEST = "invalid_request"
    INVALID_TRANSITION = "invalid_transition"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not_found"
    VERSION_CONFLICT = "version_conflict"
    LEASE_LOST = "lease_lost"
    STALE_FENCING_TOKEN = "stale_fencing_token"
    POLICY_DENIED = "policy_denied"
    APPROVAL_REQUIRED = "approval_required"
    APPROVAL_INVALID = "approval_invalid"
    CREDENTIAL_DENIED = "credential_access_denied"
    ARTIFACT_DENIED = "artifact_access_denied"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    INTERNAL_ERROR = "internal_error"


class InternalError(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    code: InternalErrorCode
    message: str
    detail: str | None = None
    retryable: bool = False


class InternalRequestContext(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    tenant_id: str = Field(min_length=1)
    service_identity: ServiceIdentity
    request_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    causation_id: str = Field(min_length=1)
    deadline: datetime | None = None


class LeaseAssertion(ContractModel):
    key_id: str
    audience: str
    tenant_id: str
    root_session_id: str | None = None
    session_id: str
    run_id: str
    runtime_id: str | None = None
    user_id: str | None = None
    lease_id: str
    fencing_token: int = Field(ge=1)
    expires_at: datetime
    signature: str


class EventInput(ContractModel):
    type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    visibility: str = "internal"


class SessionAppendRequest(ContractModel):
    context: InternalRequestContext
    root_session_id: str
    session_id: str
    run_id: str | None = None
    command_id: str
    expected_version: int = Field(ge=0)
    operation: str
    actor_type: str
    actor_id: str
    events: tuple[EventInput, ...]
    command_result: dict[str, Any] = Field(default_factory=dict)
    lease_assertion: LeaseAssertion | None = None


class SessionAppendResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    events: tuple[dict[str, Any], ...]
    command_result: dict[str, Any]
    deduplicated: bool = False


class SessionFeedRequest(ContractModel):
    context: InternalRequestContext
    session_id: str
    from_version: int = Field(default=1, ge=1)
    limit: int = Field(default=100, ge=1, le=1000)
    event_types: tuple[str, ...] | None = None


class SessionFeedResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    events: tuple[dict[str, Any], ...]
    next_version: int | None = None


class OutboxClaimRequest(ContractModel):
    context: InternalRequestContext
    destination: Literal["projection", "delivery", "control"]
    worker_id: str
    limit: int = Field(default=100, ge=1, le=1000)
    claim_ttl_seconds: int = Field(default=30, ge=1, le=3600)
    wait_seconds: float = Field(default=0, ge=0, le=30)


class OutboxRecord(ContractModel):
    outbox_id: str
    event_id: str
    event: dict[str, Any]
    claim_token: str
    attempt: int = Field(ge=1)


class OutboxClaimResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    records: tuple[OutboxRecord, ...]


class OutboxDispositionRequest(ContractModel):
    context: InternalRequestContext
    destination: Literal["projection", "delivery", "control"]
    worker_id: str
    outbox_id: str
    claim_token: str
    disposition: Literal["ack", "nack", "poison"]
    reason: str | None = None


class OutboxDispositionResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    accepted: bool


class CheckpointState(ContractModel):
    phase: str
    resume_cursor: str | None = None
    artifact_refs: tuple[str, ...] = ()
    harness_state: dict[str, Any] = Field(default_factory=dict)


class SaveCheckpointRequest(ContractModel):
    context: InternalRequestContext
    session_id: str
    run_id: str
    lease_assertion: LeaseAssertion
    state: CheckpointState


class LoadCheckpointRequest(ContractModel):
    context: InternalRequestContext
    session_id: str
    run_id: str


class CheckpointResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    found: bool
    fencing_token: int | None = None
    state: CheckpointState | None = None
    updated_at: datetime | None = None


class CancellationRequest(ContractModel):
    context: InternalRequestContext
    session_id: str
    run_id: str


class CancellationResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    cancelled: bool


class ValidateLeaseRequest(ContractModel):
    context: InternalRequestContext
    assertion: LeaseAssertion


class ValidateLeaseResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    valid: bool
    fencing_token: int
    expires_at: datetime


class RuntimeRegistrationRequest(ContractModel):
    context: InternalRequestContext
    runtime_id: str
    runtime_type: str
    role: str
    node_id: str
    capabilities: dict[str, Any] = Field(default_factory=dict)
    capacity: int = Field(ge=0)
    draining: bool = False


class RuntimeHeartbeatRequest(ContractModel):
    context: InternalRequestContext
    runtime_id: str
    capacity_available: int = Field(ge=0)
    active_lease_ids: tuple[str, ...] = ()
    draining: bool = False


class RuntimeHeartbeatResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    accepted: bool
    observed_at: datetime


class AssignmentClaimRequest(ContractModel):
    context: InternalRequestContext
    runtime_id: str
    role: str
    capabilities: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=1, ge=1, le=100)


class AssignmentRecord(ContractModel):
    task_id: str
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    runtime_id: str
    lease_assertion: LeaseAssertion
    role: str
    resource_profile: dict[str, Any] = Field(default_factory=dict)
    budget: dict[str, Any] = Field(default_factory=dict)
    deadline: datetime | None = None


class AssignmentClaimResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    assignments: tuple[AssignmentRecord, ...]


class AssignmentDispositionRequest(ContractModel):
    context: InternalRequestContext
    task_id: str
    runtime_id: str
    lease_id: str
    fencing_token: int = Field(ge=1)
    disposition: Literal["ack", "finish", "fail"]
    outcome: str | None = None


class AssignmentDispositionResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    accepted: bool


class ModelGenerateRequest(ContractModel):
    context: InternalRequestContext
    model_call_id: str
    run_id: str
    messages: tuple[dict[str, Any], ...]
    tools: tuple[dict[str, Any], ...] = ()
    capability: str = "general"
    preferred_model: str | None = None
    allowed_providers: tuple[str, ...] = ()
    data_classification: str = "internal"
    max_output_tokens: int = Field(default=8192, gt=0)


class ModelGenerateResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    model_call_id: str
    provider: str
    model: str
    completed_output: str
    deltas: tuple[str, ...] = ()
    tool_calls: tuple[dict[str, Any], ...] = ()
    finish_reason: str = "stop"
    usage: dict[str, int | float] = Field(default_factory=dict)


class ModelStreamEvent(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    model_call_id: str
    sequence: int = Field(ge=1)
    type: Literal["delta", "tool_call", "usage", "completed", "error"]
    payload: dict[str, Any] = Field(default_factory=dict)


class ModelCancelRequest(ContractModel):
    context: InternalRequestContext
    model_call_id: str
    run_id: str


class ModelCancelResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    model_call_id: str
    cancelled: bool


class RuntimeServiceConfig(ContractModel):
    runtime_id: str
    control_base_url: str
    session_base_url: str
    model_gateway_base_url: str
    hands_url: str
    artifact_base_url: str
    workload_token_file: str


class PolicyEvaluateRequest(ContractModel):
    context: InternalRequestContext
    subject: str
    action: str
    resource: str
    input_digest: str
    policy_version: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)


class PolicyEvaluateResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    decision_id: str
    decision: Literal["allow", "deny", "allow_with_constraints", "require_approval"]
    policy_version: str
    constraints: dict[str, Any] = Field(default_factory=dict)
    expires_at: datetime


class PolicyValidateDecisionRequest(ContractModel):
    context: InternalRequestContext
    decision_id: str
    action: str
    resource: str


class PolicyValidateDecisionResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    valid: bool
    decision: str
    policy_version: str
    constraints: dict[str, Any] = Field(default_factory=dict)
    expires_at: datetime


class ApprovalRequest(ContractModel):
    context: InternalRequestContext
    approval_id: str
    session_id: str
    run_id: str
    action_digest: str
    policy_version: str
    expires_at: datetime


class ApprovalCommandRequest(ContractModel):
    context: InternalRequestContext
    operation: Literal[
        "request",
        "record_human_response",
        "validate",
        "cancel",
        "expire",
    ]
    approval_id: str
    session_id: str
    run_id: str
    action_digest: str
    policy_version: str
    decision: str | None = None
    feedback: str | None = None
    expires_at: datetime | None = None


class ApprovalValidationResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    valid: bool
    status: str


class CredentialInvokeRequest(ContractModel):
    context: InternalRequestContext
    session_id: str
    credential_ref: str
    operation: str
    target: str
    method: str
    policy_decision_id: str
    request: dict[str, Any] = Field(default_factory=dict)


class CredentialInvokeResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    usage_id: str
    status: str
    response: dict[str, Any] = Field(default_factory=dict)
    side_effect_status: str = "completed"


class CredentialSignRequest(ContractModel):
    context: InternalRequestContext
    credential_ref: str
    operation: str
    target: str
    policy_decision_id: str
    payload_digest: str


class CredentialSignResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    usage_id: str
    signed_headers: dict[str, str] = Field(default_factory=dict)
    expires_at: datetime


class CredentialResourceRequest(ContractModel):
    context: InternalRequestContext
    operation: Literal["initialize", "revoke"]
    credential_ref: str
    resource: str
    allowed_operations: tuple[str, ...] = ()
    expires_at: datetime | None = None


class CredentialResourceResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    credential_ref: str
    status: Literal["active", "revoked"]


class ArtifactCreateUploadRequest(ContractModel):
    context: InternalRequestContext
    root_session_id: str
    session_id: str
    name: str
    media_type: str
    expected_size: int = Field(ge=0)
    expected_checksum: str
    classification: str = "internal"


class ArtifactUploadResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    artifact_id: str
    version: int = Field(ge=1)
    upload_id: str
    upload_url: str
    expires_at: datetime
    upload_mode: Literal["single", "multipart"] = "single"
    part_size: int | None = None
    part_urls: tuple[str, ...] = ()


class ArtifactFinalizeRequest(ContractModel):
    context: InternalRequestContext
    artifact_id: str
    version: int = Field(ge=1)
    upload_id: str
    size: int = Field(ge=0)
    checksum: str
    parts: tuple[dict[str, Any], ...] = ()


class ArtifactFinalizeResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    artifact_ref: dict[str, Any]
    status: Literal["uploaded", "scanning", "ready", "quarantined"]


class ArtifactDownloadRequest(ContractModel):
    context: InternalRequestContext
    artifact_id: str
    version: int = Field(ge=1)
    actor_id: str
    policy_decision_id: str


class ArtifactDownloadResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    download_url: str
    expires_at: datetime


class AdminOperationRequest(ContractModel):
    context: InternalRequestContext
    operation_id: str
    owner_service: ServiceIdentity
    operation: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class AdminOperationResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    operation_id: str
    status: Literal["accepted", "running", "completed", "failed"]
    result: dict[str, Any] = Field(default_factory=dict)


class McpRegistryAdminRequest(ContractModel):
    context: InternalRequestContext
    command_id: str
    actor_id: str
    expected_revision: int = 0
    operation: str
    server_id: str | None = None
    config: dict[str, Any] | None = None
    target_revision: int | None = None


class McpRegistryAdminResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    operation_id: str
    status: str
    server_id: str
    target_revision: int | None = None
    result: dict[str, Any] = Field(default_factory=dict)
    safe_error_code: str | None = None


class McpRegistrySnapshotRequest(ContractModel):
    context: InternalRequestContext


class McpRegistrySnapshotResponse(ContractModel):
    api_version: str = INTERNAL_API_VERSION
    servers: tuple[dict[str, Any], ...] = ()

