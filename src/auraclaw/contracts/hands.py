from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, Literal, TypeVar

from pydantic import Field, model_validator

from auraclaw.contracts.internal import ContractModel, LeaseAssertion
from auraclaw.contracts.tools import ArtifactRef

T = TypeVar("T")

HANDS_CONTRACT_VERSION = "2026-08-19"
HANDS_MAX_REQUEST_BYTES = 1 * 1024 * 1024
HANDS_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
HANDS_TOOLS_LIST = "/internal/v1/hands/tools/list"
HANDS_TOOLS_CALL = "/internal/v1/hands/tools/call"
HANDS_RESOURCES_LIST = "/internal/v1/hands/resources/list"
HANDS_RESOURCE_TEMPLATES_LIST = "/internal/v1/hands/resources/templates/list"
HANDS_RESOURCES_READ = "/internal/v1/hands/resources/read"
HANDS_PROMPTS_LIST = "/internal/v1/hands/prompts/list"
HANDS_PROMPTS_GET = "/internal/v1/hands/prompts/get"
HANDS_INVOCATIONS_CANCEL = "/internal/v1/hands/invocations/cancel"
HANDS_INVOCATIONS_STATUS = "/internal/v1/hands/invocations/status"


class HandsTrustedContext(ContractModel):
    """Workload identity recovered from bearer token + signed lease assertion."""

    tenant_id: str = Field(min_length=1)
    root_session_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    runtime_id: str = Field(min_length=1)
    lease_id: str = Field(min_length=1)
    fencing_token: int = Field(ge=1)
    deadline: datetime | None = None
    lease_assertion: LeaseAssertion | None = None
    user_id: str | None = None
    dept_id: str | None = None


class HandsError(ContractModel):
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    detail: str | None = None
    retryable: bool = False
    kind: Literal["transport", "contract", "not_found"] = "contract"


class HandsPage(ContractModel, Generic[T]):
    items: tuple[T, ...] = ()
    next_cursor: str | None = None


class HandsCapabilityDescriptor(ContractModel):
    capability_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    title: str = ""
    description: str = ""
    server_id: str | None = None
    content_digest: str | None = None


class HandsToolDescriptor(ContractModel):
    name: str = Field(min_length=1)
    version: str = "1"
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)
    read_only: bool = False
    destructive: bool = False
    risk_level: str | None = None


class HandsResourceDescriptor(ContractModel):
    name: str = Field(min_length=1)
    uri: str | None = None
    uri_template: str | None = None
    title: str | None = None
    description: str | None = None
    mime_type: str | None = None
    size: int | None = Field(default=None, ge=0)
    classification: str | None = None
    content_digest: str | None = None
    source_revision: str | None = None

    @model_validator(mode="after")
    def validate_locator(self) -> HandsResourceDescriptor:
        if (self.uri is None) == (self.uri_template is None):
            raise ValueError("resource descriptor must define exactly one of uri or uri_template")
        return self


class HandsResourceContent(ContractModel):
    uri: str = Field(min_length=1)
    mime_type: str | None = None
    text: str | None = None
    blob: str | None = None
    artifact_ref: ArtifactRef | None = None
    content_digest: str | None = None
    source_revision: str | None = None
    classification: str = "internal"
    policy_decision_id: str | None = None
    security_findings: tuple[str, ...] = ()
    cache_hit: bool = False
    inline: bool = True

    @model_validator(mode="after")
    def validate_content(self) -> HandsResourceContent:
        if (self.text is None) == (self.blob is None):
            raise ValueError("resource content must define exactly one of text or blob")
        return self


class HandsPromptArgument(ContractModel):
    name: str = Field(min_length=1)
    title: str | None = None
    description: str | None = None
    required: bool = False


class HandsPromptDescriptor(ContractModel):
    name: str = Field(min_length=1)
    title: str | None = None
    description: str | None = None
    arguments: tuple[HandsPromptArgument, ...] = ()


class HandsPromptMessage(ContractModel):
    role: Literal["user", "assistant"]
    content: dict[str, Any]


class HandsPromptResult(ContractModel):
    description: str | None = None
    messages: tuple[HandsPromptMessage, ...]


class HandsToolCall(ContractModel):
    tool_invocation_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    version: str = "1"
    arguments: dict[str, Any] = Field(default_factory=dict)
    expected_side_effect: str = "read"
    idempotency_key: str | None = None
    deadline: datetime | None = None
    approval_id: str | None = None
    credential_ref: str | None = None


class HandsToolResult(ContractModel):
    status: str
    content: str | dict[str, Any] | None = None
    summary: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    error_code: str | None = None
    side_effect_status: str = "not_started"

    def as_dict(self) -> dict[str, Any]:
        payload: str | dict[str, Any] | None = self.content
        if isinstance(payload, dict) and "artifact_ref" in payload:
            payload = dict(payload)
        return {
            "status": self.status,
            "content": payload,
            "summary": self.summary,
            "metadata": dict(self.metadata),
            "error_code": self.error_code,
            "side_effect_status": self.side_effect_status,
        }


class HandsListRequest(ContractModel):
    cursor: str | None = None


class HandsReadResourceRequest(ContractModel):
    uri: str = Field(min_length=1)


class HandsGetPromptRequest(ContractModel):
    name: str = Field(min_length=1)
    arguments: dict[str, str] = Field(default_factory=dict)


class HandsCancelRequest(ContractModel):
    tool_invocation_id: str = Field(min_length=1)


class HandsCancelResponse(ContractModel):
    cancelled: bool


class HandsInvocationStatusResponse(ContractModel):
    found: bool
    status: str | None = None
    side_effect_status: str | None = None
    error_code: str | None = None
    cancel_requested: bool = False
    result: HandsToolResult | None = None


class HandsReadResourceResponse(ContractModel):
    contents: tuple[HandsResourceContent, ...]


class CapabilitySnapshot(ContractModel):
    connector_id: str = Field(min_length=1)
    tools: tuple[HandsToolDescriptor, ...] = ()
    resources: tuple[HandsResourceDescriptor, ...] = ()
    resource_templates: tuple[HandsResourceDescriptor, ...] = ()
    prompts: tuple[HandsPromptDescriptor, ...] = ()
    source_revision: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


def hands_error_from_exception(exc: Exception) -> HandsError:
    code = str(getattr(exc, "code", "") or "internal_error")
    message = str(getattr(exc, "message", "") or exc)
    detail = getattr(exc, "detail", None)
    retryable = int(getattr(exc, "status_code", 500) or 500) >= 500
    if isinstance(exc, KeyError):
        return HandsError(
            code="not_found",
            message="Hands capability not found",
            detail=str(exc),
            kind="not_found",
        )
    if isinstance(exc, (TypeError, ValueError)):
        return HandsError(
            code="invalid_request",
            message="Invalid Hands request",
            detail=str(exc),
            kind="contract",
        )
    return HandsError(
        code=code,
        message=message,
        detail=None if detail is None else str(detail),
        retryable=retryable,
        kind="transport" if retryable else "contract",
    )
