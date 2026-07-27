from typing import Any

from pydantic import BaseModel, Field, field_validator


class CreateTaskRequest(BaseModel):
    goal: str = Field(min_length=1, max_length=100_000)


class CancelTaskRequest(BaseModel):
    reason: str = Field(default="cancelled by user", max_length=2_000)


class CloseSessionRequest(BaseModel):
    reason: str = Field(default="closed by user", max_length=2_000)


class AppendMessageRequest(BaseModel):
    message: str = Field(min_length=1, max_length=100_000)


class ApprovalResponseRequest(BaseModel):
    decision: str = Field(pattern="^(approved|rejected)$")
    feedback: str | None = Field(default=None, max_length=10_000)


class TaskAcceptedResponse(BaseModel):
    session_id: str
    run_id: str
    status: str
    status_url: str
    result_url: str
    stream_url: str


class CommandResponse(BaseModel):
    session_id: str
    run_id: str | None
    status: str
    run_status: str | None = None


class ApprovalCommandResponse(CommandResponse):
    approval_id: str
    decision: str


class TaskView(BaseModel):
    tenant_id: str
    session_id: str
    root_session_id: str
    run_id: str | None
    status: str
    run_status: str | None = None
    goal: str
    progress: float
    current_stage: str
    result_summary: str | None
    result_ref: str | None
    artifact_refs: list[Any]
    error: dict[str, Any] | None
    delivery_status: str | None = None
    delivery_id: str | None = None
    delivery_attempt_count: int = 0
    delivery_response_summary: str | None = None
    projection_version: int
    projected_at: str

    @field_validator("error", mode="before")
    @classmethod
    def coerce_error(cls, value: Any) -> dict[str, Any] | None:
        if value is None or isinstance(value, dict):
            return value
        return {"message": str(value)}


class ErrorResponse(BaseModel):
    code: str
    message: str
    detail: str | None = None
