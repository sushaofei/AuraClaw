from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Response, status

from auraclaw.api.dependencies import (
    RequestIdentity,
    command_context,
    get_collaboration_projection,
    get_task_service,
    request_identity,
)
from auraclaw.api.models import (
    AppendMessageRequest,
    ApprovalCommandResponse,
    ApprovalResponseRequest,
    CancelTaskRequest,
    CommandResponse,
    CreateTaskRequest,
    TaskAcceptedResponse,
    TaskView,
)
from auraclaw.application.tasks import TaskService
from auraclaw.domain.ports import CollaborationReader

router = APIRouter(prefix="/v1", tags=["tasks"])
Identity = Annotated[RequestIdentity, Depends(request_identity)]
TaskServiceDependency = Annotated[TaskService, Depends(get_task_service)]
CollaborationDependency = Annotated[
    CollaborationReader, Depends(get_collaboration_projection)
]


@router.post("/tasks", response_model=TaskAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_task(
    request: CreateTaskRequest,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=0,
        operation="create_task",
    )
    return await service.create_task(goal=request.goal, context=context)


@router.get("/tasks/{session_id}", response_model=TaskView)
async def get_task(
    session_id: str,
    response: Response,
    identity: Identity,
    service: TaskServiceDependency,
    min_version: int | None = None,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> dict[str, Any]:
    task = await service.get_task(tenant_id=identity.tenant_id, session_id=session_id)
    if min_version is not None and int(task["projection_version"]) < min_version:
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "1"
    etag = f'W/"{task["projection_version"]}"'
    response.headers["ETag"] = etag
    projection_is_fresh = min_version is None or int(task["projection_version"]) >= min_version
    if projection_is_fresh and task["status"] not in {"completed", "failed", "cancelled"}:
        response.headers["Retry-After"] = "2"
    if if_none_match == etag and projection_is_fresh:
        response.status_code = status.HTTP_304_NOT_MODIFIED
    return task


@router.get("/tasks/{session_id}/children")
async def list_children(
    session_id: str,
    identity: Identity,
    collaboration: CollaborationDependency,
) -> dict[str, Any]:
    children = await collaboration.list_children(identity.tenant_id, session_id)
    return {"root_session_id": session_id, "children": children}


@router.get("/tasks/{session_id}/result")
async def get_result(
    session_id: str,
    response: Response,
    identity: Identity,
    service: TaskServiceDependency,
    min_version: int | None = None,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> dict[str, Any]:
    task = await service.get_task(tenant_id=identity.tenant_id, session_id=session_id)
    projection_is_fresh = min_version is None or int(task["projection_version"]) >= min_version
    result_is_ready = task["status"] in {"completed", "failed", "cancelled"}
    if not projection_is_fresh or not result_is_ready:
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "2"
    etag = f'W/"{task["projection_version"]}"'
    response.headers["ETag"] = etag
    if if_none_match == etag and projection_is_fresh and result_is_ready:
        response.status_code = status.HTTP_304_NOT_MODIFIED
    return {
        "session_id": session_id,
        "run_id": task["run_id"],
        "status": task["status"],
        "result_summary": task["result_summary"],
        "result_ref": task["result_ref"],
        "artifact_refs": task["artifact_refs"],
        "error": task["error"],
        "projection_version": task["projection_version"],
    }


@router.post(
    "/sessions/{session_id}/messages",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def append_message(
    session_id: str,
    request: AppendMessageRequest,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
        operation="append_message",
    )
    return await service.append_message(
        session_id=session_id, message=request.message, context=context
    )


@router.post(
    "/sessions/{session_id}/runs",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def request_run(
    session_id: str,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
        operation="request_run",
    )
    return await service.request_run(session_id=session_id, context=context)


@router.post(
    "/sessions/{session_id}/cancel",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_task(
    session_id: str,
    request: CancelTaskRequest,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
        operation="cancel_task",
    )
    return await service.cancel_task(
        session_id=session_id,
        reason=request.reason,
        context=context,
    )


@router.post(
    "/sessions/{session_id}/resume",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def resume_task(
    session_id: str,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
        operation="resume_task",
    )
    return await service.resume_task(session_id=session_id, context=context)


@router.post(
    "/sessions/{session_id}/approvals/{approval_id}/responses",
    response_model=ApprovalCommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def record_approval_response(
    session_id: str,
    approval_id: str,
    request: ApprovalResponseRequest,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
        operation="record_approval_response",
    )
    return await service.record_approval_response(
        session_id=session_id,
        approval_id=approval_id,
        decision=request.decision,
        feedback=request.feedback,
        context=context,
    )
