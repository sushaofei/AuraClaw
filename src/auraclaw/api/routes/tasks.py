from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Response, status

from auraclaw.api.dependencies import (
    RequestIdentity,
    command_context,
    get_task_service,
    request_identity,
)
from auraclaw.api.models import (
    CancelTaskRequest,
    CommandResponse,
    CreateTaskRequest,
    TaskAcceptedResponse,
    TaskView,
)
from auraclaw.application.tasks import TaskService

router = APIRouter(prefix="/v1", tags=["tasks"])
Identity = Annotated[RequestIdentity, Depends(request_identity)]
TaskServiceDependency = Annotated[TaskService, Depends(get_task_service)]


@router.post("/tasks", response_model=TaskAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_task(
    request: CreateTaskRequest,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=0,
    )
    return await service.create_task(goal=request.goal, context=context)


@router.get("/tasks/{session_id}", response_model=TaskView)
async def get_task(
    session_id: str,
    response: Response,
    identity: Identity,
    service: TaskServiceDependency,
    min_version: int | None = None,
) -> dict[str, Any]:
    task = await service.get_task(tenant_id=identity.tenant_id, session_id=session_id)
    if min_version is not None and int(task["projection_version"]) < min_version:
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "1"
    response.headers["ETag"] = f'W/"{task["projection_version"]}"'
    return task


@router.get("/tasks/{session_id}/result")
async def get_result(
    session_id: str,
    response: Response,
    identity: Identity,
    service: TaskServiceDependency,
) -> dict[str, Any]:
    task = await service.get_task(tenant_id=identity.tenant_id, session_id=session_id)
    if task["status"] not in {"completed", "failed", "cancelled"}:
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "2"
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
    "/sessions/{session_id}/cancel",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_task(
    session_id: str,
    request: CancelTaskRequest,
    identity: Identity,
    service: TaskServiceDependency,
    idempotency_key: str = Header(alias="Idempotency-Key"),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
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
    idempotency_key: str = Header(alias="Idempotency-Key"),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
    )
    return await service.resume_task(session_id=session_id, context=context)
