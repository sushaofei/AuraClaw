from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Query, Response, status

from auraclaw.api.dependencies import (
    RequestIdentity,
    command_context,
    get_sync_invocation_gateway,
    get_task_command_gateway,
    get_task_query_service,
    get_task_result_waiter,
    request_identity,
)
from auraclaw.api.models import (
    ActivityPageResponse,
    AppendMessageRequest,
    ApprovalCommandResponse,
    ApprovalResponseRequest,
    CancelTaskRequest,
    CloseSessionRequest,
    CommandResponse,
    CreateTaskRequest,
    RequestRunRequest,
    SyncCreateTaskRequest,
    TaskAcceptedResponse,
    TaskListResponse,
    TaskView,
)
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.gateways.query.waiter import TaskResultWaiter, WaitedResult, decorate_result
from auraclaw.gateways.task.commands import TaskCommandGateway
from auraclaw.gateways.task.invocations import SyncInvocationGateway

router = APIRouter(prefix="/v1", tags=["tasks"])
Identity = Annotated[RequestIdentity, Depends(request_identity)]
TaskCommandDependency = Annotated[TaskCommandGateway, Depends(get_task_command_gateway)]
TaskQueryDependency = Annotated[TaskQueryService, Depends(get_task_query_service)]
TaskWaiterDependency = Annotated[TaskResultWaiter, Depends(get_task_result_waiter)]
SyncInvocationDependency = Annotated[SyncInvocationGateway, Depends(get_sync_invocation_gateway)]


def _apply_wait_outcome(
    response: Response, waited: WaitedResult, session_id: str
) -> dict[str, Any]:
    body = decorate_result(waited.result, session_id=session_id, outcome=waited.outcome)
    if waited.outcome == "timeout":
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "2"
    elif waited.outcome in {"needs_human", "needs_resume"}:
        response.status_code = status.HTTP_409_CONFLICT
    else:
        response.status_code = status.HTTP_200_OK
    return body


@router.get("/approval-modes")
async def approval_modes(identity: Identity) -> dict[str, Any]:
    del identity
    return {
        "version": 1,
        "modes": ["request_approval", "auto_review", "full_access"],
        "defaults": {"streaming": "request_approval", "non_streaming": "full_access"},
    }


@router.post("/tasks", response_model=TaskAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_task(
    request: CreateTaskRequest,
    identity: Identity,
    service: TaskCommandDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=0,
        operation="create_task",
    )
    return await service.create_task(
        goal=request.goal,
        context=context,
        read_refresh=[grant.model_dump() for grant in request.read_refresh],
        source=request.source,
        schedule_id=request.schedule_id,
        occurrence_id=request.occurrence_id,
        interaction_mode=request.interaction_mode,
        approval_mode=request.approval_mode,
    )


@router.post("/tasks/sync")
async def sync_invoke_task(
    request: SyncCreateTaskRequest,
    response: Response,
    identity: Identity,
    service: SyncInvocationDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=0,
        operation="create_task",
    )
    accepted, waited = await service.invoke(
        goal=request.goal,
        context=context,
        timeout_seconds=request.timeout_seconds,
        approval_mode=request.approval_mode,
    )
    return _apply_wait_outcome(response, waited, str(accepted["session_id"]))


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks(
    identity: Identity,
    query: TaskQueryDependency,
    kind: str | None = Query(default=None, pattern="^(chat|scheduled)$"),
    status_filter: str | None = Query(default=None, alias="status"),
    cursor: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    return await query.list_tasks(
        tenant_id=identity.tenant_id,
        kind=kind,
        status=status_filter,
        cursor=cursor,
        limit=min(max(limit, 1), 100),
    )


@router.get("/tasks/{session_id}", response_model=TaskView)
async def get_task(
    session_id: str,
    response: Response,
    identity: Identity,
    query: TaskQueryDependency,
    min_version: int | None = None,
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> dict[str, Any]:
    task = await query.get_task(tenant_id=identity.tenant_id, session_id=session_id)
    if min_version is not None and int(task["projection_version"]) < min_version:
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "1"
    etag = f'W/"{task["projection_version"]}"'
    response.headers["ETag"] = etag
    projection_is_fresh = min_version is None or int(task["projection_version"]) >= min_version
    if projection_is_fresh and task["run_status"] not in {"completed", "failed", "cancelled"}:
        response.headers["Retry-After"] = "2"
    if if_none_match == etag and projection_is_fresh:
        response.status_code = status.HTTP_304_NOT_MODIFIED
    return task


@router.get("/tasks/{session_id}/children")
async def list_children(
    session_id: str,
    identity: Identity,
    query: TaskQueryDependency,
) -> dict[str, Any]:
    children = await query.list_children(identity.tenant_id, session_id)
    return {"root_session_id": session_id, "children": children}


@router.get("/tasks/{session_id}/result")
async def get_result(
    session_id: str,
    response: Response,
    identity: Identity,
    query: TaskQueryDependency,
    waiter: TaskWaiterDependency,
    min_version: int | None = None,
    wait: bool = False,
    timeout_seconds: int | None = Query(default=None, ge=1, le=3600),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> dict[str, Any]:
    if wait:
        waited = await waiter.wait(
            identity.tenant_id,
            session_id,
            timeout_seconds=waiter.clamp_timeout(timeout_seconds),
        )
        return _apply_wait_outcome(response, waited, session_id)

    result = await query.get_result(tenant_id=identity.tenant_id, session_id=session_id)
    projection_is_fresh = min_version is None or int(result["projection_version"]) >= min_version
    result_is_ready = result["status"] in {"completed", "failed", "cancelled"}
    if not projection_is_fresh or not result_is_ready:
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "2"
    etag = f'W/"{result["projection_version"]}"'
    response.headers["ETag"] = etag
    if if_none_match == etag and projection_is_fresh and result_is_ready:
        response.status_code = status.HTTP_304_NOT_MODIFIED
    return result


@router.get("/tasks/{session_id}/transcript")
async def get_transcript(
    session_id: str,
    identity: Identity,
    query: TaskQueryDependency,
) -> dict[str, Any]:
    return await query.get_transcript(tenant_id=identity.tenant_id, session_id=session_id)


@router.get("/tasks/{session_id}/activity", response_model=ActivityPageResponse)
async def get_activity(
    session_id: str,
    response: Response,
    identity: Identity,
    query: TaskQueryDependency,
    after_version: int = Query(default=0, ge=0),
    limit: int = Query(default=200, ge=1, le=200),
) -> dict[str, Any]:
    activity = await query.get_activity(
        tenant_id=identity.tenant_id,
        session_id=session_id,
        after_version=after_version,
        limit=limit,
    )
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Activity-Version"] = str(activity["source_version"])
    return activity


@router.post(
    "/sessions/{session_id}/messages",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def append_message(
    session_id: str,
    request: AppendMessageRequest,
    identity: Identity,
    service: TaskCommandDependency,
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
    service: TaskCommandDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
    expected_version: int = Header(alias="X-Expected-Version"),
    request: RequestRunRequest | None = None,
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
        operation="request_run",
    )
    return await service.request_run(
        session_id=session_id,
        context=context,
        approval_mode=request.approval_mode if request else None,
        read_refresh=[grant.model_dump() for grant in request.read_refresh] if request else [],
    )


@router.post(
    "/sessions/{session_id}/cancel",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def cancel_task(
    session_id: str,
    request: CancelTaskRequest,
    identity: Identity,
    service: TaskCommandDependency,
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
    "/sessions/{session_id}/close",
    response_model=CommandResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def close_session(
    session_id: str,
    request: CloseSessionRequest,
    identity: Identity,
    service: TaskCommandDependency,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
    expected_version: int = Header(alias="X-Expected-Version"),
) -> dict[str, Any]:
    context = command_context(
        identity=identity,
        command_id=idempotency_key,
        expected_version=expected_version,
        operation="close_session",
    )
    return await service.close_session(
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
    service: TaskCommandDependency,
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
    service: TaskCommandDependency,
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
