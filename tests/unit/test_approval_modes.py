from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

from auraclaw.contracts.approval_mode import ApprovalMode, InteractionMode
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import (
    AuthorizationError,
    InvalidTransitionError,
    VersionConflictError,
)
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.internal import (
    InternalRequestContext,
    PolicyEvaluateRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.domain.session import SessionAggregate
from auraclaw.gateways.query.activity import build_activity
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import policy_routes
from auraclaw.policy.approval_modes import ApprovalModeResolver, ReviewResult
from auraclaw.policy.internal_service import PolicyInternalService
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.session.task_service import TaskService


def context(
    command: str = "create", version: int = 0, operation: str = "create_task"
) -> CommandContext:
    return CommandContext(
        command_id=command,
        tenant_id="tenant",
        actor=Actor(type="user", id="user"),
        correlation_id="test",
        expected_version=version,
        operation=operation,
    )


async def setup(
    mode: ApprovalMode | None = None, interaction: InteractionMode = InteractionMode.STREAMING
):
    events = InMemoryEventStore()
    projection = InMemoryTaskProjection()
    task = TaskService(
        event_store=events,
        reader=projection,
        relay=OutboxRelay(events, projection),
        admission=AllowAllAdmissionController(),
    )
    accepted = await task.create_task(
        goal="Inspect this sensitive report",
        context=context(),
        interaction_mode=interaction,
        approval_mode=mode,
    )
    request = PolicyEvaluateRequest(
        context=InternalRequestContext(
            tenant_id="tenant",
            service_identity=ServiceIdentity.ACTION_HANDS,
            request_id="policy",
            correlation_id="test",
            causation_id="tool",
        ),
        session_id=accepted["session_id"],
        run_id=accepted["run_id"],
        subject="runtime",
        action="sensitive.read",
        resource="report",
        input_digest="parameters",
        attributes={"action_kind": "resource", "permission": "read-only"},
    )
    return events, projection, task, accepted, request


@pytest.mark.parametrize(
    "interaction,expected",
    [
        (InteractionMode.STREAMING, ApprovalMode.REQUEST_APPROVAL),
        (InteractionMode.NON_STREAMING, ApprovalMode.FULL_ACCESS),
    ],
)
async def test_defaults_durable_and_idempotent(interaction, expected):
    events, projection, task, accepted, request = await setup(interaction=interaction)
    assert accepted["effective_approval_mode"] == expected
    assert "_request_fingerprint" not in accepted
    repeated = await task.create_task(
        goal="Inspect this sensitive report", context=context(), interaction_mode=interaction
    )
    assert repeated == accepted
    with pytest.raises(VersionConflictError):
        await task.create_task(
            goal="Inspect this sensitive report",
            context=context(),
            approval_mode=ApprovalMode.AUTO_REVIEW,
        )
    stored = await events.load("tenant", request.session_id)
    aggregate = SessionAggregate.from_events(stored)
    restored = SessionAggregate.from_snapshot(aggregate.snapshot_state(), aggregate.version)
    assert restored.approval.effective_approval_mode == expected
    await projection.rebuild(stored)
    assert (await projection.get_task("tenant", request.session_id))[
        "effective_approval_mode"
    ] == expected


@pytest.mark.parametrize("mode", list(ApprovalMode))
@pytest.mark.parametrize("decision", list(PolicyDecision))
async def test_all_policy_decisions_and_non_write_action(mode, decision):
    events, _, _, _, request = await setup(mode)
    reviewer = AsyncMock()
    reviewer.review.return_value = ReviewResult(
        approved=True, reason="Explicitly authorized safe action"
    )
    resolver = ApprovalModeResolver(events, reviewer)
    actual, evidence = await resolver.resolve(request, decision, "policy-v1")
    expected = decision
    if decision == PolicyDecision.REQUIRE_APPROVAL and mode != ApprovalMode.REQUEST_APPROVAL:
        expected = PolicyDecision.ALLOW
    assert actual == expected
    assert reviewer.review.await_count == int(
        decision == PolicyDecision.REQUIRE_APPROVAL and mode == ApprovalMode.AUTO_REVIEW
    )
    if evidence:
        assert evidence["effective_approval_mode"] == mode


@pytest.mark.parametrize("failure", [False, TimeoutError(), ValueError("invalid JSON")])
async def test_auto_escalation_persists_across_restart(failure):
    events, _, _, _, request = await setup(ApprovalMode.AUTO_REVIEW)
    reviewer = AsyncMock()
    if failure is False:
        reviewer.review.return_value = ReviewResult(
            approved=False, reason="Cannot verify disclosure scope"
        )
    else:
        reviewer.review.side_effect = failure
    first = await ApprovalModeResolver(events, reviewer).resolve(
        request, PolicyDecision.REQUIRE_APPROVAL, "v1"
    )
    assert first[0] == PolicyDecision.REQUIRE_APPROVAL
    reviewer.review.side_effect = None
    reviewer.review.return_value = ReviewResult(approved=True, reason="late approval")
    second = await ApprovalModeResolver(events, reviewer).resolve(
        request, PolicyDecision.REQUIRE_APPROVAL, "v1"
    )
    assert first == second
    assert reviewer.review.await_count == 1
    trace = build_activity(await events.load("tenant", request.session_id))
    assert any(node["title"] == "需要你确认" for node in trace)


async def test_late_auto_approval_cannot_override_human_escalation():
    events, _, _, _, request = await setup(ApprovalMode.AUTO_REVIEW)
    slow = AsyncMock()
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed(*args, **kwargs):
        started.set()
        await release.wait()
        return ReviewResult(approved=True, reason="late")

    slow.review.side_effect = delayed
    pending = asyncio.create_task(
        ApprovalModeResolver(events, slow).resolve(request, PolicyDecision.REQUIRE_APPROVAL, "v1")
    )
    await started.wait()
    winner = await ApprovalModeResolver(events).resolve(
        request, PolicyDecision.REQUIRE_APPROVAL, "v1"
    )
    release.set()
    assert (await pending) == winner
    assert winner[0] == PolicyDecision.REQUIRE_APPROVAL
    completed = [
        e
        for e in await events.load("tenant", request.session_id)
        if e.type == "policy.review.completed"
    ]
    assert len(completed) == 1


async def test_next_run_change_is_atomic_bound_and_replayable():
    events, _, task, accepted, request = await setup()
    with pytest.raises(InvalidTransitionError):
        await task.request_run(
            session_id=request.session_id,
            context=context("next", 2, "request_run"),
            approval_mode=ApprovalMode.FULL_ACCESS,
        )
    await events.append(
        root_session_id=request.session_id,
        session_id=request.session_id,
        run_id=request.run_id,
        context=context("complete", 2, "complete"),
        events=[NewEvent(type="run.completed", payload={})],
        command_result={},
    )
    ctx = context("next", 3, "request_run")
    next_run = await task.request_run(
        session_id=request.session_id, context=ctx, approval_mode=ApprovalMode.FULL_ACCESS
    )
    assert next_run == await task.request_run(
        session_id=request.session_id, context=ctx, approval_mode=ApprovalMode.FULL_ACCESS
    )
    with pytest.raises(VersionConflictError):
        await task.request_run(
            session_id=request.session_id, context=ctx, approval_mode=ApprovalMode.AUTO_REVIEW
        )
    for wrong in [
        request,
        request.model_copy(
            update={"context": request.context.model_copy(update={"tenant_id": "other"})}
        ),
    ]:
        with pytest.raises(AuthorizationError):
            await ApprovalModeResolver(events).resolve(wrong, PolicyDecision.REQUIRE_APPROVAL, "v1")
    assert next_run["effective_approval_mode"] == "full_access"


async def test_remote_policy_uses_canonical_mode_not_attributes():
    events, _, _, _, request = await setup()
    service = PolicyInternalService(mode_resolver=ApprovalModeResolver(events))
    from auraclaw.infrastructure.clients.policy import RemotePolicyClient

    app = create_contract_app(
        "policy",
        policy_routes(service),
        workload_identities={"hands-token": ServiceIdentity.ACTION_HANDS},
    )
    client = RemotePolicyClient(
        "http://policy", bearer_token="hands-token", transport=httpx.ASGITransport(app)
    )
    try:
        response = await client.evaluate_action(
            tenant_id="tenant",
            subject="runtime",
            action="write",
            resource="report",
            input_digest="parameters",
            correlation_id="test",
            attributes={
                "permission": "write-with-approval",
                "session_id": request.session_id,
                "run_id": request.run_id,
                "approval_mode": "full_access",
            },
        )
        assert response.decision == PolicyDecision.REQUIRE_APPROVAL
    finally:
        await client.aclose()


async def test_reviewer_gateway_boundary_and_structured_result():
    from auraclaw.infrastructure.clients.approval_reviewer import RemoteAutoApprovalReviewer
    from auraclaw.internal.routes import model_routes
    from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
    from auraclaw.runtime.ports import ModelResponse

    model = AsyncMock()

    # Force the stable generate port rather than AsyncMock's fabricated streaming method.
    class Model:
        async def generate(self, request):
            assert request.tools == ()
            assert request.max_output_tokens == 512
            model.calls.append(request)
            return ModelResponse(
                model_call_id=request.model_call_id,
                provider="test",
                model="reviewer",
                completed_output='{"approved":true,"reason":"Safe and within explicit scope"}',
                deltas=(),
            )

    model.calls = []
    gateway = ModelGatewayInternalService(Model())
    app = create_contract_app(
        "model", model_routes(gateway), workload_identities={"policy-token": ServiceIdentity.POLICY}
    )
    reviewer = RemoteAutoApprovalReviewer(
        "http://model", bearer_token="policy-token", transport=httpx.ASGITransport(app)
    )
    _, _, _, _, request = await setup(ApprovalMode.AUTO_REVIEW)
    try:
        result = await reviewer.review(
            request, review_id="r1", user_intent="Read the report", action={"target": "report"}
        )
        assert result.approved
        assert len(model.calls) == 1
    finally:
        await reviewer.aclose()
    from auraclaw.contracts.internal import ModelGenerateRequest

    for identity, purpose, tools in [
        (ServiceIdentity.POLICY, "execution", ()),
        (ServiceIdentity.POLICY, "approval_review", ({"name": "write"},)),
        (ServiceIdentity.AGENT_RUNTIME, "approval_review", ()),
    ]:
        with pytest.raises(AuthorizationError):
            await gateway.generate(
                ModelGenerateRequest(
                    context=request.context.model_copy(update={"service_identity": identity}),
                    purpose=purpose,
                    model_call_id="invalid",
                    run_id=request.run_id,
                    messages=(),
                    tools=tools,
                    max_output_tokens=512,
                )
            )


async def test_public_defaults_and_validation():
    from fastapi import FastAPI

    from auraclaw.api.dependencies import (
        RequestIdentity,
        get_sync_invocation_gateway,
        get_task_command_gateway,
        request_identity,
    )
    from auraclaw.api.routes.tasks import router
    from auraclaw.gateways.query.waiter import WaitedResult
    from auraclaw.gateways.task.commands import TaskCommandGateway
    from auraclaw.gateways.task.invocations import SyncInvocationGateway

    events, projection, service, _, _ = await setup()
    commands = TaskCommandGateway(service)
    waiter = AsyncMock()
    waiter.clamp_timeout = lambda value: 1

    async def wait_for_human(tenant_id, session_id, **kwargs):
        return WaitedResult(
            outcome="needs_human", result={"session_id": session_id, "status": "waiting_for_human"}
        )

    waiter.wait.side_effect = wait_for_human
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[request_identity] = lambda: RequestIdentity(
        tenant_id="tenant", actor=Actor(type="user", id="user"), correlation_id="test"
    )
    app.dependency_overrides[get_task_command_gateway] = lambda: commands
    app.dependency_overrides[get_sync_invocation_gateway] = lambda: SyncInvocationGateway(
        commands, waiter
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://api"
    ) as client:
        caps = await client.get("/v1/approval-modes")
        assert caps.json()["defaults"]["non_streaming"] == "full_access"
        created = await client.post(
            "/v1/tasks", headers={"Idempotency-Key": "public"}, json={"goal": "hello"}
        )
        assert (
            created.status_code == 202
            and created.json()["effective_approval_mode"] == "request_approval"
        )
        invalid = await client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "invalid"},
            json={"goal": "hello", "approval_mode": "yes"},
        )
        assert invalid.status_code == 422
        sync = await client.post(
            "/v1/tasks/sync", headers={"Idempotency-Key": "sync-public"}, json={"goal": "hello"}
        )
        assert sync.status_code == 409 and sync.json()["code"] == "needs_human"
        sid = sync.json()["session_id"]
        assert (
            SessionAggregate.from_events(
                await events.load("tenant", sid)
            ).approval.effective_approval_mode
            == ApprovalMode.FULL_ACCESS
        )


@pytest.mark.parametrize(
    "mode,approved,expected_calls",
    [
        (ApprovalMode.REQUEST_APPROVAL, True, 0),
        (ApprovalMode.AUTO_REVIEW, True, 1),
        (ApprovalMode.AUTO_REVIEW, False, 0),
        (ApprovalMode.FULL_ACCESS, False, 1),
    ],
)
async def test_hands_remote_policy_executes_only_after_resolved_approval(
    mode, approved, expected_calls
):
    from datetime import UTC, datetime, timedelta

    from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
    from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolInvocation, ToolPermission
    from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
    from auraclaw.infrastructure.clients.policy import RemotePolicyClient
    from auraclaw.projection.approval.projector import InMemoryApprovalProjection

    events, _, _, _, request = await setup(mode)
    reviewer = AsyncMock()
    reviewer.review.return_value = ReviewResult(approved=approved, reason="review outcome")
    service = PolicyInternalService(mode_resolver=ApprovalModeResolver(events, reviewer))
    app = create_contract_app(
        "policy",
        policy_routes(service),
        workload_identities={"token": ServiceIdentity.ACTION_HANDS},
    )
    client = RemotePolicyClient(
        "http://policy", bearer_token="token", transport=httpx.ASGITransport(app)
    )
    hands = AsyncMock()
    hands.execute.return_value = {"ok": True}
    capability = ToolCapability(
        name="report",
        version="1",
        description="managed report",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        permission=ToolPermission.WRITE_WITH_APPROVAL,
        risk_level=RiskLevel.HIGH,
    )
    gateway = ToolGateway(
        registry=ToolRegistry((capability,)),
        policy=client,
        approvals=InMemoryApprovalProjection(),
        hands=hands,
        artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=b"approval-mode-test-key"),
    )
    invocation = ToolInvocation(
        tool_invocation_id="tool",
        tenant_id="tenant",
        root_session_id=request.session_id,
        session_id=request.session_id,
        run_id=request.run_id,
        tool_name="report",
        tool_version="1",
        arguments={"target": "report"},
        expected_side_effect="write",
        idempotency_key="tool",
        deadline=datetime.now(UTC) + timedelta(minutes=1),
        fencing_token=1,
        actor_id="runtime",
    )
    try:
        first = await gateway.execute(invocation)
        await gateway.execute(invocation)
        assert hands.execute.await_count == expected_calls
        assert first.error_code == (None if expected_calls else "approval_required")
    finally:
        await client.aclose()


async def test_schedule_default_and_child_inheritance_ignore_metadata_mode():
    from dataclasses import replace

    from auraclaw.contracts.collaboration import ChildSpec, CollaborationRole, OutputContract
    from auraclaw.session.collaboration_service import CollaborationService

    events, projection, task, _, _ = await setup()
    scheduled = await task.create_task(
        goal="scheduled work",
        source="schedule",
        schedule_id="schedule",
        occurrence_id="one",
        context=context("schedule"),
    )
    assert scheduled["effective_approval_mode"] == "full_access"
    parent = await task.create_task(
        goal="parent", context=context("parent"), approval_mode=ApprovalMode.AUTO_REVIEW
    )
    collaboration = CollaborationService(event_store=events, relay=OutboxRelay(events, projection))
    child = await collaboration.create_child(
        root_session_id=parent["session_id"],
        parent_session_id=parent["session_id"],
        context=replace(
            context("child", operation="create_child"), actor=Actor(type="coordinator", id="test")
        ),
        spec=ChildSpec(
            task_key="child",
            role=CollaborationRole.WORKER,
            goal="child work",
            output_contract=OutputContract(),
            metadata={"approval_mode": "full_access"},
        ),
    )
    state = SessionAggregate.from_events(await events.load("tenant", child["session_id"]))
    assert state.approval.effective_approval_mode == ApprovalMode.AUTO_REVIEW
    assert state.approval.approval_mode_source == "inherited"


async def test_pending_approval_cache_is_scoped_to_run():
    from dataclasses import replace
    from datetime import UTC, datetime, timedelta

    from auraclaw.action.policy import PolicyEngine
    from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
    from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolInvocation, ToolPermission
    from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
    from auraclaw.projection.approval.projector import InMemoryApprovalProjection

    capability = ToolCapability(
        name="report",
        version="1",
        description="report",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        permission=ToolPermission.WRITE_WITH_APPROVAL,
        risk_level=RiskLevel.HIGH,
    )
    hands = AsyncMock()
    gateway = ToolGateway(
        registry=ToolRegistry((capability,)),
        policy=PolicyEngine(),
        approvals=InMemoryApprovalProjection(),
        hands=hands,
        artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=b"approval-mode-test-key"),
    )
    first = ToolInvocation(
        tool_invocation_id="first",
        tenant_id="tenant",
        root_session_id="s",
        session_id="s",
        run_id="first-run",
        tool_name="report",
        tool_version="1",
        arguments={},
        expected_side_effect="write",
        idempotency_key="first",
        deadline=datetime.now(UTC) + timedelta(minutes=1),
        fencing_token=1,
        actor_id="runtime",
    )
    second = replace(
        first, tool_invocation_id="second", run_id="second-run", idempotency_key="second"
    )
    one, two = await gateway.execute(first), await gateway.execute(second)
    assert (
        one.metadata["approval_request"]["approval_id"]
        != two.metadata["approval_request"]["approval_id"]
    )
    assert two.metadata["approval_request"]["run_id"] == "second-run"
    assert hands.execute.await_count == 0
