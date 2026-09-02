from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from auraclaw.contracts.errors import CollaborationValidationError
from auraclaw.contracts.events import NewEvent
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint
from auraclaw.runtime.clients import IdempotentToolClient
from auraclaw.runtime.collaboration_controller import (
    AWAIT_CHILDREN,
    CREATE_CHILD,
    JOIN,
    PUBLISH_RESULT,
    PUBLISH_REVIEW,
    RuntimeCollaborationController,
)
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall


class _Control:
    def __init__(self) -> None:
        self.checkpoint: RuntimeCheckpoint | None = None
        self.outcome: str | None = None
        self.suspended: str | None = None

    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None:
        del resource_id, fencing_token

    async def is_cancelled(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> bool:
        del tenant_id, session_id, run_id
        return False

    async def save_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None:
        self.checkpoint = checkpoint

    async def load_checkpoint(
        self, tenant_id: str, session_id: str, run_id: str
    ) -> RuntimeCheckpoint | None:
        del tenant_id, session_id, run_id
        return self.checkpoint

    async def finish_assignment(self, task_id: str, outcome: str) -> None:
        del task_id
        self.outcome = outcome

    async def suspend_assignment(self, task_id: str, reason: str) -> None:
        del task_id
        self.suspended = reason

    async def suspend_with_checkpoint(
        self, task_id: str, checkpoint: RuntimeCheckpoint, reason: str
    ) -> None:
        del task_id
        self.checkpoint = checkpoint
        self.suspended = reason


class _Session:
    def __init__(self, goal: str) -> None:
        self.events: list[Any] = [
            SimpleNamespace(
                type="session.created",
                payload={"goal": goal, "role": "root"},
                run_id=None,
                occurred_at=datetime.now(UTC),
            )
        ]

    async def load(self, assignment: RuntimeAssignment) -> list[Any]:
        del assignment
        return list(self.events)

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: list[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[Any]:
        del command_id, operation, expected_version
        appended = [
            SimpleNamespace(
                type=event.type,
                payload=dict(event.payload),
                run_id=assignment.run_id,
                occurred_at=datetime.now(UTC),
            )
            for event in events
        ]
        self.events.extend(appended)
        return appended


class _RuntimeEvents:
    async def publish(self, event: object) -> None:
        del event


class _Model:
    def __init__(self, responses: list[ModelResponse]) -> None:
        self._responses = responses
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        response = self._responses[len(self.requests) - 1]
        return response.__class__(
            **{**response.__dict__, "model_call_id": request.model_call_id}
        )


class _CollaborationClient:
    def __init__(self) -> None:
        self.children: list[dict[str, Any]] = []

    async def execute(
        self,
        assignment: RuntimeAssignment,
        *,
        operation: str,
        arguments: dict[str, Any],
        command_id: str,
    ) -> dict[str, Any]:
        del assignment, command_id
        if operation == "get_graph":
            return {"root_session_id": "root", "children": list(self.children)}
        if operation == "create_child":
            child = {
                "session_id": "child-one",
                "status": "runnable",
                "task_key": arguments["spec"]["task_key"],
            }
            self.children.append(child)
            return child
        raise AssertionError(operation)


def _assignment(
    role: str = "root", *, tool_permissions: tuple[str, ...] = ()
) -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-m13",
        root_session_id="root",
        session_id="root" if role == "root" else "child",
        run_id="run-m13",
        runtime_id="runtime-m13",
        lease_id="lease-m13",
        fencing_token=1,
        role=role,
        resource_profile={"tool_permissions": list(tool_permissions)},
    )


def _response(*calls: ToolCall, output: str = "") -> ModelResponse:
    return ModelResponse(
        model_call_id="placeholder",
        provider="test",
        model="test",
        completed_output=output,
        tool_calls=tuple(calls),
    )


def test_root_model_can_create_children_then_suspend_without_completing_run() -> None:
    async def scenario() -> None:
        control = _Control()
        session = _Session("coordinate a complex task")
        client = _CollaborationClient()
        model = _Model(
            [
                _response(
                    ToolCall(
                        tool_invocation_id="create-one",
                        name=CREATE_CHILD,
                        arguments={
                            "spec": {
                                "task_key": "child-one",
                                "role": "worker",
                                "goal": "do work",
                                "output_contract": {
                                    "required_fields": ["summary", "result_ref"]
                                },
                            }
                        },
                    )
                ),
                _response(
                    ToolCall(
                        tool_invocation_id="await-one",
                        name=AWAIT_CHILDREN,
                        arguments={"child_session_ids": ["child-one"]},
                    )
                ),
            ]
        )
        harness = AgentHarness(
            control_store=control,  # type: ignore[arg-type]
            session=session,  # type: ignore[arg-type]
            model=model,
            tools=IdempotentToolClient(),
            runtime_events=_RuntimeEvents(),
            collaboration_controller=RuntimeCollaborationController(client),
        )
        await harness.execute(_assignment())
        assert control.suspended == "waiting_children"
        assert control.outcome is None
        assert control.checkpoint is not None
        assert control.checkpoint.phase == "agent.waiting_children"
        assert control.checkpoint.state["waiting_child_ids"] == ["child-one"]
        assert not any(event.type == "run.completed" for event in session.events)
        assert CREATE_CHILD in {
            tool["function"]["name"] for tool in model.requests[0].tools
        }

    asyncio.run(scenario())


def test_simple_root_completes_without_creating_children() -> None:
    async def scenario() -> None:
        control = _Control()
        session = _Session("answer directly")
        model = _Model([_response(output="direct answer")])
        harness = AgentHarness(
            control_store=control,  # type: ignore[arg-type]
            session=session,  # type: ignore[arg-type]
            model=model,
            tools=IdempotentToolClient(),
            runtime_events=_RuntimeEvents(),
            collaboration_controller=RuntimeCollaborationController(
                _CollaborationClient()
            ),
        )
        await harness.execute(_assignment())
        assert control.outcome == "completed"
        assert control.suspended is None
        assert any(event.type == "run.completed" for event in session.events)

    asyncio.run(scenario())


def test_worker_without_publish_result_fails_closed() -> None:
    async def scenario() -> None:
        control = _Control()
        harness = AgentHarness(
            control_store=control,  # type: ignore[arg-type]
            session=_Session("worker goal"),  # type: ignore[arg-type]
            model=_Model([_response(output="forgot to publish")]),
            tools=IdempotentToolClient(),
            runtime_events=_RuntimeEvents(),
            collaboration_controller=RuntimeCollaborationController(
                _CollaborationClient()
            ),
        )
        with pytest.raises(
            CollaborationValidationError, match="not published"
        ):
            await harness.execute(_assignment("worker"))
        assert control.outcome is None

    asyncio.run(scenario())


def test_worker_model_receives_authoritative_child_goal_and_contract() -> None:
    async def scenario() -> None:
        control = _Control()
        session = _Session("unused root goal")
        session.events = [
            SimpleNamespace(
                type="child.created",
                payload={
                    "goal": "create price deviation test cases",
                    "output_contract": {
                        "required_fields": ["summary", "result_ref"],
                        "require_artifacts": True,
                    },
                    "input_refs": ["skill://price-insight-deviation"],
                    "tool_permissions": ["price.read"],
                },
                run_id="run-m13",
                occurred_at=datetime.now(UTC),
            )
        ]
        model = _Model([_response(output="forgot to publish")])
        harness = AgentHarness(
            control_store=control,  # type: ignore[arg-type]
            session=session,  # type: ignore[arg-type]
            model=model,
            tools=IdempotentToolClient(),
            runtime_events=_RuntimeEvents(),
            collaboration_controller=RuntimeCollaborationController(
                _CollaborationClient()
            ),
        )
        with pytest.raises(CollaborationValidationError, match="not published"):
            await harness.execute(_assignment("worker"))
        user_messages = [
            message["content"]
            for message in model.requests[0].messages
            if message["role"] == "user"
        ]
        assert len(user_messages) == 1
        assert "create price deviation test cases" in user_messages[0]
        assert '"require_artifacts":true' in user_messages[0]
        assert '"tool_permissions":["price.read"]' in user_messages[0]

    asyncio.run(scenario())


def test_reprovisioned_waiting_coordinator_does_not_poll_model_again() -> None:
    async def scenario() -> None:
        control = _Control()
        control.checkpoint = RuntimeCheckpoint(
            tenant_id="tenant-m13",
            session_id="root",
            run_id="run-m13",
            fencing_token=1,
            phase="agent.waiting_children",
            state={
                "turn_index": 5,
                "steps_used": 5,
                "sequence": 20,
                "usage": {},
                "capability_state": {},
                "call_index": 0,
                "call_signatures": {},
                "waiting_child_ids": ["child-one"],
            },
            updated_at=datetime.now(UTC),
        )
        collaboration = _CollaborationClient()
        collaboration.children = [
            {"session_id": "child-one", "status": "running"}
        ]
        model = _Model([_response(output="must not be called")])
        harness = AgentHarness(
            control_store=control,  # type: ignore[arg-type]
            session=_Session("root goal"),  # type: ignore[arg-type]
            model=model,
            tools=IdempotentToolClient(),
            runtime_events=_RuntimeEvents(),
            collaboration_controller=RuntimeCollaborationController(collaboration),
        )

        await harness.execute(_assignment())

        assert model.requests == []
        assert control.suspended == "waiting_children"
        assert control.checkpoint is not None
        assert control.checkpoint.state["waiting_child_ids"] == ["child-one"]

    asyncio.run(scenario())


def test_collaboration_tools_are_role_scoped_and_freeze_owner_operations() -> None:
    controller = RuntimeCollaborationController(_CollaborationClient())

    def names(role: str) -> set[str]:
        return {
            str(tool["function"]["name"])
            for tool in controller.model_tools(_assignment(role))
        }

    coordinator = names("root")
    assert {CREATE_CHILD, AWAIT_CHILDREN, JOIN}.issubset(coordinator)
    assert not any(name.endswith(("delegate", "handoff")) for name in coordinator)
    assert names("worker") == {PUBLISH_RESULT}
    assert names("repair") == {PUBLISH_RESULT}
    assert names("reviewer") == {PUBLISH_REVIEW}

    create_schema = next(
        tool["function"]["parameters"]
        for tool in controller.model_tools(_assignment())
        if tool["function"]["name"] == CREATE_CHILD
    )
    spec_schema = create_schema["properties"]["spec"]
    assert set(spec_schema["required"]) == {
        "task_key",
        "role",
        "goal",
        "output_contract",
    }
    assert "owner" not in spec_schema["properties"]
    assert "tenant_id" not in spec_schema["properties"]
    assert "tool_permissions" not in spec_schema["properties"]


def test_child_tool_permissions_are_bounded_by_root_grant() -> None:
    controller = RuntimeCollaborationController(_CollaborationClient())
    tools = controller.model_tools(
        _assignment(tool_permissions=("price.read", "evidence.read"))
    )
    create_schema = next(
        tool["function"]["parameters"]
        for tool in tools
        if tool["function"]["name"] == CREATE_CHILD
    )
    permission_schema = create_schema["properties"]["spec"]["properties"][
        "tool_permissions"
    ]
    assert permission_schema["items"]["enum"] == ["evidence.read", "price.read"]
    assert permission_schema["maxItems"] == 2


def test_child_permission_escalation_returns_denied_without_calling_client() -> None:
    async def scenario() -> None:
        client = _CollaborationClient()
        controller = RuntimeCollaborationController(client)
        result = await controller.execute(
            _assignment(tool_permissions=("price.read",)),
            ToolCall(
                tool_invocation_id="create-denied",
                name=CREATE_CHILD,
                arguments={
                    "spec": {
                        "task_key": "denied-child",
                        "role": "worker",
                        "goal": "do work",
                        "output_contract": {"required_fields": ["summary"]},
                        "tool_permissions": ["read-only"],
                    }
                },
            ),
        )
        assert result.result == {
            "status": "denied",
            "error_code": "authorization_denied",
            "summary": "Child tool permissions exceed the Root grant",
        }
        assert client.children == []

    asyncio.run(scenario())


def test_unsupported_child_result_contract_is_rejected_before_creation() -> None:
    async def scenario() -> None:
        client = _CollaborationClient()
        controller = RuntimeCollaborationController(client)
        result = await controller.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="create-invalid-contract",
                name=CREATE_CHILD,
                arguments={
                    "spec": {
                        "task_key": "invalid-contract-child",
                        "role": "worker",
                        "goal": "create a document",
                        "output_contract": {
                            "required_fields": ["test_document"]
                        },
                    }
                },
            ),
        )
        assert result.result == {
            "status": "denied",
            "error_code": "collaboration_invalid",
            "summary": "unsupported Child Result fields: test_document",
        }
        assert client.children == []

    asyncio.run(scenario())


def test_child_permission_escalation_is_recoverable_in_agent_loop() -> None:
    async def scenario() -> None:
        control = _Control()
        session = _Session("delegate safely")
        client = _CollaborationClient()
        model = _Model(
            [
                _response(
                    ToolCall(
                        tool_invocation_id="create-denied",
                        name=CREATE_CHILD,
                        arguments={
                            "spec": {
                                "task_key": "denied-child",
                                "role": "worker",
                                "goal": "do work",
                                "output_contract": {"required_fields": ["summary"]},
                                "tool_permissions": ["read-only"],
                            }
                        },
                    )
                ),
                _response(output="The requested Child grant is unavailable."),
            ]
        )
        harness = AgentHarness(
            control_store=control,  # type: ignore[arg-type]
            session=session,  # type: ignore[arg-type]
            model=model,
            tools=IdempotentToolClient(),
            runtime_events=_RuntimeEvents(),
            collaboration_controller=RuntimeCollaborationController(client),
        )
        await harness.execute(_assignment(tool_permissions=("price.read",)))
        assert control.outcome == "completed"
        assert client.children == []
        completed_call = next(
            event for event in session.events if event.type == "tool.call.completed"
        )
        assert completed_call.payload["result"]["error_code"] == "authorization_denied"
        assert any(event.type == "run.completed" for event in session.events)
        assert not any(event.type == "run.failed" for event in session.events)

    asyncio.run(scenario())
