from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from auraclaw.contracts.collaboration import (
    ChildResult,
    ChildSpec,
    CollaborationRole,
    OutputContract,
    ReviewDecision,
    ReviewResult,
)
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import AuthorizationError, CollaborationValidationError
from auraclaw.contracts.events import Actor
from auraclaw.contracts.internal import (
    CollaborationCommandRequest,
    CollaborationCommandResponse,
    ServiceIdentity,
)
from auraclaw.internal.security import LeaseAssertionVerifier
from auraclaw.session.collaboration_service import CollaborationService
from auraclaw.session.internal_service import outbox_wake_destinations
from auraclaw.session.ports import EventStore

OutboxWakeHook = Callable[[Sequence[str]], None]


class CollaborationInternalService:
    """Lease-fenced Runtime command boundary for CollaborationService."""

    def __init__(
        self,
        collaboration: CollaborationService,
        events: EventStore,
        *,
        lease_verifier: LeaseAssertionVerifier,
        outbox_wake: OutboxWakeHook | None = None,
    ) -> None:
        self._collaboration = collaboration
        self._events = events
        self._lease_verifier = lease_verifier
        self._outbox_wake = outbox_wake

    async def command(
        self, request: CollaborationCommandRequest
    ) -> CollaborationCommandResponse:
        if request.context.service_identity is not ServiceIdentity.AGENT_RUNTIME:
            raise AuthorizationError("collaboration commands are restricted to agent-runtime")
        assertion = request.lease_assertion
        if request.context.tenant_id != assertion.tenant_id:
            raise AuthorizationError("request and lease tenant mismatch")
        if request.root_session_id != assertion.root_session_id:
            raise AuthorizationError("request and lease Root Session mismatch")
        if request.session_id != assertion.session_id:
            raise AuthorizationError("request and lease Session mismatch")
        await self._lease_verifier.verify(
            assertion,
            tenant_id=request.context.tenant_id,
            session_id=request.session_id,
            run_id=assertion.run_id,
        )
        actor_type = self._actor_type(request.operation, assertion.role)
        if (
            actor_type == "coordinator"
            and request.operation != "get_graph"
            and request.session_id != request.root_session_id
        ):
            raise AuthorizationError("v1 Coordinator writes require the Root Session lease")
        actor_id = assertion.runtime_id
        if actor_id is None:
            raise AuthorizationError("Runtime lease has no runtime identity")
        result = await self._execute(request, actor_type=actor_type, actor_id=actor_id)
        if self._outbox_wake is not None and request.operation != "get_graph":
            self._outbox_wake(tuple(self._wake_destinations(request.operation)))
        return CollaborationCommandResponse(result=result)

    async def _execute(
        self,
        request: CollaborationCommandRequest,
        *,
        actor_type: str,
        actor_id: str,
    ) -> dict[str, Any]:
        arguments = request.arguments
        if request.operation == "get_graph":
            graph = await self._collaboration.graph(
                request.context.tenant_id, request.root_session_id
            )
            return {
                "root_session_id": graph.root_session_id,
                "children": [
                    {
                        "session_id": node.session_id,
                        "parent_session_id": node.parent_session_id,
                        "task_key": node.task_key,
                        "role": node.role.value,
                        "goal": node.goal,
                        "dependency_ids": list(node.dependency_ids),
                        "status": node.status,
                        "result": node.result,
                        "target_session_id": node.target_session_id,
                        "run_id": node.run_id,
                    }
                    for node in graph.nodes.values()
                    if node.session_id != graph.root_session_id
                ],
            }

        target_session_id = self._target_session_id(request)
        expected_version = 0
        if target_session_id is not None:
            expected_version = len(
                await self._events.load(
                    request.context.tenant_id, target_session_id
                )
            )
        context = CommandContext(
            command_id=request.command_id,
            tenant_id=request.context.tenant_id,
            actor=Actor(type=actor_type, id=actor_id),
            correlation_id=request.context.correlation_id,
            causation_id=request.context.causation_id,
            expected_version=expected_version,
            operation=f"collaboration.{request.operation}",
        )
        if request.operation == "create_child":
            spec_value = self._dict(arguments, "spec")
            return await self._collaboration.create_child(
                root_session_id=request.root_session_id,
                parent_session_id=str(
                    arguments.get("parent_session_id", request.root_session_id)
                ),
                spec=ChildSpec(
                    task_key=self._required_str(spec_value, "task_key"),
                    role=CollaborationRole(self._required_str(spec_value, "role")),
                    goal=self._required_str(spec_value, "goal"),
                    output_contract=OutputContract.from_dict(
                        self._dict(spec_value, "output_contract")
                    ),
                    dependency_ids=self._strings(spec_value, "dependency_ids"),
                    input_refs=self._strings(spec_value, "input_refs"),
                    tool_permissions=self._strings(spec_value, "tool_permissions"),
                    budget=float(spec_value.get("budget", 1.0)),
                    runtime_budget=dict(spec_value.get("runtime_budget", {})),
                    metadata=dict(spec_value.get("metadata", {})),
                ),
                context=context,
            )
        if request.operation == "set_dependencies":
            return await self._collaboration.set_dependencies(
                root_session_id=request.root_session_id,
                child_session_id=self._required_str(arguments, "child_session_id"),
                dependency_ids=self._strings(arguments, "dependency_ids"),
                context=context,
            )
        if request.operation == "request_review":
            return await self._collaboration.request_review(
                root_session_id=request.root_session_id,
                parent_session_id=str(
                    arguments.get("parent_session_id", request.root_session_id)
                ),
                task_key=self._required_str(arguments, "task_key"),
                target_session_id=self._required_str(arguments, "target_session_id"),
                goal=self._required_str(arguments, "goal"),
                budget=float(arguments.get("budget", 1.0)),
                runtime_budget=dict(arguments.get("runtime_budget", {})),
                context=context,
            )
        if request.operation == "cancel_child":
            return await self._collaboration.cancel_child(
                root_session_id=request.root_session_id,
                child_session_id=self._required_str(arguments, "child_session_id"),
                reason=str(arguments.get("reason", "cancelled by Coordinator")),
                context=context,
            )
        if request.operation == "join":
            return await self._collaboration.join(
                root_session_id=request.root_session_id,
                child_session_ids=self._strings(arguments, "child_session_ids"),
                result_summary=self._required_str(arguments, "result_summary"),
                result_ref=str(
                    arguments.get(
                        "result_ref",
                        f"result://{request.root_session_id}/{request.lease_assertion.run_id}",
                    )
                ),
                context=context,
            )
        if request.operation == "publish_result":
            return await self._collaboration.publish_child_result(
                root_session_id=request.root_session_id,
                child_session_id=request.session_id,
                child_result=ChildResult(
                    summary=self._required_str(arguments, "summary"),
                    result_ref=self._required_str(arguments, "result_ref"),
                    artifact_refs=self._strings(arguments, "artifact_refs"),
                    evidence_refs=self._strings(arguments, "evidence_refs"),
                    limitations=self._strings(arguments, "limitations"),
                ),
                context=context,
            )
        if request.operation == "publish_review":
            return await self._collaboration.publish_review(
                root_session_id=request.root_session_id,
                review_session_id=request.session_id,
                review=ReviewResult(
                    decision=ReviewDecision(
                        self._required_str(arguments, "decision")
                    ),
                    evidence_refs=self._strings(arguments, "evidence_refs"),
                    findings=self._strings(arguments, "findings"),
                    repair_suggestions=self._strings(
                        arguments, "repair_suggestions"
                    ),
                ),
                context=context,
            )
        raise CollaborationValidationError(
            f"unsupported collaboration operation: {request.operation}"
        )

    @staticmethod
    def _actor_type(operation: str, role: str) -> str:
        if operation in {
            "get_graph",
            "create_child",
            "set_dependencies",
            "request_review",
            "cancel_child",
            "join",
        }:
            if role not in {"root", "coordinator"}:
                raise AuthorizationError("assignment role cannot change the Task DAG")
            return "coordinator"
        if operation == "publish_result":
            if role not in {"worker", "repair"}:
                raise AuthorizationError("assignment role cannot publish a Child Result")
            return "worker"
        if operation == "publish_review":
            if role != "reviewer":
                raise AuthorizationError("assignment role cannot publish a review")
            return "reviewer"
        raise AuthorizationError("unsupported collaboration operation")

    @staticmethod
    def _target_session_id(request: CollaborationCommandRequest) -> str | None:
        if request.operation in {"set_dependencies", "cancel_child"}:
            return str(request.arguments.get("child_session_id", "")) or None
        if request.operation in {"join"}:
            return request.root_session_id
        if request.operation in {"publish_result", "publish_review"}:
            return request.session_id
        return None

    @staticmethod
    def _wake_destinations(operation: str) -> frozenset[str]:
        event_types = {
            "create_child": ("child.created", "run.requested"),
            "set_dependencies": ("dependency.changed",),
            "request_review": ("child.created", "run.requested"),
            "cancel_child": ("run.cancelled",),
            "join": ("join.completed", "run.completed"),
            "publish_result": ("child.result_published", "run.completed"),
            "publish_review": ("review.completed", "run.completed"),
        }[operation]
        destinations: set[str] = set()
        for event_type in event_types:
            destinations.update(outbox_wake_destinations((event_type,)))
        return frozenset(destinations)

    @staticmethod
    def _dict(value: dict[str, Any], key: str) -> dict[str, Any]:
        selected = value.get(key)
        if not isinstance(selected, dict):
            raise CollaborationValidationError(f"{key} must be an object")
        return dict(selected)

    @staticmethod
    def _required_str(value: dict[str, Any], key: str) -> str:
        selected = str(value.get(key, "")).strip()
        if not selected:
            raise CollaborationValidationError(f"{key} is required")
        return selected

    @staticmethod
    def _strings(value: dict[str, Any], key: str) -> tuple[str, ...]:
        selected = value.get(key, ())
        if not isinstance(selected, (list, tuple)):
            raise CollaborationValidationError(f"{key} must be an array")
        return tuple(str(item) for item in selected)
