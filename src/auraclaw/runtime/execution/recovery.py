from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint
from auraclaw.domain.runtime_budget import usage as reserved_usage
from auraclaw.domain.skill_execution import RUN_TERMINAL_EVENTS, pending_skill_invocations
from auraclaw.runtime.event_committer import CanonicalEventCommitter
from auraclaw.runtime.execution.serialization import response_to_dict
from auraclaw.runtime.execution_guard import RuntimeExecutionGuard
from auraclaw.runtime.execution_state import RuntimePhase
from auraclaw.runtime.ports import ModelResponse, RuntimeControlClient, SessionClient, ToolCall
from auraclaw.runtime.progress_store import RuntimeProgressStore


class RecoveryCapabilities(Protocol):
    """Recovery can observe receipts and release resources, never dispatch a tool."""

    async def invocation_status(
        self, assignment: RuntimeAssignment, invocation_id: str
    ) -> dict[str, Any]: ...

    async def release_run(self, assignment: RuntimeAssignment) -> None: ...


class RecoveryCollaboration(Protocol):
    async def child_state(
        self, assignment: RuntimeAssignment
    ) -> tuple[tuple[str, ...], tuple[str, ...]]: ...

    async def join_routing_plan(
        self, assignment: RuntimeAssignment, child_session_ids: tuple[str, ...]
    ) -> dict[str, Any]: ...


def _error_payload(error: BaseException) -> str:
    message = getattr(error, "message", None)
    if not isinstance(message, str) or not message.strip():
        message = str(error).strip() or type(error).__name__
    detail = getattr(error, "detail", None)
    if isinstance(detail, str) and detail.strip() and detail.strip() != message:
        return f"{message}: {detail.strip()}"
    return message.strip()


def _error_code(error: BaseException) -> str:
    code = getattr(error, "code", None)
    return str(code) if isinstance(code, str) and code else type(error).__name__


class RuntimeRecoveryService:
    """Recover canonical outcomes, settle pre-dispatch stops and persist waits."""

    def __init__(
        self,
        *,
        control: RuntimeControlClient,
        session: SessionClient,
        guard: RuntimeExecutionGuard,
        events: CanonicalEventCommitter,
        progress: RuntimeProgressStore,
        capabilities: RecoveryCapabilities | None,
        collaboration: RecoveryCollaboration | None,
    ) -> None:
        self._control = control
        self._session = session
        self._guard_service = guard
        self._events = events
        self._progress = progress
        self._capability_controller = capabilities
        self._collaboration_controller = collaboration

    async def recover_pending_outcomes(self, assignment: RuntimeAssignment) -> bool:
        """Read-only reconciliation survives a stopped Run; no model or business call is allowed."""
        await self._guard_service.fence(assignment)
        events = await self._session.load(assignment)
        pending = pending_skill_invocations(events, run_id=assignment.run_id)
        if not pending:
            return False
        if self._capability_controller is None:
            await self._control.suspend_assignment(
                self._progress.task_id(assignment), "waiting_for_tool"
            )
            return True
        # Bound each wake. Remaining receipts stay in Canonical Events for the next wake.
        for requested in pending[:8]:
            invocation_id = str(requested.payload["tool_invocation_id"])
            observed = await self._capability_controller.invocation_status(
                assignment, invocation_id
            )
            known = observed.get("status") == "success" or (
                observed.get("status") in {"error", "denied", "cancelled"}
                and observed.get("side_effect_status") not in {None, "unknown"}
            )
            if not known:
                continue
            await self._events.append_capability_event(
                assignment,
                NewEvent(
                    type="skill.invocation.settled",
                    payload={
                        **requested.payload,
                        "status": observed["status"],
                        "side_effect_status": observed.get("side_effect_status"),
                    },
                ),
                recovery=True,
            )
        events = await self._session.load(assignment)
        if pending_skill_invocations(events, run_id=assignment.run_id):
            await self._control.suspend_assignment(
                self._progress.task_id(assignment), "waiting_for_tool"
            )
            return True
        for activation_id in dict.fromkeys(e.payload["skill_activation_id"] for e in pending):
            await self._events.append_capability_event(
                assignment,
                NewEvent(
                    type="skill.cancelled",
                    payload={
                        "skill_activation_id": activation_id,
                        "error_code": "run_stopped_after_invocation_settled",
                    },
                ),
                recovery=True,
            )
        events = await self._session.load(assignment)
        terminal = next(
            (e for e in events if e.run_id == assignment.run_id and e.type in RUN_TERMINAL_EVENTS),
            None,
        )
        if terminal is None:
            await self._events.append_once(
                assignment,
                events,
                "run.failed",
                {"run_id": assignment.run_id, "error_code": "run_stopped_after_invocation_settled"},
                identity=assignment.run_id,
                recovery=True,
            )
        await self._capability_controller.release_run(assignment)
        await self._control.finish_assignment(
            self._progress.task_id(assignment),
            terminal.type[4:] if terminal is not None else "failed",
        )
        return True

    async def settle_unstarted_batch(
        self, assignment: RuntimeAssignment, calls: tuple[ToolCall, ...], turn_index: int, code: str
    ) -> None:
        # This cursor has not dispatched any of these calls. Settle every requested
        # model tool output so the next Run does not inherit orphan tool messages.
        for call in calls:
            events = await self._session.load(assignment)
            await self._events.append_once(
                assignment,
                events,
                "tool.call.requested",
                {
                    "tool_invocation_id": call.tool_invocation_id,
                    "name": call.name,
                    "arguments": call.arguments,
                    "turn_index": turn_index,
                    "runtime_decision": "not_dispatched",
                    "version": call.version,
                    "expected_side_effect": call.expected_side_effect,
                },
                identity=call.tool_invocation_id,
                recovery=True,
            )
            await self.settle_local_stop(assignment, call, turn_index, code)

    async def settle_local_stop(
        self, assignment: RuntimeAssignment, call: ToolCall, turn_index: int, code: str
    ) -> None:
        """Settle a proven pre-dispatch rejection using the existing invocation identity."""
        events = await self._session.load(assignment)
        await self._events.append_once(
            assignment,
            events,
            "tool.call.completed",
            {
                "tool_invocation_id": call.tool_invocation_id,
                "name": call.name,
                "turn_index": turn_index,
                "result": {
                    "status": "denied",
                    "error_code": code,
                    "side_effect_status": "not_started",
                    "retryable": False,
                    "summary": "Runtime stopped this call before execution.",
                    "metadata": {
                        "decision": "not_dispatched",
                        "policy_version": assignment.budget.policy_version,
                    },
                },
            },
            identity=call.tool_invocation_id,
            recovery=True,
        )

    async def record_failure(self, assignment: RuntimeAssignment, error: Exception) -> bool:
        """Return True when pending-result recovery owns assignment disposition."""
        if await self.recover_pending_outcomes(assignment):
            return True
        events = await self._session.load(assignment)
        if any(
            event.type in {"run.completed", "run.failed", "run.cancelled"}
            and event.run_id == assignment.run_id
            for event in events
        ):
            if self._capability_controller is not None:
                await self._capability_controller.release_run(assignment)
            return False
        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
        )
        if (
            checkpoint is not None
            and checkpoint.phase.startswith("capability.")
            and isinstance(checkpoint.state.get("capability_state"), dict)
        ):
            capability_state = dict(checkpoint.state["capability_state"])
            for item in capability_state.get("active_skills", ()):
                if not isinstance(item, dict):
                    continue
                activation = item.get("activation")
                if not isinstance(activation, dict):
                    continue
                activation_id = str(activation["skill_activation_id"])
                if any(
                    event.type in {"skill.completed", "skill.failed", "skill.cancelled"}
                    and event.payload.get("skill_activation_id") == activation_id
                    for event in events
                ):
                    continue
                binding = dict(activation["binding"])
                await self._session.append(
                    assignment,
                    [
                        NewEvent(
                            type="skill.failed",
                            payload={
                                "skill_activation_id": activation_id,
                                "activation_key": activation["activation_key"],
                                "skill_name": binding["skill_name"],
                                "skill_version": binding["skill_version"],
                                "package_digest": binding["package_digest"],
                                "policy_version": binding["policy_version"],
                                "policy_decision_id": binding.get("policy_decision_id"),
                                "error": _error_payload(error),
                                "error_code": _error_code(error),
                            },
                        )
                    ],
                    command_id=f"runtime:skill.failed:{activation_id}",
                    operation="runtime.skill.failed",
                )
                events = await self._session.load(assignment)
        await self._events.append_once(
            assignment,
            events,
            "run.failed",
            {
                "run_id": assignment.run_id,
                "error": _error_payload(error),
                "error_code": _error_code(error),
                "error_details": self.failure_details(assignment, error, checkpoint, events),
            },
            identity=assignment.run_id,
            visibility=Visibility.USER,
            recovery=True,
        )
        if self._capability_controller is not None:
            await self._capability_controller.release_run(assignment)
        return False

    @staticmethod
    def failure_details(
        assignment: RuntimeAssignment, error: Exception, checkpoint: Any, events: list[Any]
    ) -> dict[str, Any]:
        state = checkpoint.state if checkpoint is not None else {}
        usage = dict(state.get("usage", {}))
        code = _error_code(error)
        category = {
            "runtime_step_budget_exceeded": "steps",
            "runtime_output_token_budget_exceeded": "output_tokens",
            "runtime_cost_budget_exceeded": "cost",
            "runtime_deadline_exceeded": "deadline",
            "runtime_no_progress_detected": "no_progress",
        }.get(code, "execution")
        # References, never raw tool data; authorization remains on the Session query path.
        completed = [
            e for e in events if e.run_id == assignment.run_id and e.type == "tool.call.completed"
        ]
        successful = [
            e.payload["tool_invocation_id"]
            for e in completed
            if e.payload.get("result", {}).get("status") == "success"
        ]
        return {
            "policy_version": assignment.budget.policy_version,
            "category": category,
            "reason_code": code,
            "scope": "run",
            "concluding_reason": state.get("concluding_reason"),
            "partial_summary": (
                f"Run stopped with {len(successful)} successful tool results retained in the "
                "Session. Unfinished work is not marked successful; inspect tool settlements "
                "before retrying any operation."
            ),
            "budget": {
                "max_steps": assignment.budget.max_steps,
                "max_output_tokens": assignment.budget.max_output_tokens,
                "max_cost": assignment.budget.max_cost,
                "deadline": (
                    assignment.deadline.isoformat() if assignment.deadline is not None else None
                ),
            },
            "usage": (
                reserved_usage(events, assignment.run_id)
                if assignment.budget.policy_version == "2"
                else {
                    "steps_used": state.get("steps_used", 0),
                    "output_tokens": usage.get("output_tokens"),
                    "cost": usage.get("cost"),
                }
            ),
            "successful_tool_invocation_ids": successful,
        }

    async def record_approval_wait(
        self,
        assignment: RuntimeAssignment,
        *,
        response: ModelResponse,
        call: ToolCall,
        call_index: int,
        sequence: int,
        state: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        raw_metadata = result.get("metadata", {})
        metadata = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
        raw_request = metadata.get("approval_request", {})
        approval_request = dict(raw_request) if isinstance(raw_request, dict) else {}
        approval_id = str(approval_request.get("approval_id", call.tool_invocation_id))
        events = await self._session.load(assignment)
        await self._events.append_once(
            assignment,
            events,
            "approval.requested",
            approval_request,
            identity=approval_id,
            visibility=Visibility.USER,
        )
        events = await self._session.load(assignment)
        await self._events.append_once(
            assignment,
            events,
            "tool.call.denied",
            {
                "tool_invocation_id": call.tool_invocation_id,
                "name": call.name,
                "error_code": "approval_required",
                "approval_id": approval_id,
            },
            identity=call.tool_invocation_id,
        )
        waiting = {
            **state,
            "response": response_to_dict(response),
            "call_index": call_index,
            "tool_invocation_id": call.tool_invocation_id,
            "approval_id": approval_id,
            "sequence": sequence,
        }
        waiting.pop("result", None)
        waiting.pop("side_events", None)
        await self._progress.save_checkpoint(
            assignment, RuntimePhase.CAPABILITY_APPROVAL_WAITING, waiting
        )

    async def load_checkpoint(self, assignment: RuntimeAssignment) -> RuntimeCheckpoint | None:
        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
        )
        if assignment.budget.policy_version == "2":
            facts = await self._session.load(assignment)
            receipt = next(
                (
                    e
                    for e in reversed(facts)
                    if e.run_id == assignment.run_id and e.type == "runtime.progress.recorded"
                ),
                None,
            )
            if receipt is not None:
                checkpoint = RuntimeCheckpoint(
                    tenant_id=assignment.tenant_id,
                    session_id=assignment.session_id,
                    run_id=assignment.run_id,
                    fencing_token=assignment.fencing_token,
                    phase=str(receipt.payload["phase"]),
                    state=dict(receipt.payload["state"]),
                    updated_at=datetime.now(UTC),
                )
        return checkpoint

    async def resume_wait(
        self,
        assignment: RuntimeAssignment,
        checkpoint: RuntimeCheckpoint | None,
        state: dict[str, Any],
    ) -> bool:
        if checkpoint is not None and checkpoint.phase == "capability.approval_waiting":
            approval_id = str(state.get("approval_id", ""))
            session_events = await self._session.load(assignment)
            approved = bool(approval_id) and any(
                event.type == "approval.approved"
                and event.payload.get("approval_id") == approval_id
                for event in session_events
            )
            if not approved:
                await self._control.finish_assignment(
                    self._progress.task_id(assignment), "waiting_for_human"
                )
                return True
        if (
            checkpoint is not None
            and checkpoint.phase
            in {
                "agent.waiting_children",
                "collaboration.waiting_children",
            }
            and self._collaboration_controller is not None
        ):
            waiting = tuple(str(item) for item in state.get("waiting_child_ids", ()) if item)
            _, active_children = await self._collaboration_controller.child_state(assignment)
            still_waiting = tuple(
                child_id for child_id in waiting if child_id in set(active_children)
            )
            if still_waiting:
                await self._progress.suspend_for_children(
                    assignment,
                    state,
                    still_waiting,
                )
                return True
            router_submission = state.get("router_plan_submission")
            if isinstance(router_submission, dict):
                routed_children = tuple(
                    str(item) for item in router_submission.get("child_session_ids", ()) if item
                )
                if routed_children and set(routed_children) == set(waiting):
                    join_result = await self._collaboration_controller.join_routing_plan(
                        assignment,
                        routed_children,
                    )
                    state["router_plan_join"] = dict(join_result)
                    state["collaboration_terminal"] = True
                    state["waiting_child_ids"] = []
                    await self._progress.save_checkpoint(
                        assignment, RuntimePhase.AGENT_COMPLETED, state
                    )
                    await self._control.finish_assignment(
                        self._progress.task_id(assignment), "completed"
                    )
                    return True
        return False
