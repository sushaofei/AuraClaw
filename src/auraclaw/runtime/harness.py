from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from auraclaw.contracts.errors import (
    BudgetExceededError,
    CollaborationValidationError,
    ModelProviderError,
    RuntimeCancelledError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.clients import assignment_resource_id
from auraclaw.runtime.collaboration_controller import RuntimeCollaborationController
from auraclaw.runtime.model_stream import iter_model_stream
from auraclaw.runtime.ports import (
    ModelClient,
    ModelPolicy,
    ModelRequest,
    ModelResponse,
    RuntimeControlClient,
    RuntimeEvent,
    RuntimeEventPublisher,
    SessionClient,
    ToolCall,
    ToolClient,
)

logger = logging.getLogger(__name__)


def _error_payload(error: BaseException) -> str:
    message = getattr(error, "message", None)
    if not isinstance(message, str) or not message.strip():
        message = str(error).strip() or type(error).__name__
    detail = getattr(error, "detail", None)
    if isinstance(detail, str) and detail.strip() and detail.strip() != message:
        return f"{message}: {detail.strip()}"
    return message.strip()


class InjectionPoint(StrEnum):
    BEFORE_MODEL = "before_model"
    AFTER_MODEL = "after_model"
    BEFORE_TOOL = "before_tool"
    AFTER_TOOL = "after_tool"


FailureInjector = Callable[[InjectionPoint], Awaitable[None] | None]


class AgentHarness:
    """Recoverable Agent harness with optional governed multi-turn capabilities."""

    _APPROVAL_TERMINAL_TYPES = frozenset(
        {
            "approval.approved",
            "approval.rejected",
            "approval.expired",
            "approval.cancelled",
            "human.response.recorded",
        }
    )

    def __init__(
        self,
        *,
        control_store: RuntimeControlClient,
        session: SessionClient,
        model: ModelClient,
        tools: ToolClient,
        runtime_events: RuntimeEventPublisher,
        model_policy: ModelPolicy | None = None,
        capability_controller: RuntimeCapabilityController | None = None,
        collaboration_controller: RuntimeCollaborationController | None = None,
        failure_injector: FailureInjector | None = None,
    ) -> None:
        self._control = control_store
        self._session = session
        self._model = model
        self._tools = tools
        self._runtime_events = runtime_events
        self._policy = model_policy or ModelPolicy()
        self._capability_controller = capability_controller
        self._collaboration_controller = collaboration_controller
        self._failure_injector = failure_injector

    async def execute(self, assignment: RuntimeAssignment) -> None:
        execute_started = time.perf_counter()
        await self._guard(assignment)
        if assignment.budget.max_steps < 1:
            raise BudgetExceededError("Runtime step budget is exhausted")
        events = await self._session.load(assignment)
        await self._append_once(
            assignment,
            events,
            "run.started",
            {"run_id": assignment.run_id, "runtime_id": assignment.runtime_id},
            identity=assignment.run_id,
        )
        logger.info(
            "ttft.run_started session=%s run=%s since_execute_ms=%.2f",
            assignment.session_id,
            assignment.run_id,
            (time.perf_counter() - execute_started) * 1_000,
        )
        if any(
            event.type == "run.completed" and event.run_id == assignment.run_id for event in events
        ):
            await self._control.finish_assignment(self._task_id(assignment), "completed")
            return
        if self._capability_controller is not None or self._collaboration_controller is not None:
            await self._execute_capability_loop(
                assignment, events=events, execute_started=execute_started
            )
            return

        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id, assignment.session_id, assignment.run_id
        )
        model_call_id = f"mdl_{assignment.run_id}_step_1"
        if checkpoint is None or checkpoint.phase == "model_pending":
            checkpoint_ready = asyncio.create_task(
                self._save_checkpoint(
                    assignment,
                    "model_pending",
                    {"model_call_id": model_call_id, "sequence": 0},
                )
            )
            await self._inject(InjectionPoint.BEFORE_MODEL)
            await self._guard(assignment)
            messages = self._build_messages(events)
            if not messages:
                checkpoint_ready.cancel()
                with suppress(asyncio.CancelledError):
                    await checkpoint_ready
                raise ModelProviderError(
                    "model request has no user/assistant messages "
                    f"(session={assignment.session_id} run={assignment.run_id})"
                )
            request = ModelRequest(
                model_call_id=model_call_id,
                tenant_id=assignment.tenant_id,
                run_id=assignment.run_id,
                messages=messages,
                policy=self._policy,
                max_output_tokens=assignment.budget.max_output_tokens,
            )
            await self._record_model_input(assignment, events, request=request, turn_index=0)
            response, sequence = await self._generate_with_live_deltas(
                assignment,
                request,
                sequence=0,
                execute_started=execute_started,
                prep=checkpoint_ready,
            )
            await self._validate_usage(assignment, response)
            await self._save_checkpoint(
                assignment,
                "model_completed",
                {
                    "response": self._response_to_dict(response),
                    "sequence": sequence,
                    "deltas_published": True,
                },
            )
            await self._inject(InjectionPoint.AFTER_MODEL)
        elif checkpoint.phase == "model_completed":
            response = self._response_from_dict(dict(checkpoint.state["response"]))
            sequence = int(checkpoint.state.get("sequence", 0))
            if not checkpoint.state.get("deltas_published"):
                for delta in response.deltas:
                    sequence += 1
                    await self._publish_delta(assignment, sequence, delta)
        else:
            response_data = checkpoint.state.get("response")
            if not isinstance(response_data, dict):
                raise RuntimeError(f"invalid Runtime checkpoint phase: {checkpoint.phase}")
            response = self._response_from_dict(response_data)
            sequence = int(checkpoint.state.get("sequence", 0))
            if not checkpoint.state.get("deltas_published"):
                for delta in response.deltas:
                    sequence += 1
                    await self._publish_delta(assignment, sequence, delta)

        events = await self._session.load(assignment)
        await self._append_once(
            assignment,
            events,
            "model.output.completed",
            {
                "model_call_id": response.model_call_id,
                "provider": response.provider,
                "model": response.model,
                "output": response.completed_output,
                "finish_reason": response.finish_reason,
                "usage": response.usage,
            },
            identity=response.model_call_id,
            visibility=Visibility.USER,
        )
        await self._save_checkpoint(
            assignment,
            "model_recorded",
            {"response": self._response_to_dict(response), "sequence": sequence},
        )

        for index, call in enumerate(response.tool_calls):
            can_continue = await self._run_tool(assignment, response, call, index, sequence)
            if not can_continue:
                await self._control.finish_assignment(
                    self._task_id(assignment), "waiting_for_human"
                )
                return

        events = await self._session.load(assignment)
        await self._append_once(
            assignment,
            events,
            "run.completed",
            {
                "run_id": assignment.run_id,
                "result_summary": response.completed_output,
            },
            identity=assignment.run_id,
            visibility=Visibility.USER,
        )
        await self._save_checkpoint(
            assignment,
            "completed",
            {"response": self._response_to_dict(response), "sequence": sequence},
        )
        await self._control.finish_assignment(self._task_id(assignment), "completed")

    async def _execute_capability_loop(
        self,
        assignment: RuntimeAssignment,
        *,
        events: list[Any] | None = None,
        execute_started: float | None = None,
    ) -> None:
        assert self._capability_controller is not None or self._collaboration_controller is not None
        if execute_started is None:
            execute_started = time.perf_counter()
        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
        )
        state: dict[str, Any] = (
            dict(checkpoint.state)
            if checkpoint is not None and checkpoint.phase.startswith(("capability.", "agent."))
            else {
                "turn_index": 0,
                "steps_used": 0,
                "sequence": 0,
                "usage": {},
                "capability_state": (
                    self._capability_controller.empty_state()
                    if self._capability_controller is not None
                    else {}
                ),
                "call_index": 0,
                "call_signatures": {},
            }
        )
        capability_state = dict(state.get("capability_state", {}))
        if (
            self._capability_controller is not None
            and not capability_state.get("required_capabilities_preloaded")
        ):
            state["capability_state"] = await self._capability_controller.preload_required(
                assignment,
                capability_state,
            )
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
                    self._task_id(assignment), "waiting_for_human"
                )
                return
        turn_events = events
        while int(state.get("steps_used", 0)) < assignment.budget.max_steps:
            await self._guard(assignment)
            turn_index = int(state.get("turn_index", 0))
            model_call_id = f"mdl_{assignment.run_id}_turn_{turn_index + 1}"
            resume_phase = checkpoint.phase if checkpoint is not None else ""
            if resume_phase == "capability.approval_waiting":
                resume_phase = "capability.model_completed"
            if (
                resume_phase
                in {
                    "capability.model_completed",
                    "capability.call_completed",
                }
                and int(state.get("turn_index", -1)) == turn_index
                and isinstance(state.get("response"), dict)
            ):
                response = self._response_from_dict(dict(state["response"]))
            else:
                if turn_events is None:
                    turn_events = await self._session.load(assignment)
                capability_state = dict(state.get("capability_state", {}))
                trusted: tuple[dict[str, Any], ...] = ()
                if self._capability_controller is not None:
                    if await self._apply_skill_binding_disposition(
                        assignment,
                        state,
                        capability_state,
                    ):
                        return
                    trusted += await self._capability_controller.trusted_messages(
                        assignment, capability_state
                    )
                if self._collaboration_controller is not None:
                    trusted += await self._collaboration_controller.trusted_messages(assignment)
                model_tools: tuple[dict[str, Any], ...] = ()
                if self._capability_controller is not None:
                    model_tools += self._capability_controller.model_tools(capability_state)
                if self._collaboration_controller is not None:
                    model_tools += self._collaboration_controller.model_tools(assignment)
                output_tokens_used = int(dict(state.get("usage", {})).get("output_tokens", 0))
                remaining_output_tokens = assignment.budget.max_output_tokens - output_tokens_used
                if remaining_output_tokens < 1:
                    raise BudgetExceededError(
                        "Runtime cumulative output token budget was exhausted"
                    )
                checkpoint_ready = asyncio.create_task(
                    self._save_checkpoint(
                        assignment,
                        "capability.model_pending",
                        {
                            **state,
                            "model_call_id": model_call_id,
                            "call_index": 0,
                        },
                    )
                )
                await self._inject(InjectionPoint.BEFORE_MODEL)
                await self._guard(assignment)
                request = ModelRequest(
                    model_call_id=model_call_id,
                    tenant_id=assignment.tenant_id,
                    run_id=assignment.run_id,
                    messages=(
                        *trusted,
                        *self._build_capability_messages(turn_events),
                    ),
                    tools=model_tools,
                    policy=self._policy,
                    max_output_tokens=remaining_output_tokens,
                )
                await self._record_model_input(
                    assignment,
                    turn_events,
                    request=request,
                    turn_index=turn_index,
                )
                response, sequence = await self._generate_with_live_deltas(
                    assignment,
                    request,
                    sequence=int(state.get("sequence", 0)),
                    publish_deltas=True,
                    execute_started=execute_started,
                    prep=checkpoint_ready,
                )
                turn_events = None
                usage = self._accumulate_usage(dict(state.get("usage", {})), response.usage)
                self._validate_cumulative_usage(assignment, usage)
                state = {
                    **state,
                    "response": self._response_to_dict(response),
                    "usage": usage,
                    "steps_used": int(state.get("steps_used", 0)) + 1,
                    "call_index": 0,
                    "sequence": sequence,
                    "deltas_published": True,
                }
                await self._save_checkpoint(assignment, "capability.model_completed", state)
                checkpoint = await self._control.load_checkpoint(
                    assignment.tenant_id,
                    assignment.session_id,
                    assignment.run_id,
                )
                await self._inject(InjectionPoint.AFTER_MODEL)

            sequence = int(state.get("sequence", 0))
            if not response.tool_calls and not state.get("deltas_published"):
                for delta in response.deltas:
                    sequence += 1
                    await self._publish_delta(assignment, sequence, delta)
            state["sequence"] = sequence
            events = await self._session.load(assignment)
            await self._append_once(
                assignment,
                events,
                "model.turn.completed",
                {
                    "model_call_id": response.model_call_id,
                    "turn_index": turn_index,
                    "output": response.completed_output,
                    "finish_reason": response.finish_reason,
                    "usage": response.usage,
                    "tool_calls": [self._tool_call_to_dict(call) for call in response.tool_calls],
                },
                identity=response.model_call_id,
            )

            call_index = int(state.get("call_index", 0))
            while call_index < len(response.tool_calls):
                call = response.tool_calls[call_index]
                pending_approval_id = state.get("approval_id")
                if (
                    pending_approval_id
                    and call.tool_invocation_id == state.get("tool_invocation_id")
                    and call.approval_id is None
                ):
                    call = replace(call, approval_id=str(pending_approval_id))
                events = await self._session.load(assignment)
                await self._append_once(
                    assignment,
                    events,
                    "tool.call.requested",
                    {
                        "tool_invocation_id": call.tool_invocation_id,
                        "name": call.name,
                        "arguments": call.arguments,
                        "turn_index": turn_index,
                        "version": call.version,
                        "expected_side_effect": call.expected_side_effect,
                        "activity": self._tool_activity_metadata(
                            dict(state.get("capability_state", {})), call
                        ),
                    },
                    identity=call.tool_invocation_id,
                )
                is_completed_resume = (
                    checkpoint is not None
                    and checkpoint.phase == "capability.call_completed"
                    and checkpoint.state.get("tool_invocation_id") == call.tool_invocation_id
                )
                if is_completed_resume:
                    assert checkpoint is not None
                    result = dict(checkpoint.state["result"])
                    capability_state = dict(checkpoint.state.get("capability_state", {}))
                    side_events = tuple(
                        NewEvent(
                            type=str(item["type"]),
                            payload=dict(item["payload"]),
                            visibility=Visibility(str(item.get("visibility", "internal"))),
                        )
                        for item in checkpoint.state.get("side_events", ())
                    )
                    terminal = bool(checkpoint.state.get("collaboration_terminal"))
                    waiting_child_ids = tuple(
                        str(item) for item in checkpoint.state.get("waiting_child_ids", ())
                    )
                else:
                    if int(state.get("steps_used", 0)) >= assignment.budget.max_steps:
                        raise BudgetExceededError("Runtime capability step budget was exhausted")
                    signature = hashlib.sha256(
                        json.dumps(
                            {"name": call.name, "arguments": call.arguments},
                            sort_keys=True,
                            separators=(",", ":"),
                            default=str,
                        ).encode()
                    ).hexdigest()
                    signatures = dict(state.get("call_signatures", {}))
                    repeated = int(signatures.get(signature, 0)) + 1
                    if repeated > 3:
                        raise BudgetExceededError(
                            "Runtime detected a repeated no-progress capability call"
                        )
                    signatures[signature] = repeated
                    state["call_signatures"] = signatures
                    await self._inject(InjectionPoint.BEFORE_TOOL)
                    await self._guard(assignment)
                    terminal = False
                    waiting_child_ids = ()
                    if (
                        self._collaboration_controller is not None
                        and self._collaboration_controller.owns(call.name)
                    ):
                        if (
                            self._collaboration_controller.is_terminal(call.name)
                            and call_index != len(response.tool_calls) - 1
                        ):
                            raise CollaborationValidationError(
                                "terminal collaboration tool must be the final tool call"
                            )
                        collaboration_execution = await self._collaboration_controller.execute(
                            assignment, call
                        )
                        result = collaboration_execution.result
                        capability_state = dict(state.get("capability_state", {}))
                        side_events = ()
                        terminal = collaboration_execution.terminal
                        waiting_child_ids = collaboration_execution.waiting_child_ids
                    else:
                        if self._capability_controller is None:
                            raise CollaborationValidationError(
                                f"unsupported Runtime tool: {call.name}"
                            )
                        execution = await self._capability_controller.execute(
                            assignment,
                            call,
                            dict(state.get("capability_state", {})),
                        )
                        result = execution.result
                        capability_state = execution.state
                        side_events = execution.events
                    state = {
                        **state,
                        "capability_state": capability_state,
                        "result": result,
                        "side_events": [
                            {
                                "type": event.type,
                                "payload": event.payload,
                                "visibility": event.visibility.value,
                            }
                            for event in side_events
                        ],
                        "tool_invocation_id": call.tool_invocation_id,
                        "call_index": call_index,
                        "collaboration_terminal": terminal,
                        "waiting_child_ids": list(waiting_child_ids),
                        "steps_used": int(state.get("steps_used", 0)) + 1,
                    }
                    if int(state["steps_used"]) > assignment.budget.max_steps:
                        raise BudgetExceededError("Runtime capability step budget was exhausted")
                    await self._save_checkpoint(assignment, "capability.call_completed", state)
                    checkpoint = await self._control.load_checkpoint(
                        assignment.tenant_id,
                        assignment.session_id,
                        assignment.run_id,
                    )
                    await self._inject(InjectionPoint.AFTER_TOOL)

                if result.get("error_code") == "approval_required":
                    await self._record_approval_wait(
                        assignment,
                        response=response,
                        call=call,
                        call_index=call_index,
                        sequence=sequence,
                        state=state,
                        result=result,
                    )
                    await self._control.finish_assignment(
                        self._task_id(assignment), "waiting_for_human"
                    )
                    return

                for side_event in side_events:
                    await self._append_capability_event(assignment, side_event)
                events = await self._session.load(assignment)
                await self._append_once(
                    assignment,
                    events,
                    "tool.call.completed",
                    {
                        "tool_invocation_id": call.tool_invocation_id,
                        "name": call.name,
                        "result": result,
                        "turn_index": turn_index,
                    },
                    identity=call.tool_invocation_id,
                )
                if waiting_child_ids:
                    waiting_state = {
                        **state,
                        "turn_index": turn_index + 1,
                        "call_index": call_index + 1,
                        "waiting_child_ids": list(waiting_child_ids),
                    }
                    waiting_state.pop("response", None)
                    waiting_state.pop("result", None)
                    await self._save_checkpoint(assignment, "agent.waiting_children", waiting_state)
                    await self._control.suspend_assignment(
                        self._task_id(assignment), "waiting_children"
                    )
                    return
                if terminal:
                    await self._save_checkpoint(assignment, "agent.completed", state)
                    await self._control.finish_assignment(self._task_id(assignment), "completed")
                    return
                call_index += 1
                state = {
                    **state,
                    "capability_state": capability_state,
                    "call_index": call_index,
                    "side_events": [],
                }
                await self._save_checkpoint(assignment, "capability.model_completed", state)
                checkpoint = await self._control.load_checkpoint(
                    assignment.tenant_id,
                    assignment.session_id,
                    assignment.run_id,
                )

            if response.tool_calls:
                state = {
                    **state,
                    "turn_index": turn_index + 1,
                    "call_index": 0,
                }
                state.pop("response", None)
                state.pop("result", None)
                state.pop("tool_invocation_id", None)
                await self._save_checkpoint(assignment, "capability.model_pending", state)
                checkpoint = await self._control.load_checkpoint(
                    assignment.tenant_id,
                    assignment.session_id,
                    assignment.run_id,
                )
                continue

            events = await self._session.load(assignment)
            if self._collaboration_controller is not None:
                all_children, active_children = await self._collaboration_controller.child_state(
                    assignment
                )
                if active_children:
                    waiting_state = {
                        **state,
                        "turn_index": turn_index + 1,
                        "call_index": 0,
                        "waiting_child_ids": list(active_children),
                    }
                    waiting_state.pop("response", None)
                    waiting_state.pop("result", None)
                    await self._save_checkpoint(assignment, "agent.waiting_children", waiting_state)
                    await self._control.suspend_assignment(
                        self._task_id(assignment), "waiting_children"
                    )
                    return
                if all_children and assignment.role in {"root", "coordinator"}:
                    raise CollaborationValidationError("Coordinator graph was not joined")
                if assignment.role in {"worker", "repair", "reviewer"}:
                    raise CollaborationValidationError("role output contract was not published")
            await self._append_once(
                assignment,
                events,
                "model.output.completed",
                {
                    "model_call_id": response.model_call_id,
                    "provider": response.provider,
                    "model": response.model,
                    "output": response.completed_output,
                    "finish_reason": response.finish_reason,
                    "usage": dict(state.get("usage", {})),
                },
                identity=response.model_call_id,
                visibility=Visibility.USER,
            )
            if self._capability_controller is not None:
                for event in self._capability_controller.terminal_events(
                    dict(state.get("capability_state", {})),
                    response.completed_output,
                ):
                    await self._append_capability_event(assignment, event)
            events = await self._session.load(assignment)
            await self._append_once(
                assignment,
                events,
                "run.completed",
                {
                    "run_id": assignment.run_id,
                    "result_summary": response.completed_output,
                },
                identity=assignment.run_id,
                visibility=Visibility.USER,
            )
            await self._save_checkpoint(assignment, "capability.completed", state)
            await self._control.finish_assignment(self._task_id(assignment), "completed")
            return
        raise BudgetExceededError("Runtime capability step budget was exhausted")

    async def _apply_skill_binding_disposition(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        capability_state: dict[str, Any],
    ) -> bool:
        assert self._capability_controller is not None
        disposition = await self._capability_controller.binding_disposition(
            assignment, capability_state
        )
        if disposition is None or disposition.get("action") == "continue":
            return False
        action = str(disposition["action"])
        activation_id = str(disposition.get("skill_activation_id") or "unknown")
        evidence = {
            "skill_activation_id": activation_id,
            "action": action,
            "reason_code": disposition.get("reason_code"),
            "policy_version": disposition.get("policy_version"),
            "policy_decision_id": disposition.get("policy_decision_id"),
            "binding": disposition.get("binding"),
        }
        events = await self._session.load(assignment)
        await self._append_once(
            assignment,
            events,
            "skill.revocation.applied",
            evidence,
            identity=f"{activation_id}:{action}",
        )
        state = {**state, "skill_revocation": evidence}
        if action == "pause":
            await self._save_checkpoint(assignment, "capability.skill_revoked_paused", state)
            await self._control.suspend_assignment(self._task_id(assignment), "skill_revoked")
            return True
        events = await self._session.load(assignment)
        await self._append_once(
            assignment,
            events,
            "skill.cancelled",
            evidence,
            identity=activation_id,
        )
        events = await self._session.load(assignment)
        await self._append_once(
            assignment,
            events,
            "run.cancelled",
            {
                "run_id": assignment.run_id,
                "reason": "skill_revoked",
                "skill_activation_id": activation_id,
            },
            identity=assignment.run_id,
            visibility=Visibility.USER,
        )
        await self._save_checkpoint(assignment, "capability.skill_revoked_cancelled", state)
        await self._control.finish_assignment(self._task_id(assignment), "cancelled")
        return True

    async def record_failure(self, assignment: RuntimeAssignment, error: Exception) -> None:
        """Persist a terminal business fact before releasing a failed assignment."""
        events = await self._session.load(assignment)
        if any(
            event.type in {"run.completed", "run.failed", "run.cancelled"}
            and event.run_id == assignment.run_id
            for event in events
        ):
            return
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
                            },
                        )
                    ],
                    command_id=f"runtime:skill.failed:{activation_id}",
                    operation="runtime.skill.failed",
                )
                events = await self._session.load(assignment)
        await self._append_once(
            assignment,
            events,
            "run.failed",
            {
                "run_id": assignment.run_id,
                "error": _error_payload(error),
            },
            identity=assignment.run_id,
            visibility=Visibility.USER,
        )

    async def _record_approval_wait(
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
        await self._append_once(
            assignment,
            events,
            "approval.requested",
            approval_request,
            identity=approval_id,
            visibility=Visibility.USER,
        )
        events = await self._session.load(assignment)
        await self._append_once(
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
            "response": self._response_to_dict(response),
            "call_index": call_index,
            "tool_invocation_id": call.tool_invocation_id,
            "approval_id": approval_id,
            "sequence": sequence,
        }
        waiting.pop("result", None)
        waiting.pop("side_events", None)
        await self._save_checkpoint(assignment, "capability.approval_waiting", waiting)

    async def _append_capability_event(
        self, assignment: RuntimeAssignment, event: NewEvent
    ) -> None:
        if event.payload.get("skill_activation_id"):
            identity = str(event.payload["skill_activation_id"])
        else:
            identity = hashlib.sha256(
                json.dumps(
                    event.payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode()
            ).hexdigest()[:24]
        await self._guard(assignment)
        await self._session.append(
            assignment,
            [event],
            command_id=f"runtime:{event.type}:{identity}",
            operation=f"runtime.{event.type}",
        )

    async def _run_tool(
        self,
        assignment: RuntimeAssignment,
        response: ModelResponse,
        call: ToolCall,
        index: int,
        sequence: int,
    ) -> bool:
        events = await self._session.load(assignment)
        completed = next(
            (
                event
                for event in events
                if event.type == "tool.call.completed"
                and event.payload.get("tool_invocation_id") == call.tool_invocation_id
            ),
            None,
        )
        if completed is not None:
            return True
        await self._append_once(
            assignment,
            events,
            "tool.call.requested",
            {
                "tool_invocation_id": call.tool_invocation_id,
                "name": call.name,
                "arguments": call.arguments,
                "version": call.version,
                "expected_side_effect": call.expected_side_effect,
                "activity": {"source": "tool"},
            },
            identity=call.tool_invocation_id,
        )
        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id, assignment.session_id, assignment.run_id
        )
        if (
            checkpoint is not None
            and checkpoint.phase == "tool_completed"
            and checkpoint.state.get("tool_invocation_id") == call.tool_invocation_id
        ):
            result = dict(checkpoint.state["result"])
        else:
            await self._save_checkpoint(
                assignment,
                "tool_pending",
                {
                    "response": self._response_to_dict(response),
                    "tool_index": index,
                    "tool_invocation_id": call.tool_invocation_id,
                    "sequence": sequence,
                },
            )
            await self._inject(InjectionPoint.BEFORE_TOOL)
            await self._guard(assignment)
            result = await self._tools.execute(assignment, call)
            await self._guard(assignment)
            await self._save_checkpoint(
                assignment,
                "tool_completed",
                {
                    "response": self._response_to_dict(response),
                    "tool_index": index,
                    "tool_invocation_id": call.tool_invocation_id,
                    "result": result,
                    "sequence": sequence,
                },
            )
            await self._inject(InjectionPoint.AFTER_TOOL)
        events = await self._session.load(assignment)
        if result.get("error_code") == "approval_required":
            metadata = result.get("metadata", {})
            approval_request = metadata.get("approval_request", {})
            approval_id = str(approval_request.get("approval_id", call.tool_invocation_id))
            await self._append_once(
                assignment,
                events,
                "approval.requested",
                dict(approval_request),
                identity=approval_id,
                visibility=Visibility.USER,
            )
            events = await self._session.load(assignment)
            await self._append_once(
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
            await self._save_checkpoint(
                assignment,
                "approval_waiting",
                {
                    "response": self._response_to_dict(response),
                    "tool_index": index,
                    "tool_invocation_id": call.tool_invocation_id,
                    "approval_id": approval_id,
                    "sequence": sequence,
                },
            )
            return False
        await self._append_once(
            assignment,
            events,
            "tool.call.completed",
            {
                "tool_invocation_id": call.tool_invocation_id,
                "name": call.name,
                "result": result,
            },
            identity=call.tool_invocation_id,
        )
        await self._save_checkpoint(
            assignment,
            "model_recorded",
            {"response": self._response_to_dict(response), "sequence": sequence},
        )
        return True

    async def _append_once(
        self,
        assignment: RuntimeAssignment,
        existing: list[Any],
        event_type: str,
        payload: dict[str, Any],
        *,
        identity: str,
        visibility: Visibility = Visibility.INTERNAL,
    ) -> None:
        if event_type == "approval.requested":
            if self._approval_request_is_pending(existing, identity):
                return
        elif any(
            event.type == event_type
            and (
                event.payload.get("run_id") == identity
                or event.payload.get("model_call_id") == identity
                or event.payload.get("tool_invocation_id") == identity
                or event.payload.get("approval_id") == identity
            )
            for event in existing
        ):
            return
        await self._guard(assignment)
        appended = await self._session.append(
            assignment,
            [NewEvent(type=event_type, payload=payload, visibility=visibility)],
            command_id=f"runtime:{event_type}:{assignment.run_id}:{identity}",
            operation=f"runtime.{event_type}",
            expected_version=len(existing),
        )
        if appended:
            existing.extend(appended)
        else:
            # Store-level command dedup: local version may already be ahead.
            refreshed = await self._session.load(assignment)
            existing.clear()
            existing.extend(refreshed)

    @classmethod
    def _approval_request_is_pending(cls, existing: list[Any], approval_id: str) -> bool:
        open_request = False
        for event in existing:
            if str(event.payload.get("approval_id", "")) != approval_id:
                continue
            if event.type == "approval.requested":
                open_request = True
            elif event.type in cls._APPROVAL_TERMINAL_TYPES:
                open_request = False
        return open_request

    async def _record_model_input(
        self,
        assignment: RuntimeAssignment,
        existing: list[Any],
        *,
        request: ModelRequest,
        turn_index: int,
    ) -> None:
        await self._append_once(
            assignment,
            existing,
            "model.input.prepared",
            self._model_input_evidence(request, turn_index=turn_index),
            identity=request.model_call_id,
            visibility=Visibility.USER,
        )

    @staticmethod
    def _model_input_evidence(request: ModelRequest, *, turn_index: int) -> dict[str, Any]:
        roles: dict[str, int] = {}
        user_prompt_preview = ""
        for message in request.messages:
            role = str(message.get("role", "unknown"))
            roles[role] = roles.get(role, 0) + 1
            if role == "user" and isinstance(message.get("content"), str):
                user_prompt_preview = " ".join(str(message["content"]).split())[:800]
        tool_names: list[str] = []
        for tool in request.tools:
            function = tool.get("function")
            if isinstance(function, dict) and function.get("name"):
                tool_names.append(str(function["name"]))
        encoded = json.dumps(
            {"messages": request.messages, "tools": request.tools},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode()
        tool_encoded = json.dumps(
            request.tools,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode()
        return {
            "model_call_id": request.model_call_id,
            "turn_index": turn_index,
            "message_count": len(request.messages),
            "role_counts": roles,
            "user_prompt_preview": user_prompt_preview,
            "input_digest": f"sha256:{hashlib.sha256(encoded).hexdigest()}",
            "tool_schema_digest": f"sha256:{hashlib.sha256(tool_encoded).hexdigest()}",
            "tool_names": tool_names,
            "preferred_model": request.policy.preferred_model,
            "allowed_providers": list(request.policy.allowed_providers),
            "data_classification": request.policy.data_classification,
            "max_output_tokens": request.max_output_tokens,
            "trusted_instruction_count": roles.get("system", 0),
        }

    @staticmethod
    def _tool_activity_metadata(capability_state: dict[str, Any], call: ToolCall) -> dict[str, Any]:
        for capability_id, item in dict(capability_state.get("loaded", {})).items():
            if not isinstance(item, dict):
                continue
            if item.get("kind") != "tool" or item.get("canonical_name") != call.name:
                continue
            server_id = str(item.get("server_id", ""))
            return {
                "source": "mcp" if server_id else "catalog",
                "capability_id": str(item.get("capability_id") or capability_id),
                "kind": "tool",
                "server_id": server_id or None,
                "version": str(item.get("version") or call.version),
            }
        return {
            "source": "auraclaw" if call.name.startswith("auraclaw.") else "tool",
            "version": call.version,
        }

    async def _guard(self, assignment: RuntimeAssignment) -> None:
        await self._control.assert_fencing(
            assignment_resource_id(assignment), assignment.fencing_token
        )
        if await self._control.is_cancelled(
            assignment.tenant_id, assignment.session_id, assignment.run_id
        ):
            raise RuntimeCancelledError(f"Runtime run cancelled: {assignment.run_id}")
        if assignment.deadline is not None and datetime.now(UTC) >= assignment.deadline:
            raise RuntimeCancelledError(f"Runtime deadline exceeded: {assignment.run_id}")

    async def _save_checkpoint(
        self, assignment: RuntimeAssignment, phase: str, state: dict[str, Any]
    ) -> None:
        await self._control.save_checkpoint(
            RuntimeCheckpoint(
                tenant_id=assignment.tenant_id,
                session_id=assignment.session_id,
                run_id=assignment.run_id,
                fencing_token=assignment.fencing_token,
                phase=phase,
                state=state,
                updated_at=datetime.now(UTC),
            )
        )

    async def _inject(self, point: InjectionPoint) -> None:
        if self._failure_injector is None:
            return
        result = self._failure_injector(point)
        if inspect.isawaitable(result):
            await result

    async def _publish_delta(
        self, assignment: RuntimeAssignment, sequence: int, delta: str
    ) -> None:
        try:
            await self._runtime_events.publish(
                RuntimeEvent(
                    event_id=f"rte_{uuid4().hex}",
                    tenant_id=assignment.tenant_id,
                    root_session_id=assignment.root_session_id,
                    session_id=assignment.session_id,
                    run_id=assignment.run_id,
                    sequence=sequence,
                    type="model.output.delta",
                    timestamp=datetime.now(UTC),
                    payload={"delta": delta},
                    visibility="user",
                )
            )
        except Exception:
            # The ephemeral stream is deliberately not a result-delivery guarantee.
            return

    async def _generate_with_live_deltas(
        self,
        assignment: RuntimeAssignment,
        request: ModelRequest,
        *,
        sequence: int,
        publish_deltas: bool = True,
        execute_started: float | None = None,
        prep: Awaitable[None] | None = None,
    ) -> tuple[ModelResponse, int]:
        response: ModelResponse | None = None
        stream_started = time.perf_counter()
        first_delta_logged = False
        if execute_started is None:
            execute_started = stream_started

        async def _next_chunk(iterator: Any) -> Any | None:
            try:
                return await anext(iterator)
            except StopAsyncIteration:
                return None

        stream = iter_model_stream(self._model, request)
        iterator = stream.__aiter__()
        first_chunk_task = asyncio.create_task(_next_chunk(iterator))
        try:
            if prep is not None:
                await prep
            chunk = await first_chunk_task
        except BaseException:
            if not first_chunk_task.done():
                first_chunk_task.cancel()
                with suppress(asyncio.CancelledError):
                    await first_chunk_task
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()
            raise
        if chunk is None:
            raise ModelProviderError("model stream ended without a completed response")
        while True:
            if chunk.kind == "delta":
                if publish_deltas and chunk.delta:
                    sequence += 1
                    await self._publish_delta(assignment, sequence, chunk.delta)
                    if not first_delta_logged:
                        first_delta_logged = True
                        now = time.perf_counter()
                        logger.info(
                            "ttft.first_delta session=%s run=%s model_call=%s "
                            "execute_ms=%.2f stream_ms=%.2f",
                            assignment.session_id,
                            assignment.run_id,
                            request.model_call_id,
                            (now - execute_started) * 1_000,
                            (now - stream_started) * 1_000,
                        )
            elif chunk.kind == "completed":
                response = chunk.response
            chunk = await _next_chunk(iterator)
            if chunk is None:
                break
        if response is None:
            raise ModelProviderError("model stream ended without a completed response")
        return response, sequence

    @staticmethod
    def _build_messages(events: list[Any]) -> tuple[dict[str, Any], ...]:
        messages: list[dict[str, Any]] = []
        for event in events:
            if event.type == "session.created":
                messages.append({"role": "user", "content": event.payload.get("goal", "")})
            elif event.type == "user.message.appended":
                messages.append({"role": "user", "content": event.payload.get("message", "")})
            elif event.type == "model.output.completed":
                messages.append({"role": "assistant", "content": event.payload.get("output", "")})
        return tuple(messages)

    @staticmethod
    def _build_capability_messages(
        events: list[Any],
    ) -> tuple[dict[str, Any], ...]:
        messages: list[dict[str, Any]] = []
        for event in events:
            if event.type == "session.created":
                messages.append({"role": "user", "content": event.payload.get("goal", "")})
            elif event.type == "user.message.appended":
                messages.append(
                    {
                        "role": "user",
                        "content": event.payload.get("message", ""),
                    }
                )
            elif event.type == "model.turn.completed":
                calls = []
                for raw_call in event.payload.get("tool_calls", ()):
                    if not isinstance(raw_call, dict):
                        continue
                    calls.append(
                        {
                            "id": str(raw_call["tool_invocation_id"]),
                            "type": "function",
                            "function": {
                                "name": str(raw_call["name"]),
                                "arguments": json.dumps(
                                    raw_call.get("arguments", {}),
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                            },
                        }
                    )
                message: dict[str, Any] = {
                    "role": "assistant",
                    "content": str(event.payload.get("output", "")),
                }
                if calls:
                    message["tool_calls"] = calls
                messages.append(message)
            elif event.type == "tool.call.completed":
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": str(event.payload.get("tool_invocation_id", "")),
                        "content": json.dumps(
                            event.payload.get("result", {}),
                            sort_keys=True,
                            separators=(",", ":"),
                            default=str,
                        ),
                    }
                )
            elif event.type == "model.output.completed":
                messages.append(
                    {
                        "role": "assistant",
                        "content": event.payload.get("output", ""),
                    }
                )
        return tuple(messages)

    @staticmethod
    def _tool_call_to_dict(call: ToolCall) -> dict[str, Any]:
        return {
            "tool_invocation_id": call.tool_invocation_id,
            "name": call.name,
            "arguments": call.arguments,
            "version": call.version,
            "expected_side_effect": call.expected_side_effect,
            "approval_id": call.approval_id,
            "credential_ref": call.credential_ref,
            "idempotency_key": call.idempotency_key,
        }

    @staticmethod
    def _response_to_dict(response: ModelResponse) -> dict[str, Any]:
        return {
            "model_call_id": response.model_call_id,
            "provider": response.provider,
            "model": response.model,
            "completed_output": response.completed_output,
            "deltas": list(response.deltas),
            "tool_calls": [
                {
                    "tool_invocation_id": call.tool_invocation_id,
                    "name": call.name,
                    "arguments": call.arguments,
                    "version": call.version,
                    "expected_side_effect": call.expected_side_effect,
                    "approval_id": call.approval_id,
                    "credential_ref": call.credential_ref,
                    "idempotency_key": call.idempotency_key,
                }
                for call in response.tool_calls
            ],
            "finish_reason": response.finish_reason,
            "usage": response.usage,
        }

    @staticmethod
    def _response_from_dict(data: dict[str, Any]) -> ModelResponse:
        return ModelResponse(
            model_call_id=str(data["model_call_id"]),
            provider=str(data["provider"]),
            model=str(data["model"]),
            completed_output=str(data["completed_output"]),
            deltas=tuple(str(delta) for delta in data.get("deltas", [])),
            tool_calls=tuple(
                ToolCall(
                    tool_invocation_id=str(call["tool_invocation_id"]),
                    name=str(call["name"]),
                    arguments=dict(call.get("arguments", {})),
                    version=str(call.get("version", "1")),
                    expected_side_effect=str(call.get("expected_side_effect", "read")),
                    approval_id=call.get("approval_id"),
                    credential_ref=call.get("credential_ref"),
                    idempotency_key=call.get("idempotency_key"),
                )
                for call in data.get("tool_calls", [])
            ),
            finish_reason=str(data.get("finish_reason", "stop")),
            usage=dict(data.get("usage", {})),
        )

    @staticmethod
    async def _validate_usage(assignment: RuntimeAssignment, response: ModelResponse) -> None:
        output_tokens = int(response.usage.get("output_tokens", 0))
        if output_tokens > assignment.budget.max_output_tokens:
            raise BudgetExceededError("provider output exceeded Runtime token budget")
        cost = float(response.usage.get("cost", 0.0))
        if assignment.budget.max_cost is not None and cost > assignment.budget.max_cost:
            raise BudgetExceededError("provider output exceeded Runtime cost budget")

    @staticmethod
    def _accumulate_usage(
        current: dict[str, int | float],
        update: dict[str, int | float],
    ) -> dict[str, int | float]:
        result = dict(current)
        for key, value in update.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                result[key] = result.get(key, 0) + value
        return result

    @staticmethod
    def _validate_cumulative_usage(
        assignment: RuntimeAssignment,
        usage: dict[str, int | float],
    ) -> None:
        if int(usage.get("output_tokens", 0)) > assignment.budget.max_output_tokens:
            raise BudgetExceededError("provider cumulative output exceeded Runtime token budget")
        if (
            assignment.budget.max_cost is not None
            and float(usage.get("cost", 0.0)) > assignment.budget.max_cost
        ):
            raise BudgetExceededError("provider cumulative cost exceeded Runtime budget")

    @staticmethod
    def _task_id(assignment: RuntimeAssignment) -> str:
        return f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
