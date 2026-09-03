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
from enum import StrEnum
from typing import Any

from auraclaw.contracts.errors import (
    BudgetExceededError,
    CollaborationValidationError,
    ModelOutputTruncatedError,
    ModelProviderError,
    RuntimeCancelledError,
    TerminalBudgetExceededError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.domain.skill_execution import RUN_TERMINAL_EVENTS, pending_skill_invocations
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.collaboration_controller import RuntimeCollaborationController
from auraclaw.runtime.event_committer import CanonicalEventCommitter
from auraclaw.runtime.execution_guard import RuntimeExecutionGuard
from auraclaw.runtime.execution_state import RuntimePhase
from auraclaw.runtime.ports import (
    ModelClient,
    ModelPolicy,
    ModelRequest,
    ModelResponse,
    RuntimeControlClient,
    RuntimeEventPublisher,
    SessionClient,
    ToolCall,
    ToolClient,
)
from auraclaw.runtime.progress_store import RuntimeProgressStore
from auraclaw.runtime.rounds import (
    ModelRoundExecutor,
    ToolRoundDisposition,
)
from auraclaw.runtime.tool_round import ToolRoundExecutor

logger = logging.getLogger(__name__)


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


class InjectionPoint(StrEnum):
    BEFORE_MODEL = "before_model"
    AFTER_MODEL = "after_model"
    BEFORE_TOOL = "before_tool"
    AFTER_TOOL = "after_tool"


class FinishReasonKind(StrEnum):
    STOP = "stop"
    TOOL_CALLS = "tool_calls"
    TRUNCATED = "truncated"
    REFUSED = "refused"
    UNKNOWN = "unknown"


FailureInjector = Callable[[InjectionPoint], Awaitable[None] | None]


class RuntimeExecutionEngine:
    """Recoverable execution engine with optional governed multi-turn capabilities."""

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
        per_turn_output_tokens: int = 4096,
        terminal_output_reserve: int = 512,
        max_truncation_recoveries: int = 1,
    ) -> None:
        if per_turn_output_tokens < 1 or terminal_output_reserve < 1:
            raise ValueError("Runtime output token limits must be positive")
        if max_truncation_recoveries < 0:
            raise ValueError("max_truncation_recoveries cannot be negative")
        self._control = control_store
        self._guard_service = RuntimeExecutionGuard(control_store)
        self._events = CanonicalEventCommitter(session, self._guard_service)
        self._progress = RuntimeProgressStore(control_store)
        self._tool_rounds = ToolRoundExecutor(
            control=control_store,
            session=session,
            tools=tools,
            guard=self._guard_service,
            progress=self._progress,
            events=self._events,
            response_serializer=self._response_to_dict,
        )
        self._session = session
        self._model = model
        self._model_rounds = ModelRoundExecutor(
            model=model,
            control=control_store,
            runtime_events=runtime_events,
        )
        self._tools = tools
        self._runtime_events = runtime_events
        self._policy = model_policy or ModelPolicy()
        self._capability_controller = capability_controller
        self._collaboration_controller = collaboration_controller
        self._failure_injector = failure_injector
        self._per_turn_output_tokens = per_turn_output_tokens
        self._terminal_output_reserve = terminal_output_reserve
        self._max_truncation_recoveries = max_truncation_recoveries

    async def execute(self, assignment: RuntimeAssignment) -> None:
        execute_started = time.perf_counter()
        try:
            await self._guard_service.check(assignment)
        except RuntimeCancelledError:
            if await self._recover_pending_outcomes(assignment):
                return
            raise
        if assignment.budget.max_steps < 1:
            raise BudgetExceededError("Runtime step budget is exhausted")
        events = await self._session.load(assignment)
        terminal = next((e for e in events if e.run_id == assignment.run_id
                         and e.type in RUN_TERMINAL_EVENTS), None)
        if terminal is not None:
            if await self._recover_pending_outcomes(assignment):
                return
            await self._control.finish_assignment(self._task_id(assignment), terminal.type[4:])
            return
        await self._events.append_once(
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
            if self._capability_controller is not None:
                await self._capability_controller.release_run(assignment)
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
                self._progress.save_checkpoint(
                    assignment,
                    RuntimePhase.MODEL_PENDING,
                    {"model_call_id": model_call_id, "sequence": 0},
                )
            )
            await self._inject(InjectionPoint.BEFORE_MODEL)
            await self._guard_service.check(assignment)
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
                session_id=assignment.session_id,
                messages=messages,
                policy=self._policy,
                max_output_tokens=assignment.budget.max_output_tokens,
            )
            await self._record_model_input(assignment, events, request=request, turn_index=0)
            model_round = await self._model_rounds.execute(
                assignment,
                request,
                sequence=0,
                execute_started=execute_started,
                prep=checkpoint_ready,
            )
            response, sequence = model_round.response, model_round.sequence
            await self._validate_usage(assignment, response)
            await self._progress.save_checkpoint(
                assignment,
                RuntimePhase.MODEL_COMPLETED,
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
                    await self._model_rounds.publish_delta(assignment, sequence, delta)
        else:
            response_data = checkpoint.state.get("response")
            if not isinstance(response_data, dict):
                raise RuntimeError(f"invalid Runtime checkpoint phase: {checkpoint.phase}")
            response = self._response_from_dict(response_data)
            sequence = int(checkpoint.state.get("sequence", 0))
            if not checkpoint.state.get("deltas_published"):
                for delta in response.deltas:
                    sequence += 1
                    await self._model_rounds.publish_delta(assignment, sequence, delta)

        events = await self._session.load(assignment)
        await self._events.append_once(
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
        await self._progress.save_checkpoint(
            assignment,
            RuntimePhase.MODEL_RECORDED,
            {"response": self._response_to_dict(response), "sequence": sequence},
        )

        for index, call in enumerate(response.tool_calls):
            tool_round = await self._tool_rounds.execute(
                assignment,
                response,
                call,
                index,
                sequence,
                before_tool=lambda: self._inject(InjectionPoint.BEFORE_TOOL),
                after_tool=lambda: self._inject(InjectionPoint.AFTER_TOOL),
            )
            if tool_round.disposition is ToolRoundDisposition.WAITING_FOR_HUMAN:
                await self._control.finish_assignment(
                    self._task_id(assignment), "waiting_for_human"
                )
                return

        events = await self._session.load(assignment)
        await self._events.append_once(
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
        await self._progress.save_checkpoint(
            assignment,
            RuntimePhase.COMPLETED,
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
        if (
            checkpoint is not None
            and checkpoint.phase in {
                "agent.waiting_children",
                "collaboration.waiting_children",
            }
            and self._collaboration_controller is not None
        ):
            waiting = tuple(
                str(item)
                for item in state.get("waiting_child_ids", ())
                if item
            )
            _, active_children = await self._collaboration_controller.child_state(
                assignment
            )
            still_waiting = tuple(
                child_id for child_id in waiting if child_id in set(active_children)
            )
            if still_waiting:
                await self._progress.suspend_for_children(
                    assignment,
                    state,
                    still_waiting,
                )
                return
        turn_events = events
        while int(state.get("steps_used", 0)) < assignment.budget.max_steps:
            await self._guard_service.check(assignment)
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
                    "capability.workflow_running",
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
                terminal_role = assignment.role in {"worker", "repair", "reviewer"}
                recovery_turn = bool(state.get("truncation_recovery_pending"))
                terminal_reserve = min(
                    self._terminal_output_reserve,
                    max(1, assignment.budget.max_output_tokens // 4),
                )
                available_for_turn = remaining_output_tokens
                if terminal_role and not recovery_turn:
                    available_for_turn -= terminal_reserve
                    if available_for_turn < 1:
                        logger.warning(
                            "agent.terminal_budget_exhausted role=%s turn=%s "
                            "remaining=%s reserve=%s",
                            assignment.role,
                            turn_index,
                            remaining_output_tokens,
                            terminal_reserve,
                        )
                        raise TerminalBudgetExceededError(
                            "terminal collaboration reserve leaves no model turn budget"
                        )
                elif recovery_turn:
                    available_for_turn = min(available_for_turn, terminal_reserve)
                    trusted += (
                        {
                            "role": "system",
                            "content": (
                                "The previous model output was truncated before the required "
                                "terminal collaboration tool call. Do not repeat or continue the "
                                "large body. Persist any large payload through an authorized "
                                "Artifact or Resource write capability, then call the required "
                                "publish_result, publish_review, or join tool with a short summary "
                                "and valid references. If no governed write path exists, publish "
                                "an explicit limitation or failure instead of inventing a "
                                "reference."
                            ),
                        },
                    )
                checkpoint_ready = asyncio.create_task(
                    self._progress.save_checkpoint(
                        assignment,
                        RuntimePhase.CAPABILITY_MODEL_PENDING,
                        {
                            **state,
                            "model_call_id": model_call_id,
                            "call_index": 0,
                        },
                    )
                )
                await self._inject(InjectionPoint.BEFORE_MODEL)
                await self._guard_service.check(assignment)
                request = ModelRequest(
                    model_call_id=model_call_id,
                    tenant_id=assignment.tenant_id,
                    run_id=assignment.run_id,
                    session_id=assignment.session_id,
                    messages=(
                        *trusted,
                        *self._build_capability_messages(turn_events),
                    ),
                    tools=model_tools,
                    policy=self._policy,
                    max_output_tokens=min(
                        self._per_turn_output_tokens, available_for_turn
                    ),
                    runtime_metrics=(
                        self._capability_controller.trusted_message_metrics(
                            assignment
                        )
                        if self._capability_controller is not None
                        else {}
                    ),
                    prompt_cache_key=(
                        self._capability_controller.prompt_cache_key(
                            assignment, capability_state
                        )
                        if self._capability_controller is not None
                        else None
                    ),
                )
                await self._record_model_input(
                    assignment,
                    turn_events,
                    request=request,
                    turn_index=turn_index,
                )
                model_round = await self._model_rounds.execute(
                    assignment,
                    request,
                    sequence=int(state.get("sequence", 0)),
                    publish_deltas=True,
                    execute_started=execute_started,
                    prep=checkpoint_ready,
                )
                response, sequence = model_round.response, model_round.sequence
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
                    "last_turn_was_truncation_recovery": recovery_turn,
                    "truncation_recovery_pending": False,
                }
                await self._progress.save_checkpoint(
                    assignment, RuntimePhase.CAPABILITY_MODEL_COMPLETED, state
                )
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
                    await self._model_rounds.publish_delta(assignment, sequence, delta)
            state["sequence"] = sequence
            events = await self._session.load(assignment)
            await self._events.append_once(
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
                await self._events.append_once(
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
                    recovering_pending = any(
                        isinstance(item, dict) and item.get("workflow_status") == "unknown"
                        and (item.get("activation", {}).get("activation_key")
                             == call.tool_invocation_id)
                        for item in state.get("capability_state", {}).get("active_skills", ())
                    )
                    signatures = dict(state.get("call_signatures", {}))
                    repeated = int(signatures.get(signature, 0)) + (0 if recovering_pending else 1)
                    if repeated > 3:
                        logger.warning(
                            "tool.argument_validation_failed capability_id=%s "
                            "repeat_count=%s side_effect_status=not_started outcome=bounded",
                            call.name,
                            repeated,
                        )
                        raise BudgetExceededError(
                            "Runtime detected a repeated no-progress capability call"
                        )
                    signatures[signature] = repeated
                    state["call_signatures"] = signatures
                    await self._inject(InjectionPoint.BEFORE_TOOL)
                    await self._guard_service.check(assignment)
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
                        async def checkpoint_capability_progress(
                            capability_progress: dict[str, Any],
                            capability_events: tuple[NewEvent, ...] = (),
                            current_state: dict[str, Any] = state,
                            current_call: ToolCall = call,
                            current_call_index: int = call_index,
                        ) -> None:
                            recovery = bool(capability_events) and all(
                                event.type == "skill.invocation.settled"
                                for event in capability_events
                            )
                            if recovery:
                                await self._guard_service.fence(assignment)
                            else:
                                await self._guard_service.check(assignment)
                            for event in capability_events:
                                await self._events.append_capability_event(
                                    assignment, event, recovery=recovery
                                )
                            progress_state = {
                                **current_state,
                                "capability_state": capability_progress,
                                "tool_invocation_id": current_call.tool_invocation_id,
                                "call_index": current_call_index,
                            }
                            await self._progress.save_checkpoint(
                                assignment,
                                RuntimePhase.CAPABILITY_WORKFLOW_RUNNING,
                                progress_state,
                            )

                        self._capability_controller.restore_skill_events(
                            state.get("capability_state", {}),
                            [event for event in await self._session.load(assignment)
                             if event.run_id == assignment.run_id],
                        )
                        execution = await self._capability_controller.execute(
                            assignment,
                            call,
                            dict(state.get("capability_state", {})),
                            progress=checkpoint_capability_progress,
                        )
                        result = execution.result
                        capability_state = execution.state
                        side_events = execution.events
                    if result.get("error_code") == "tool_schema_invalid":
                        logger.info(
                            "tool.argument_validation_failed capability_id=%s "
                            "repeat_count=%s side_effect_status=%s",
                            call.name,
                            repeated,
                            result.get("side_effect_status", "not_started"),
                        )
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
                        "steps_used": int(state.get("steps_used", 0))
                        + (0 if recovering_pending else 1),
                    }
                    if int(state["steps_used"]) > assignment.budget.max_steps:
                        raise BudgetExceededError("Runtime capability step budget was exhausted")
                    await self._progress.save_checkpoint(
                        assignment, RuntimePhase.CAPABILITY_CALL_COMPLETED, state
                    )
                    checkpoint = await self._control.load_checkpoint(
                        assignment.tenant_id,
                        assignment.session_id,
                        assignment.run_id,
                    )
                    await self._inject(InjectionPoint.AFTER_TOOL)

                for side_event in side_events:
                    await self._events.append_capability_event(assignment, side_event)

                if (result.get("status") in {"unknown", "paused"}
                        and result.get("skill_activation_id")):
                    # Keep the call cursor and original invocation. A wake-up
                    # queries that result before any further workflow execution.
                    await self._progress.save_checkpoint(
                        assignment, RuntimePhase.CAPABILITY_WORKFLOW_RUNNING, state
                    )
                    await self._control.suspend_assignment(
                        self._task_id(assignment),
                        "waiting_for_human" if result["status"] == "paused" else "waiting_for_tool"
                    )
                    return

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

                events = await self._session.load(assignment)
                await self._events.append_once(
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
                    await self._progress.suspend_for_children(
                        assignment, waiting_state, waiting_child_ids
                    )
                    return
                if terminal:
                    await self._progress.save_checkpoint(
                        assignment, RuntimePhase.AGENT_COMPLETED, state
                    )
                    await self._control.finish_assignment(self._task_id(assignment), "completed")
                    return
                call_index += 1
                state = {
                    **state,
                    "capability_state": capability_state,
                    "call_index": call_index,
                    "side_events": [],
                }
                await self._progress.save_checkpoint(
                    assignment, RuntimePhase.CAPABILITY_MODEL_COMPLETED, state
                )
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
                await self._progress.save_checkpoint(
                    assignment, RuntimePhase.CAPABILITY_MODEL_PENDING, state
                )
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
                    await self._progress.suspend_for_children(
                        assignment, waiting_state, active_children
                    )
                    return
                if all_children and assignment.role in {"root", "coordinator"}:
                    raise CollaborationValidationError("Coordinator graph was not joined")
                if assignment.role in {"worker", "repair", "reviewer"}:
                    finish_kind = self._classify_finish_reason(response)
                    if finish_kind is FinishReasonKind.TRUNCATED:
                        recovery_count = int(state.get("truncation_recovery_count", 0))
                        remaining_after_turn = assignment.budget.max_output_tokens - int(
                            dict(state.get("usage", {})).get("output_tokens", 0)
                        )
                        can_recover = (
                            recovery_count < self._max_truncation_recoveries
                            and int(state.get("steps_used", 0)) < assignment.budget.max_steps
                            and remaining_after_turn > 0
                        )
                        logger.warning(
                            "agent.model_output_truncated role=%s turn=%s used=%s "
                            "remaining=%s recovery_count=%s recoverable=%s",
                            assignment.role,
                            turn_index,
                            int(dict(state.get("usage", {})).get("output_tokens", 0)),
                            remaining_after_turn,
                            recovery_count,
                            can_recover,
                        )
                        if can_recover:
                            state = {
                                **state,
                                "turn_index": turn_index + 1,
                                "call_index": 0,
                                "truncation_recovery_count": recovery_count + 1,
                                "truncation_recovery_pending": True,
                            }
                            state.pop("response", None)
                            state.pop("result", None)
                            state.pop("tool_invocation_id", None)
                            await self._progress.save_checkpoint(
                                assignment, RuntimePhase.CAPABILITY_MODEL_PENDING, state
                            )
                            checkpoint = await self._control.load_checkpoint(
                                assignment.tenant_id,
                                assignment.session_id,
                                assignment.run_id,
                            )
                            continue
                        raise ModelOutputTruncatedError(
                            "model output was truncated before terminal collaboration result"
                        )
                    if state.get("last_turn_was_truncation_recovery"):
                        raise ModelOutputTruncatedError(
                            "truncation recovery ended without terminal collaboration result"
                        )
                    if finish_kind is FinishReasonKind.REFUSED:
                        raise ModelProviderError(
                            "model response was refused before terminal collaboration result"
                        )
                    if finish_kind is FinishReasonKind.UNKNOWN:
                        raise ModelProviderError(
                            f"unknown model finish reason: {response.finish_reason}"
                        )
                    raise CollaborationValidationError("role output contract was not published")
            await self._events.append_once(
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
                    await self._events.append_capability_event(assignment, event)
            events = await self._session.load(assignment)
            await self._events.append_once(
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
            await self._progress.save_checkpoint(
                assignment, RuntimePhase.CAPABILITY_COMPLETED, state
            )
            if self._capability_controller is not None:
                await self._capability_controller.release_run(assignment)
            await self._control.finish_assignment(self._task_id(assignment), "completed")
            return
        raise BudgetExceededError("Runtime capability step budget was exhausted")

    @staticmethod
    def _classify_finish_reason(response: ModelResponse) -> FinishReasonKind:
        if response.tool_calls:
            return FinishReasonKind.TOOL_CALLS
        reason = response.finish_reason.strip().lower().replace("-", "_")
        if reason in {"stop", "end_turn", "completed"}:
            return FinishReasonKind.STOP
        if reason in {"tool_calls", "function_call", "function_calls"}:
            return FinishReasonKind.TOOL_CALLS
        if reason in {"length", "max_tokens", "max_output_tokens"}:
            return FinishReasonKind.TRUNCATED
        if reason in {"content_filter", "safety", "refusal", "refused"}:
            return FinishReasonKind.REFUSED
        return FinishReasonKind.UNKNOWN

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
        await self._events.append_once(
            assignment,
            events,
            "skill.revocation.applied",
            evidence,
            identity=f"{activation_id}:{action}",
        )
        state = {**state, "skill_revocation": evidence}
        if action == "pause":
            await self._progress.save_checkpoint(
                assignment, RuntimePhase.CAPABILITY_SKILL_REVOKED_PAUSED, state
            )
            await self._capability_controller.release_run(assignment)
            await self._control.suspend_assignment(self._task_id(assignment), "waiting_for_human")
            return True
        events = await self._session.load(assignment)
        await self._events.append_capability_event(
            assignment, NewEvent(type="skill.cancelled", payload=evidence)
        )
        events = await self._session.load(assignment)
        await self._events.append_once(
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
        await self._progress.save_checkpoint(
            assignment, RuntimePhase.CAPABILITY_SKILL_REVOKED_CANCELLED, state
        )
        await self._capability_controller.release_run(assignment)
        await self._control.finish_assignment(self._task_id(assignment), "cancelled")
        return True

    async def _recover_pending_outcomes(self, assignment: RuntimeAssignment) -> bool:
        """Read-only reconciliation survives a stopped Run; no model or business call is allowed."""
        await self._guard_service.fence(assignment)
        events = await self._session.load(assignment)
        pending = pending_skill_invocations(events, run_id=assignment.run_id)
        if not pending:
            return False
        if self._capability_controller is None:
            await self._control.suspend_assignment(self._task_id(assignment), "waiting_for_tool")
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
            await self._events.append_capability_event(assignment, NewEvent(
                type="skill.invocation.settled",
                payload={**requested.payload, "status": observed["status"],
                         "side_effect_status": observed.get("side_effect_status")},
            ), recovery=True)
        events = await self._session.load(assignment)
        if pending_skill_invocations(events, run_id=assignment.run_id):
            await self._control.suspend_assignment(self._task_id(assignment), "waiting_for_tool")
            return True
        for activation_id in dict.fromkeys(e.payload["skill_activation_id"] for e in pending):
            await self._events.append_capability_event(assignment, NewEvent(
                type="skill.cancelled", payload={"skill_activation_id": activation_id,
                    "error_code": "run_stopped_after_invocation_settled"},
            ), recovery=True)
        events = await self._session.load(assignment)
        terminal = next((e for e in events if e.run_id == assignment.run_id
                         and e.type in RUN_TERMINAL_EVENTS), None)
        if terminal is None:
            await self._events.append_once(assignment, events, "run.failed",
                {"run_id": assignment.run_id, "error_code": "run_stopped_after_invocation_settled"},
                identity=assignment.run_id, recovery=True)
        await self._capability_controller.release_run(assignment)
        await self._control.finish_assignment(
            self._task_id(assignment), terminal.type[4:] if terminal is not None else "failed"
        )
        return True

    async def record_failure(self, assignment: RuntimeAssignment, error: Exception) -> bool:
        """Return True when pending-result recovery owns assignment disposition."""
        if await self._recover_pending_outcomes(assignment):
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
            },
            identity=assignment.run_id,
            visibility=Visibility.USER,
            recovery=True,
        )
        if self._capability_controller is not None:
            await self._capability_controller.release_run(assignment)
        return False

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
            "response": self._response_to_dict(response),
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

    async def _record_model_input(
        self,
        assignment: RuntimeAssignment,
        existing: list[Any],
        *,
        request: ModelRequest,
        turn_index: int,
    ) -> None:
        await self._events.append_once(
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
            if item.get("kind") != "tool":
                continue
            model_name = item.get("model_tool", {}).get("function", {}).get("name")
            if call.name not in {model_name, item.get("canonical_name")}:
                continue
            server_id = str(item.get("server_id", ""))
            return {
                "source": "mcp" if server_id else "catalog",
                **({"canonical_name": item.get("canonical_name"),
                    "invocation_ref": item["invocation_ref"]}
                   if isinstance(item.get("invocation_ref"), dict) else {}),
                "capability_id": str(item.get("capability_id") or capability_id),
                "kind": "tool",
                "server_id": server_id or None,
                "version": str(item.get("version") or call.version),
            }
        return {
            "source": "auraclaw" if call.name.startswith("auraclaw.") else "tool",
            "version": call.version,
        }

    async def _inject(self, point: InjectionPoint) -> None:
        if self._failure_injector is None:
            return
        result = self._failure_injector(point)
        if inspect.isawaitable(result):
            await result

    @staticmethod
    def _build_messages(events: list[Any]) -> tuple[dict[str, Any], ...]:
        messages: list[dict[str, Any]] = []
        for event in events:
            if event.type == "session.created":
                messages.append({"role": "user", "content": event.payload.get("goal", "")})
            elif event.type == "child.created":
                child_context = {
                    "goal": event.payload.get("goal", ""),
                    "output_contract": event.payload.get("output_contract", {}),
                    "input_refs": event.payload.get("input_refs", []),
                    "tool_permissions": event.payload.get("tool_permissions", []),
                }
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "Authoritative Child assignment:\n"
                            + json.dumps(
                                child_context,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                        ),
                    }
                )
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
            elif event.type == "child.created":
                child_context = {
                    "goal": event.payload.get("goal", ""),
                    "output_contract": event.payload.get("output_contract", {}),
                    "input_refs": event.payload.get("input_refs", []),
                    "tool_permissions": event.payload.get("tool_permissions", []),
                }
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "Authoritative Child assignment:\n"
                            + json.dumps(
                                child_context,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                        ),
                    }
                )
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
