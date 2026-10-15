from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import time
from contextlib import suppress
from dataclasses import asdict, replace
from typing import Any

from auraclaw.contracts.errors import (
    BudgetExceededError,
    CollaborationValidationError,
    ModelOutputTruncatedError,
    ModelProviderError,
    RuntimeCancelledError,
    RuntimeDeadlineExceededError,
    RuntimeNoProgressError,
    RuntimeOutputTokenBudgetExceededError,
    RuntimeStepBudgetExceededError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.domain.skill_execution import RUN_TERMINAL_EVENTS
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.collaboration_controller import RuntimeCollaborationController
from auraclaw.runtime.event_committer import CanonicalEventCommitter
from auraclaw.runtime.execution.budget import RuntimeBudgetService
from auraclaw.runtime.execution.context import RuntimeContextBuilder
from auraclaw.runtime.execution.contracts import (
    FailureInjector as FailureInjector,
)
from auraclaw.runtime.execution.contracts import (
    FinishReasonKind as FinishReasonKind,
)
from auraclaw.runtime.execution.contracts import (
    InjectionPoint as InjectionPoint,
)
from auraclaw.runtime.execution.messages import (
    build_capability_messages,
    build_messages,
    model_input_evidence,
    scope_model_call_id_to_request,
    tool_activity_metadata,
    truncation_recovery_messages,
)
from auraclaw.runtime.execution.recovery import RuntimeRecoveryService
from auraclaw.runtime.execution.serialization import (
    classify_finish_reason,
    response_from_dict,
    response_to_dict,
    tool_call_to_dict,
)
from auraclaw.runtime.execution_guard import RuntimeExecutionGuard
from auraclaw.runtime.execution_state import RuntimePhase
from auraclaw.runtime.ports import (
    ModelClient,
    ModelPolicy,
    ModelRequest,
    RuntimeControlClient,
    RuntimeEventPublisher,
    SessionClient,
    ToolCall,
    ToolClient,
)
from auraclaw.runtime.progress_store import RuntimeProgressStore
from auraclaw.runtime.repeat_policy import repeat_decision, repeat_target, wait_seconds
from auraclaw.runtime.rounds import (
    ModelRoundExecutor,
    ToolRoundDisposition,
)
from auraclaw.runtime.task_router import RuntimeTaskRouter
from auraclaw.runtime.tool_round import ToolRoundExecutor

logger = logging.getLogger(__name__)


class RuntimeExecutionEngine:
    """Recoverable execution engine with optional governed multi-turn capabilities."""

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
        task_router: RuntimeTaskRouter | None = None,
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
        self._progress = RuntimeProgressStore(control_store, session, self._events)
        self._tool_rounds = ToolRoundExecutor(
            control=control_store,
            session=session,
            tools=tools,
            guard=self._guard_service,
            progress=self._progress,
            events=self._events,
            response_serializer=response_to_dict,
        )
        self._session = session
        self._budget = RuntimeBudgetService(
            session, self._events, per_turn_output_tokens=per_turn_output_tokens,
            terminal_output_reserve=terminal_output_reserve,
        )
        self._context = RuntimeContextBuilder(capability_controller, collaboration_controller)
        self._recovery = RuntimeRecoveryService(
            control=control_store, session=session, guard=self._guard_service,
            events=self._events, progress=self._progress,
            capabilities=capability_controller, collaboration=collaboration_controller,
        )
        self._model_rounds = ModelRoundExecutor(
            model=model,
            control=control_store,
            runtime_events=runtime_events,
        )
        self._policy = model_policy or ModelPolicy()
        self._capability_controller = capability_controller
        self._collaboration_controller = collaboration_controller
        self._task_router = task_router
        self._failure_injector = failure_injector
        self._max_truncation_recoveries = max_truncation_recoveries

    async def execute(self, assignment: RuntimeAssignment) -> None:
        execute_started = time.perf_counter()
        if assignment.budget.policy_version not in {"1", "2"}:
            raise CollaborationValidationError("unsupported Runtime budget policy version")
        try:
            await self._guard_service.check(assignment)
        except (RuntimeCancelledError, RuntimeDeadlineExceededError):
            if await self._recovery.recover_pending_outcomes(assignment):
                return
            raise
        if assignment.budget.max_steps < 1:
            raise RuntimeStepBudgetExceededError("Runtime step budget is exhausted")
        events = await self._session.load(assignment)
        terminal = next((e for e in events if e.run_id == assignment.run_id
                         and e.type in RUN_TERMINAL_EVENTS), None)
        if terminal is not None:
            if await self._recovery.recover_pending_outcomes(assignment):
                return
            await self._control.finish_assignment(self._task_id(assignment), terminal.type[4:])
            return
        prior_start = next((e for e in events if e.run_id == assignment.run_id
                            and e.type == "run.started"), None)
        if (prior_start is not None and "budget_snapshot" in prior_start.payload
                and prior_start.payload["budget_snapshot"] != asdict(assignment.budget)):
            raise CollaborationValidationError("Run budget snapshot changed during recovery")
        await self._events.append_once(
            assignment,
            events,
            "run.started",
            {"run_id": assignment.run_id, "runtime_id": assignment.runtime_id,
             "budget_snapshot": asdict(assignment.budget)},
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
            messages = build_messages(events)
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
            await self._budget.validate_usage(assignment, response)
            await self._progress.save_checkpoint(
                assignment,
                RuntimePhase.MODEL_COMPLETED,
                {
                    "response": response_to_dict(response),
                    "sequence": sequence,
                    "deltas_published": True,
                },
            )
            await self._inject(InjectionPoint.AFTER_MODEL)
        elif checkpoint.phase == "model_completed":
            response = response_from_dict(dict(checkpoint.state["response"]))
            sequence = int(checkpoint.state.get("sequence", 0))
            if not checkpoint.state.get("deltas_published"):
                for delta in response.deltas:
                    sequence += 1
                    await self._model_rounds.publish_delta(assignment, sequence, delta)
        else:
            response_data = checkpoint.state.get("response")
            if not isinstance(response_data, dict):
                raise RuntimeError(f"invalid Runtime checkpoint phase: {checkpoint.phase}")
            response = response_from_dict(response_data)
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
            {"response": response_to_dict(response), "sequence": sequence},
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
            {"response": response_to_dict(response), "sequence": sequence},
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
        checkpoint = await self._recovery.load_checkpoint(assignment)
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
        default_policy = "1" if checkpoint is not None else assignment.budget.policy_version
        if state.get("budget_policy_version", default_policy) != assignment.budget.policy_version:
            raise CollaborationValidationError("Run budget policy changed during recovery")
        state["budget_policy_version"] = assignment.budget.policy_version
        capability_state = dict(state.get("capability_state", {}))
        if self._capability_controller is not None:
            self._capability_controller.restore_skill_events(
                capability_state,
                [event for event in (events or ()) if event.run_id == assignment.run_id],
            )
        if (
            self._capability_controller is not None
            and not capability_state.get("required_capabilities_preloaded")
        ):
            state["capability_state"] = await self._capability_controller.preload_required(
                assignment,
                capability_state,
            )
        if self._task_router is not None and "routing_decision" not in state:
            route = await self._task_router.route(
                assignment,
                events or await self._session.load(assignment),
                dict(state.get("capability_state", {})),
                reserve_model=(
                    lambda model_call_id, tokens: self._budget.reserve_budget(
                        assignment, model_call_id, "model", tokens
                    )
                )
                if assignment.budget.policy_version == "2"
                else None,
            )
            for event in route.events:
                await self._events.append_capability_event(assignment, event)
            state["capability_state"] = route.capability_state
            state["routing_decision"] = route.decision.model_dump(mode="json")
            state["router_metrics"] = dict(route.metrics or {})
            if route.planner_usage:
                state["usage"] = self._budget.accumulate_usage(
                    dict(state.get("usage", {})), route.planner_usage
                )
                state["steps_used"] = int(state.get("steps_used", 0)) + 1
                self._budget.validate_cumulative_usage(assignment, dict(state["usage"]))
            state["router_capability_prepared"] = bool(
                route.decision.route_kind.value in {"single_capability", "sequential_plan"}
                and route.decision.outcome.value == "adopted"
            )
            if (
                route.decision.outcome.value == "adopted"
                and route.decision.route_kind.value == "coordinator_dag"
                and route.decision.plan is not None
            ):
                if self._collaboration_controller is None:
                    raise CollaborationValidationError(
                        "adopted coordinator DAG requires Collaboration Service"
                    )
                submission = await self._collaboration_controller.submit_routing_plan(
                    assignment,
                    route.decision.plan,
                )
                children = submission.get("children", ())
                child_ids = tuple(
                    str(child["session_id"])
                    for child in children
                    if isinstance(child, dict) and child.get("session_id")
                )
                if not child_ids or len(child_ids) != len(route.decision.plan.steps):
                    raise CollaborationValidationError(
                        "Collaboration Service returned an incomplete routing plan submission"
                    )
                state["router_plan_submission"] = {
                    "plan_digest": route.decision.plan.plan_digest,
                    "child_session_ids": list(child_ids),
                }
                state["router_metrics"] = {
                    **dict(state.get("router_metrics", {})),
                    "router.semantic_planner.submitted.count": 1.0,
                }
                await self._progress.suspend_for_children(assignment, state, child_ids)
                return
        if await self._recovery.resume_wait(assignment, checkpoint, state):
            return
        turn_events = events
        while True:
            await self._guard_service.check(assignment)
            turn_index = int(state.get("turn_index", 0))
            # Start with the logical turn/fencing identity.  Once the prompt is built
            # below, bind it to a request digest as well so recovery after changing
            # child/capability state cannot reuse an id for different content.
            logical_model_call_id = (
                f"mdl_{assignment.run_id}_turn_{turn_index + 1}"
                f"_fence_{assignment.fencing_token}"
            )
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
                response = response_from_dict(dict(state["response"]))
            else:
                if int(state.get("steps_used", 0)) >= assignment.budget.max_steps:
                    raise RuntimeStepBudgetExceededError(
                        "Runtime capability step budget was exhausted"
                    )
                if turn_events is None:
                    turn_events = await self._session.load(assignment)
                concluding_reason = self._budget.concluding_reason(assignment, state, turn_events)
                if concluding_reason is not None:
                    state["concluding_reason"] = concluding_reason
                capability_state = dict(state.get("capability_state", {}))
                if self._capability_controller is not None:
                    if await self._apply_skill_binding_disposition(
                        assignment, state, capability_state,
                    ):
                        return
                context = await self._context.build(assignment, state, turn_events)
                turn_budget = self._budget.allocate_turn(assignment, state)
                trusted = context.trusted_messages
                if turn_budget.include_truncation_guidance:
                    trusted += truncation_recovery_messages()
                request = ModelRequest(
                    model_call_id=logical_model_call_id,
                    tenant_id=assignment.tenant_id,
                    run_id=assignment.run_id,
                    session_id=assignment.session_id,
                    messages=(
                        *trusted,
                        *build_capability_messages(turn_events),
                    ),
                    tools=context.tools,
                    run_max_cost=(assignment.budget.max_cost
                                  if assignment.budget.policy_version == "2" else None),
                    policy=self._policy,
                    max_output_tokens=turn_budget.max_output_tokens,
                    runtime_metrics=(
                        {
                            **self._capability_controller.trusted_message_metrics(assignment),
                            **(
                                dict(state.get("router_metrics", {}))
                                if turn_index == 0
                                else {}
                            ),
                        }
                        if self._capability_controller is not None
                        else dict(state.get("router_metrics", {}))
                    ),
                    prompt_cache_key=(
                        self._capability_controller.prompt_cache_key(
                            assignment, capability_state
                        )
                        if self._capability_controller is not None
                        else None
                    ),
                )
                request = scope_model_call_id_to_request(request)
                model_call_id = request.model_call_id
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
                await self._record_model_input(
                    assignment,
                    turn_events,
                    request=request,
                    turn_index=turn_index,
                )
                if assignment.budget.policy_version == "2":
                    await checkpoint_ready
                    await self._budget.reserve_budget(assignment, model_call_id, "model",
                                               request.max_output_tokens)
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
                usage = self._budget.accumulate_usage(dict(state.get("usage", {})), response.usage)
                state = {
                    **state,
                    "response": response_to_dict(response),
                    "usage": usage,
                    "steps_used": int(state.get("steps_used", 0)) + 1,
                    "call_index": 0,
                    "sequence": sequence,
                    "deltas_published": True,
                    "last_turn_was_truncation_recovery": bool(
                        state.get("truncation_recovery_pending")
                    ),
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
                    "tool_calls": [tool_call_to_dict(call) for call in response.tool_calls],
                },
                identity=response.model_call_id,
            )

            self._budget.validate_cumulative_usage(assignment, dict(state.get("usage", {})))
            if assignment.budget.policy_version == "2":
                if "output_tokens" not in response.usage:
                    raise ModelProviderError("Runtime cannot continue with unknown output usage")
                if assignment.budget.max_cost is not None and "cost" not in response.usage:
                    raise ModelProviderError("Runtime cannot continue with unknown cost usage")
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
                target = (repeat_target(assignment, call, dict(state.get("capability_state", {})))
                          if assignment.budget.policy_version == "2" else None)
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
                        "repeat_identity": ({"binding": target.binding,
                                             "arguments": target.arguments,
                                             "read_only": target.read_only} if target else {}),
                        "activity": tool_activity_metadata(
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
                        await self._recovery.settle_unstarted_batch(
                            assignment, response.tool_calls[call_index:], turn_index,
                            "runtime_step_budget_exceeded",
                        )
                        raise RuntimeStepBudgetExceededError(
                            "Runtime capability step budget was exhausted"
                        )
                    if assignment.budget.policy_version == "2":
                        try:
                            await self._budget.reserve_budget(
                                assignment, call.tool_invocation_id, "tool", 0
                            )
                        except BudgetExceededError as error:
                            await self._recovery.settle_unstarted_batch(
                                assignment, response.tool_calls[call_index:], turn_index, error.code
                            )
                            raise
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
                    if assignment.budget.policy_version == "1" and repeated > 3:
                        await self._recovery.settle_unstarted_batch(
                            assignment, response.tool_calls[call_index:], turn_index,
                            "tool_repeat_suppressed",
                        )
                        logger.warning(
                            "tool.repeat_suppressed capability_id=%s "
                            "repeat_count=%s side_effect_status=not_started outcome=bounded",
                            call.name,
                            repeated,
                        )
                        raise RuntimeNoProgressError(
                            "Runtime detected a repeated no-progress capability call"
                        )
                    signatures[signature] = repeated
                    state["call_signatures"] = signatures
                    await self._inject(InjectionPoint.BEFORE_TOOL)
                    await self._guard_service.check(assignment)
                    terminal = False
                    waiting_child_ids = ()
                    suppressed = (repeat_decision(assignment, call, target, events)
                                  if assignment.budget.policy_version == "2" else None)
                    try:
                        if suppressed is None and assignment.budget.policy_version == "2":
                            delay = wait_seconds(target, events, assignment.run_id)
                            while delay > 0:
                                await self._guard_service.check(assignment)
                                await asyncio.sleep(min(1.0, delay))
                                delay = wait_seconds(target, events, assignment.run_id)
                            await self._guard_service.check(assignment)
                            # A durable grant can expire while waiting; re-evaluate before dispatch.
                            suppressed = repeat_decision(assignment, call, target, events)
                    except (RuntimeCancelledError, RuntimeDeadlineExceededError) as error:
                        await self._recovery.settle_unstarted_batch(
                            assignment, response.tool_calls[call_index:], turn_index, error.code
                            )
                        raise
                    if state.get("concluding_reason"):
                        suppressed = {
                            "status": "denied", "error_code": "tool_repeat_suppressed",
                            "side_effect_status": "not_started", "retryable": False,
                            "summary": "Concluding: this business call was not executed.",
                            "metadata": {"policy_version": "2", "reason": "concluding"},
                        }
                    if suppressed is not None:
                        result = suppressed
                        capability_state = dict(state.get("capability_state", {}))
                        side_events = ()
                    elif (
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
                            "tool.repeat_suppressed capability_id=%s "
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
                        raise RuntimeStepBudgetExceededError(
                            "Runtime capability step budget was exhausted"
                        )
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
                    await self._recovery.record_approval_wait(
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
            if state.get("concluding_reason"):
                await self._events.append_once(
                    assignment, events, "model.output.completed",
                    {"model_call_id": response.model_call_id, "provider": response.provider,
                     "model": response.model, "output": response.completed_output,
                     "finish_reason": response.finish_reason, "usage": dict(state.get("usage", {})),
                     "partial": True}, identity=response.model_call_id, visibility=Visibility.USER,
                )
                reason = str(state["concluding_reason"])
                if reason == "runtime_no_progress_detected":
                    raise RuntimeNoProgressError(
                        "Runtime stopped a no-progress loop; partial results retained"
                    )
                if reason == "runtime_output_token_budget_exceeded":
                    raise RuntimeOutputTokenBudgetExceededError(
                        "Runtime exploration stopped at the reserved concluding output allowance")
                raise RuntimeStepBudgetExceededError(
                    "Runtime exploration stopped at the reserved concluding step allowance")
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
                    finish_kind = classify_finish_reason(response)
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
            model_input_evidence(request, turn_index=turn_index),
            identity=request.model_call_id,
            visibility=Visibility.USER,
        )

    async def _inject(self, point: InjectionPoint) -> None:
        if self._failure_injector is None:
            return
        result = self._failure_injector(point)
        if inspect.isawaitable(result):
            await result

    @staticmethod
    def _task_id(assignment: RuntimeAssignment) -> str:
        return f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"

    async def record_failure(self, assignment: RuntimeAssignment, error: Exception) -> bool:
        return await self._recovery.record_failure(assignment, error)
