from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from auraclaw.contracts.errors import (
    BudgetExceededError,
    ModelProviderError,
    RuntimeCancelledError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.clients import assignment_resource_id
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
        failure_injector: FailureInjector | None = None,
    ) -> None:
        self._control = control_store
        self._session = session
        self._model = model
        self._tools = tools
        self._runtime_events = runtime_events
        self._policy = model_policy or ModelPolicy()
        self._capability_controller = capability_controller
        self._failure_injector = failure_injector

    async def execute(self, assignment: RuntimeAssignment) -> None:
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
        events = await self._session.load(assignment)
        if any(
            event.type == "run.completed" and event.run_id == assignment.run_id
            for event in events
        ):
            await self._control.finish_assignment(self._task_id(assignment), "completed")
            return
        if self._capability_controller is not None:
            await self._execute_capability_loop(assignment)
            return

        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id, assignment.session_id, assignment.run_id
        )
        model_call_id = f"mdl_{assignment.run_id}_step_1"
        if checkpoint is None or checkpoint.phase == "model_pending":
            await self._save_checkpoint(
                assignment,
                "model_pending",
                {"model_call_id": model_call_id, "sequence": 0},
            )
            await self._inject(InjectionPoint.BEFORE_MODEL)
            await self._guard(assignment)
            messages = self._build_messages(events)
            if not messages:
                raise ModelProviderError(
                    "model request has no user/assistant messages "
                    f"(session={assignment.session_id} run={assignment.run_id})"
                )
            response = await self._model.generate(
                ModelRequest(
                    model_call_id=model_call_id,
                    tenant_id=assignment.tenant_id,
                    run_id=assignment.run_id,
                    messages=messages,
                    policy=self._policy,
                    max_output_tokens=assignment.budget.max_output_tokens,
                )
            )
            await self._validate_usage(assignment, response)
            await self._save_checkpoint(
                assignment,
                "model_completed",
                {"response": self._response_to_dict(response), "sequence": 0},
            )
            await self._inject(InjectionPoint.AFTER_MODEL)
        elif checkpoint.phase == "model_completed":
            response = self._response_from_dict(dict(checkpoint.state["response"]))
        else:
            response_data = checkpoint.state.get("response")
            if not isinstance(response_data, dict):
                raise RuntimeError(f"invalid Runtime checkpoint phase: {checkpoint.phase}")
            response = self._response_from_dict(response_data)

        sequence = int((checkpoint.state if checkpoint else {}).get("sequence", 0))
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
            can_continue = await self._run_tool(
                assignment, response, call, index, sequence
            )
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
        self, assignment: RuntimeAssignment
    ) -> None:
        assert self._capability_controller is not None
        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
        )
        state: dict[str, Any] = (
            dict(checkpoint.state)
            if checkpoint is not None
            and checkpoint.phase.startswith("capability.")
            else {
                "turn_index": 0,
                "steps_used": 0,
                "sequence": 0,
                "usage": {},
                "capability_state": self._capability_controller.empty_state(),
                "call_index": 0,
                "call_signatures": {},
            }
        )
        while int(state.get("steps_used", 0)) < assignment.budget.max_steps:
            await self._guard(assignment)
            turn_index = int(state.get("turn_index", 0))
            model_call_id = f"mdl_{assignment.run_id}_turn_{turn_index + 1}"
            resume_phase = checkpoint.phase if checkpoint is not None else ""
            if (
                resume_phase
                in {
                    "capability.model_completed",
                    "capability.call_completed",
                    "capability.approval_waiting",
                }
                and int(state.get("turn_index", -1)) == turn_index
                and isinstance(state.get("response"), dict)
            ):
                response = self._response_from_dict(dict(state["response"]))
            else:
                events = await self._session.load(assignment)
                capability_state = dict(state.get("capability_state", {}))
                trusted = await self._capability_controller.trusted_messages(
                    assignment, capability_state
                )
                output_tokens_used = int(
                    dict(state.get("usage", {})).get("output_tokens", 0)
                )
                remaining_output_tokens = (
                    assignment.budget.max_output_tokens - output_tokens_used
                )
                if remaining_output_tokens < 1:
                    raise BudgetExceededError(
                        "Runtime cumulative output token budget was exhausted"
                    )
                await self._save_checkpoint(
                    assignment,
                    "capability.model_pending",
                    {
                        **state,
                        "model_call_id": model_call_id,
                        "call_index": 0,
                    },
                )
                await self._inject(InjectionPoint.BEFORE_MODEL)
                await self._guard(assignment)
                response = await self._model.generate(
                    ModelRequest(
                        model_call_id=model_call_id,
                        tenant_id=assignment.tenant_id,
                        run_id=assignment.run_id,
                        messages=(
                            *trusted,
                            *self._build_capability_messages(events),
                        ),
                        tools=self._capability_controller.model_tools(
                            capability_state
                        ),
                        policy=self._policy,
                        max_output_tokens=remaining_output_tokens,
                    )
                )
                usage = self._accumulate_usage(
                    dict(state.get("usage", {})), response.usage
                )
                self._validate_cumulative_usage(assignment, usage)
                state = {
                    **state,
                    "response": self._response_to_dict(response),
                    "usage": usage,
                    "steps_used": int(state.get("steps_used", 0)) + 1,
                    "call_index": 0,
                }
                await self._save_checkpoint(
                    assignment, "capability.model_completed", state
                )
                checkpoint = await self._control.load_checkpoint(
                    assignment.tenant_id,
                    assignment.session_id,
                    assignment.run_id,
                )
                await self._inject(InjectionPoint.AFTER_MODEL)

            sequence = int(state.get("sequence", 0))
            if not response.tool_calls:
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
                    "tool_calls": [
                        self._tool_call_to_dict(call)
                        for call in response.tool_calls
                    ],
                },
                identity=response.model_call_id,
            )

            call_index = int(state.get("call_index", 0))
            while call_index < len(response.tool_calls):
                call = response.tool_calls[call_index]
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
                    },
                    identity=call.tool_invocation_id,
                )
                is_completed_resume = (
                    checkpoint is not None
                    and checkpoint.phase == "capability.call_completed"
                    and checkpoint.state.get("tool_invocation_id")
                    == call.tool_invocation_id
                )
                if is_completed_resume:
                    assert checkpoint is not None
                    result = dict(checkpoint.state["result"])
                    capability_state = dict(
                        checkpoint.state.get("capability_state", {})
                    )
                    side_events = tuple(
                        NewEvent(
                            type=str(item["type"]),
                            payload=dict(item["payload"]),
                            visibility=Visibility(
                                str(item.get("visibility", "internal"))
                            ),
                        )
                        for item in checkpoint.state.get("side_events", ())
                    )
                else:
                    if int(state.get("steps_used", 0)) >= assignment.budget.max_steps:
                        raise BudgetExceededError(
                            "Runtime capability step budget was exhausted"
                        )
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
                        "steps_used": int(state.get("steps_used", 0)) + 1,
                    }
                    if int(state["steps_used"]) > assignment.budget.max_steps:
                        raise BudgetExceededError(
                            "Runtime capability step budget was exhausted"
                        )
                    await self._save_checkpoint(
                        assignment, "capability.call_completed", state
                    )
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
                    await self._append_capability_event(
                        assignment, side_event
                    )
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
                call_index += 1
                state = {
                    **state,
                    "capability_state": capability_state,
                    "call_index": call_index,
                    "side_events": [],
                }
                await self._save_checkpoint(
                    assignment, "capability.model_completed", state
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
                await self._save_checkpoint(
                    assignment, "capability.model_pending", state
                )
                checkpoint = await self._control.load_checkpoint(
                    assignment.tenant_id,
                    assignment.session_id,
                    assignment.run_id,
                )
                continue

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
                    "usage": dict(state.get("usage", {})),
                },
                identity=response.model_call_id,
                visibility=Visibility.USER,
            )
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
            await self._save_checkpoint(
                assignment, "capability.completed", state
            )
            await self._control.finish_assignment(
                self._task_id(assignment), "completed"
            )
            return
        raise BudgetExceededError("Runtime capability step budget was exhausted")

    async def record_failure(
        self, assignment: RuntimeAssignment, error: Exception
    ) -> None:
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
                    event.type
                    in {"skill.completed", "skill.failed", "skill.cancelled"}
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
                                "policy_decision_id": binding.get(
                                    "policy_decision_id"
                                ),
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
        metadata = (
            dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
        )
        raw_request = metadata.get("approval_request", {})
        approval_request = (
            dict(raw_request) if isinstance(raw_request, dict) else {}
        )
        approval_id = str(
            approval_request.get("approval_id", call.tool_invocation_id)
        )
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
        await self._save_checkpoint(
            assignment, "capability.approval_waiting", waiting
        )

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
        if any(
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
        await self._session.append(
            assignment,
            [NewEvent(type=event_type, payload=payload, visibility=visibility)],
            command_id=f"runtime:{event_type}:{identity}",
            operation=f"runtime.{event_type}",
        )

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
                messages.append(
                    {"role": "user", "content": event.payload.get("goal", "")}
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
                        "tool_call_id": str(
                            event.payload.get("tool_invocation_id", "")
                        ),
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
    async def _validate_usage(
        assignment: RuntimeAssignment, response: ModelResponse
    ) -> None:
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
            raise BudgetExceededError(
                "provider cumulative output exceeded Runtime token budget"
            )
        if (
            assignment.budget.max_cost is not None
            and float(usage.get("cost", 0.0)) > assignment.budget.max_cost
        ):
            raise BudgetExceededError(
                "provider cumulative cost exceeded Runtime budget"
            )

    @staticmethod
    def _task_id(assignment: RuntimeAssignment) -> str:
        return f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
