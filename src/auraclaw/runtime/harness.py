from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from auraclaw.contracts.errors import (
    BudgetExceededError,
    RuntimeCancelledError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import ControlStateStore, RuntimeAssignment, RuntimeCheckpoint
from auraclaw.runtime.clients import assignment_resource_id
from auraclaw.runtime.ports import (
    ModelClient,
    ModelPolicy,
    ModelRequest,
    ModelResponse,
    RuntimeEvent,
    RuntimeEventPublisher,
    SessionClient,
    ToolCall,
    ToolClient,
)


class InjectionPoint(StrEnum):
    BEFORE_MODEL = "before_model"
    AFTER_MODEL = "after_model"
    BEFORE_TOOL = "before_tool"
    AFTER_TOOL = "after_tool"


FailureInjector = Callable[[InjectionPoint], Awaitable[None] | None]


class AgentHarness:
    """Recoverable one-step Agent harness with externally persisted checkpoints."""

    def __init__(
        self,
        *,
        control_store: ControlStateStore,
        session: SessionClient,
        model: ModelClient,
        tools: ToolClient,
        runtime_events: RuntimeEventPublisher,
        model_policy: ModelPolicy | None = None,
        failure_injector: FailureInjector | None = None,
    ) -> None:
        self._control = control_store
        self._session = session
        self._model = model
        self._tools = tools
        self._runtime_events = runtime_events
        self._policy = model_policy or ModelPolicy()
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
        if any(event.type == "run.completed" for event in events):
            await self._control.finish_assignment(self._task_id(assignment), "completed")
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
            response = await self._model.generate(
                ModelRequest(
                    model_call_id=model_call_id,
                    tenant_id=assignment.tenant_id,
                    run_id=assignment.run_id,
                    messages=self._build_messages(events),
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
        return tuple(messages)

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
    def _task_id(assignment: RuntimeAssignment) -> str:
        return f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
