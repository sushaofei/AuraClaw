from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.event_committer import CanonicalEventCommitter
from auraclaw.runtime.execution_guard import RuntimeExecutionGuard
from auraclaw.runtime.execution_state import RuntimePhase
from auraclaw.runtime.ports import (
    ModelResponse,
    RuntimeControlClient,
    SessionClient,
    ToolCall,
    ToolClient,
)
from auraclaw.runtime.progress_store import RuntimeProgressStore
from auraclaw.runtime.rounds import ToolRoundDisposition, ToolRoundResult

RoundHook = Callable[[], Awaitable[None]]
ResponseSerializer = Callable[[ModelResponse], dict[str, Any]]


class ToolRoundExecutor:
    """Execute or resume one idempotent tool round and commit its durable result."""

    def __init__(
        self,
        *,
        control: RuntimeControlClient,
        session: SessionClient,
        tools: ToolClient,
        guard: RuntimeExecutionGuard,
        progress: RuntimeProgressStore,
        events: CanonicalEventCommitter,
        response_serializer: ResponseSerializer,
    ) -> None:
        self._control = control
        self._session = session
        self._tools = tools
        self._guard = guard
        self._progress = progress
        self._events = events
        self._response_to_dict = response_serializer

    async def execute(
        self,
        assignment: RuntimeAssignment,
        response: ModelResponse,
        call: ToolCall,
        index: int,
        sequence: int,
        *,
        before_tool: RoundHook,
        after_tool: RoundHook,
    ) -> ToolRoundResult:
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
            return ToolRoundResult(
                disposition=ToolRoundDisposition.CONTINUE,
                result=dict(completed.payload.get("result", {})),
                resumed_from_checkpoint=True,
            )
        await self._events.append_once(
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
        resumed_from_checkpoint = (
            checkpoint is not None
            and checkpoint.phase == RuntimePhase.TOOL_COMPLETED
            and checkpoint.state.get("tool_invocation_id") == call.tool_invocation_id
        )
        if resumed_from_checkpoint:
            assert checkpoint is not None
            result = dict(checkpoint.state["result"])
        else:
            await self._progress.save_checkpoint(
                assignment,
                RuntimePhase.TOOL_PENDING,
                {
                    "response": self._response_to_dict(response),
                    "tool_index": index,
                    "tool_invocation_id": call.tool_invocation_id,
                    "sequence": sequence,
                },
            )
            await before_tool()
            await self._guard.check(assignment)
            result = await self._tools.execute(assignment, call)
            await self._guard.check(assignment)
            await self._progress.save_checkpoint(
                assignment,
                RuntimePhase.TOOL_COMPLETED,
                {
                    "response": self._response_to_dict(response),
                    "tool_index": index,
                    "tool_invocation_id": call.tool_invocation_id,
                    "result": result,
                    "sequence": sequence,
                },
            )
            await after_tool()
        events = await self._session.load(assignment)
        if result.get("error_code") == "approval_required":
            metadata = result.get("metadata", {})
            approval_request = metadata.get("approval_request", {})
            approval_id = str(
                approval_request.get("approval_id", call.tool_invocation_id)
            )
            await self._events.append_once(
                assignment,
                events,
                "approval.requested",
                dict(approval_request),
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
            await self._progress.save_checkpoint(
                assignment,
                RuntimePhase.APPROVAL_WAITING,
                {
                    "response": self._response_to_dict(response),
                    "tool_index": index,
                    "tool_invocation_id": call.tool_invocation_id,
                    "approval_id": approval_id,
                    "sequence": sequence,
                },
            )
            return ToolRoundResult(
                disposition=ToolRoundDisposition.WAITING_FOR_HUMAN,
                result=result,
                resumed_from_checkpoint=resumed_from_checkpoint,
            )
        await self._events.append_once(
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
        await self._progress.save_checkpoint(
            assignment,
            RuntimePhase.MODEL_RECORDED,
            {"response": self._response_to_dict(response), "sequence": sequence},
        )
        return ToolRoundResult(
            disposition=ToolRoundDisposition.CONTINUE,
            result=result,
            resumed_from_checkpoint=resumed_from_checkpoint,
        )
