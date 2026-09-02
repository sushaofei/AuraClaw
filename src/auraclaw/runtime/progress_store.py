from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from auraclaw.contracts.errors import CollaborationValidationError
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint
from auraclaw.runtime.execution_state import RuntimePhase
from auraclaw.runtime.ports import RuntimeControlClient


class RuntimeProgressStore:
    """Own persisted checkpoint and assignment-suspension commit boundaries."""

    def __init__(self, control: RuntimeControlClient) -> None:
        self._control = control

    async def save_checkpoint(
        self,
        assignment: RuntimeAssignment,
        phase: RuntimePhase,
        state: dict[str, Any],
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

    async def suspend_for_children(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        child_session_ids: tuple[str, ...],
    ) -> None:
        waiting = tuple(dict.fromkeys(str(item) for item in child_session_ids if item))
        if not waiting:
            raise CollaborationValidationError(
                "waiting_children requires a non-empty persisted Child wait set"
            )
        checkpoint = RuntimeCheckpoint(
            tenant_id=assignment.tenant_id,
            session_id=assignment.session_id,
            run_id=assignment.run_id,
            fencing_token=assignment.fencing_token,
            phase=RuntimePhase.AGENT_WAITING_CHILDREN,
            state={**state, "waiting_child_ids": list(waiting)},
            updated_at=datetime.now(UTC),
        )
        await self._control.suspend_with_checkpoint(
            self.task_id(assignment), checkpoint, "waiting_children"
        )
        persisted = await self._control.load_checkpoint(
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
        )
        persisted_waiting = (
            tuple(str(item) for item in persisted.state.get("waiting_child_ids", ()))
            if persisted is not None
            else ()
        )
        if (
            persisted is None
            or persisted.phase != RuntimePhase.AGENT_WAITING_CHILDREN
            or persisted_waiting != waiting
        ):
            raise CollaborationValidationError(
                "waiting_children checkpoint did not preserve the Child wait set"
            )

    @staticmethod
    def task_id(assignment: RuntimeAssignment) -> str:
        return f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
