from __future__ import annotations

from datetime import UTC, datetime

from auraclaw.contracts.errors import RuntimeCancelledError, RuntimeDeadlineExceededError
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.clients import assignment_resource_id
from auraclaw.runtime.ports import RuntimeControlClient


class RuntimeExecutionGuard:
    """Fail-closed fencing, cancellation and deadline checks for side-effect boundaries."""

    def __init__(self, control: RuntimeControlClient) -> None:
        self._control = control

    async def fence(self, assignment: RuntimeAssignment) -> None:
        await self._control.assert_fencing(
            assignment_resource_id(assignment), assignment.fencing_token
        )

    async def check(self, assignment: RuntimeAssignment) -> None:
        await self.fence(assignment)
        if await self._control.is_cancelled(
            assignment.tenant_id, assignment.session_id, assignment.run_id
        ):
            raise RuntimeCancelledError(f"Runtime run cancelled: {assignment.run_id}")
        if assignment.deadline is not None and datetime.now(UTC) >= assignment.deadline:
            raise RuntimeDeadlineExceededError(f"Runtime deadline exceeded: {assignment.run_id}")
