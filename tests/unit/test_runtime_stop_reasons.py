from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.contracts.errors import (
    RuntimeCostBudgetExceededError,
    RuntimeDeadlineExceededError,
    RuntimeOutputTokenBudgetExceededError,
)
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
from auraclaw.runtime.execution_engine import RuntimeExecutionEngine
from auraclaw.runtime.execution_guard import RuntimeExecutionGuard


def assignment():
    return RuntimeAssignment(tenant_id="tenant", root_session_id="root", session_id="session",
                             run_id="run", runtime_id="runtime", lease_id="lease",
                             fencing_token=1, role="root", resource_profile={}, deadline=None,
                             budget=RuntimeBudget(max_steps=48, max_output_tokens=8192, max_cost=1))


def test_token_and_cost_limits_have_distinct_reasons():
    RuntimeExecutionEngine._validate_cumulative_usage(
        assignment(), {"output_tokens": 8192, "cost": 1})
    with pytest.raises(RuntimeOutputTokenBudgetExceededError):
        RuntimeExecutionEngine._validate_cumulative_usage(assignment(), {"output_tokens": 8193})
    with pytest.raises(RuntimeCostBudgetExceededError):
        RuntimeExecutionEngine._validate_cumulative_usage(assignment(), {"cost": 1.01})


@pytest.mark.asyncio
async def test_deadline_is_not_user_cancellation():
    class Control:
        async def assert_fencing(self, *args):
            pass

        async def is_cancelled(self, *args):
            return False

    with pytest.raises(RuntimeDeadlineExceededError) as error:
        await RuntimeExecutionGuard(Control()).check(
            replace(assignment(), deadline=datetime.now(UTC) - timedelta(seconds=1)))
    assert error.value.code == "runtime_deadline_exceeded"
