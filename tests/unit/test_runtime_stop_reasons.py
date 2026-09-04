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


@pytest.mark.asyncio
async def test_v2_run_is_not_scheduled_on_an_old_runtime():
    from auraclaw.control.ports import RunnableItem, RuntimeInstance
    from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore

    store = InMemoryControlStateStore()
    item = RunnableItem(task_id="t:s:r", tenant_id="t", root_session_id="s", session_id="s",
                        run_id="r", source_version=1, budget=RuntimeBudget(policy_version="2"))
    await store.register_runtime(RuntimeInstance(runtime_id="old", runtime_type="agent",
                                                role="agent", node_id="local", capacity=1,
                                                capabilities={}))
    assert await store.select_runtime(item) is None
    await store.register_runtime(RuntimeInstance(runtime_id="new", runtime_type="agent",
                                                role="agent", node_id="local", capacity=1,
                                                capabilities={"runtime_governance_v2": True}))
    assert (await store.select_runtime(item)).runtime_id == "new"


def test_runnable_rebuild_uses_the_current_run_budget_snapshot():
    from types import SimpleNamespace

    from auraclaw.control.runnable_feed import RunnableFeedConsumer

    def event(kind, payload):
        return SimpleNamespace(type=kind, payload=payload, tenant_id="t", root_session_id="s",
                               session_id="s", run_id="r",
                               actor=SimpleNamespace(type="user", id="1"))

    item = RunnableFeedConsumer._derive([
        event("session.created", {"budget": {"max_steps": 12}}),
        event("run.requested", {"run_id": "r", "budget": {"max_steps": 48, "policy_version": "2"}}),
    ], 2)
    assert item.budget.policy_version == "2"
    assert item.budget.max_steps == 48
