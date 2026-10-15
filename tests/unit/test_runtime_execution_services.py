from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from auraclaw.contracts.errors import (
    RuntimeOutputTokenBudgetExceededError,
    TerminalBudgetExceededError,
)
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget, RuntimeCheckpoint
from auraclaw.runtime.execution.budget import RuntimeBudgetService
from auraclaw.runtime.execution.context import RuntimeContextBuilder
from auraclaw.runtime.execution.recovery import RuntimeRecoveryService
from auraclaw.runtime.execution.serialization import response_from_dict, response_to_dict


def assignment(policy="1", role="root"):
    return RuntimeAssignment(
        tenant_id="tenant",
        root_session_id="session",
        session_id="session",
        run_id="run",
        runtime_id="runtime",
        lease_id="lease",
        fencing_token=7,
        role=role,
        resource_profile={},
        budget=RuntimeBudget(max_steps=12, max_output_tokens=1024, policy_version=policy),
    )


def budget_service():
    # Allocation is independent of control, providers and event I/O.
    return RuntimeBudgetService(
        None,
        None,
        per_turn_output_tokens=1000,
        terminal_output_reserve=512,
    )


@pytest.mark.parametrize(
    "policy,role,recovery,concluding,expected,guidance",
    [
        ("1", "root", False, False, 900, False),
        ("1", "worker", False, False, 644, False),
        ("1", "worker", True, False, 256, True),
        ("2", "root", False, False, 644, False),
        ("2", "worker", True, False, 644, False),
        ("2", "worker", True, True, 256, False),
    ],
)
def test_turn_allocation_preserves_legacy_and_canonical_budget_policies(
    policy,
    role,
    recovery,
    concluding,
    expected,
    guidance,
):
    state = {"usage": {"output_tokens": 124}, "truncation_recovery_pending": recovery}
    if concluding:
        state["concluding_reason"] = "runtime_step_budget_exceeded"
    turn = budget_service().allocate_turn(assignment(policy, role), state)
    assert turn.max_output_tokens == expected
    assert turn.include_truncation_guidance is guidance
    assert state["usage"] == {"output_tokens": 124}


def test_terminal_reserve_exhaustion_and_hard_token_exhaustion_remain_distinct():
    with pytest.raises(TerminalBudgetExceededError):
        budget_service().allocate_turn(assignment(role="worker"), {"usage": {"output_tokens": 768}})
    turn = budget_service().allocate_turn(
        assignment("2", "worker"),
        {
            "usage": {"output_tokens": 768},
            "concluding_reason": "runtime_output_token_budget_exceeded",
        },
    )
    assert turn.max_output_tokens == 256
    with pytest.raises(RuntimeOutputTokenBudgetExceededError):
        budget_service().allocate_turn(assignment("2"), {"usage": {"output_tokens": 1024}})


def test_conclusion_prioritizes_steps_and_does_not_mutate_checkpoint_state():
    state = {"steps_used": 10, "usage": {"output_tokens": 900}, "concluding_reason": "old"}
    assert RuntimeBudgetService.concluding_reason(assignment("2"), state, []) == (
        "runtime_step_budget_exceeded"
    )
    assert state["concluding_reason"] == "old"
    assert RuntimeBudgetService.concluding_reason(assignment("1"), state, []) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ["1", "2"])
async def test_restore_uses_latest_canonical_progress_only_for_policy_two(policy):
    task = assignment(policy)
    legacy = RuntimeCheckpoint(
        tenant_id=task.tenant_id,
        session_id=task.session_id,
        run_id=task.run_id,
        fencing_token=1,
        phase="capability.model_pending",
        state={"sequence": 1},
        updated_at=datetime.now(UTC),
    )

    class Control:
        async def load_checkpoint(self, *_):
            return legacy

    class Session:
        async def load(self, _):
            assert policy == "2", "legacy recovery must not consult canonical progress"
            return [
                SimpleNamespace(
                    type="runtime.progress.recorded",
                    run_id=run,
                    payload={
                        "phase": "capability.call_completed",
                        "state": {"sequence": sequence},
                    },
                )
                for run, sequence in [("run", 2), ("run", 3), ("other-run", 99)]
            ]

    service = RuntimeRecoveryService(
        control=Control(),
        session=Session(),
        guard=None,
        events=None,
        progress=None,
        capabilities=None,
        collaboration=None,
    )
    checkpoint = await service.load_checkpoint(task)
    assert checkpoint is not None
    if policy == "1":
        assert checkpoint is legacy
    else:
        assert checkpoint.phase == "capability.call_completed"
        assert checkpoint.state == {"sequence": 3}
        assert checkpoint.fencing_token == task.fencing_token


@pytest.mark.asyncio
async def test_recovery_fences_before_reading_receipts_or_observing_tools():
    class Guard:
        async def fence(self, _):
            raise RuntimeError("stale fence")

    class Session:
        async def load(self, _):
            raise AssertionError("cannot read receipts after losing ownership")

    service = RuntimeRecoveryService(
        control=None,
        session=Session(),
        guard=Guard(),
        events=None,
        progress=None,
        capabilities=None,
        collaboration=None,
    )
    with pytest.raises(RuntimeError, match="stale fence"):
        await service.recover_pending_outcomes(assignment())


@pytest.mark.asyncio
async def test_context_preserves_instruction_order_and_filters_discovery_after_router_binding():
    calls = []
    bound_tool = {"type": "function", "function": {"name": "bound.read"}}
    discovery = {"type": "function", "function": {"name": "auraclaw.capabilities.load"}}
    terminal = {"type": "function", "function": {"name": "auraclaw.collaboration.publish_result"}}

    class Capabilities:
        async def trusted_messages(self, _, state):
            calls.append("capability instructions")
            assert state == {"loaded": {}}
            return ({"role": "system", "content": "capability instructions"},)

        def model_tools(self, _):
            calls.append("capability tools")
            return discovery, bound_tool

    class Collaboration:
        async def trusted_messages(self, _):
            calls.append("collaboration instructions")
            return ({"role": "system", "content": "collaboration instructions"},)

        def model_tools(self, _):
            calls.append("collaboration tools")
            return (terminal,)

    builder = RuntimeContextBuilder(Capabilities(), Collaboration())
    state = {
        "capability_state": {"loaded": {}},
        "router_capability_prepared": True,
        "routing_decision": {
            "outcome": "adopted",
            "route_kind": "sequential_plan",
            "plan": {
                "plan_digest": "sha256:plan",
                "steps": [],
            },
        },
    }
    facts = [
        SimpleNamespace(
            type="run.requested",
            payload={
                "run_id": "run",
                "read_refresh": [{"capability": "bound.read", "max_calls": 1}],
            },
        )
    ]
    context = await builder.build(assignment("2"), state, facts)
    assert calls == [
        "capability instructions",
        "collaboration instructions",
        "capability tools",
        "collaboration tools",
    ]
    contents = [message["content"] for message in context.trusted_messages]
    assert "bounded read refresh grants" in contents[0]
    assert contents[1] == "capability instructions"
    assert "plan_digest" in contents[2]
    assert contents[3] == "collaboration instructions"
    assert context.tools == (bound_tool, terminal)
    state["concluding_reason"] = "runtime_step_budget_exceeded"
    concluded = await builder.build(assignment("2"), state, facts)
    assert concluded.tools == ()
    assert "Do not publish a successful Child result" in concluded.trusted_messages[-1]["content"]
    later = await builder.build(assignment("2"), {**state, "turn_index": 1}, facts)
    assert not any("plan_digest" in message["content"] for message in later.trusted_messages)


def test_checkpoint_response_keeps_legacy_defaults_and_full_invocation_identity():
    old = {
        "model_call_id": "model-1",
        "provider": "fixture",
        "model": "fixture",
        "completed_output": "",
        "tool_calls": [
            {
                "tool_invocation_id": "tool-1",
                "name": "bound.read",
                "arguments": {"x": 1},
            }
        ],
    }
    response = response_from_dict(old)
    call = response.tool_calls[0]
    assert call.version == "1" and call.expected_side_effect == "read"
    assert response.finish_reason == "stop" and response.usage == {} and response.deltas == ()
    full = replace(
        response,
        tool_calls=(
            replace(
                call,
                version="2",
                expected_side_effect="write",
                approval_id="approval-1",
                credential_ref="credential-reference",
                idempotency_key="invocation-identity",
            ),
        ),
        usage={"output_tokens": 7, "cost": 0.01},
        deltas=("delta",),
    )
    assert response_from_dict(response_to_dict(full)) == full
