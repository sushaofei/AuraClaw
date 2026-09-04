from decimal import Decimal
from types import SimpleNamespace

import pytest

from auraclaw.contracts.errors import (
    CollaborationValidationError,
    RuntimeCostReservationUnavailableError,
    RuntimeOutputTokenBudgetExceededError,
    RuntimeStepBudgetExceededError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.domain.runtime_budget import govern, usage
from auraclaw.model_gateway.pricing import quote, settled_cost
from auraclaw.projection.task.projector import InMemoryTaskProjection


def fact(kind, payload, run="r", session="s"):
    return SimpleNamespace(type=kind, payload=payload, run_id=run, session_id=session)


def reserve(key, kind="model", tokens=5):
    return NewEvent(
        type="runtime.budget.reserved",
        payload={"reservation_id": key, "kind": kind, "output_tokens": tokens},
    )


def admit(events, proposed, session="s", run="r"):
    return govern(events, proposed, session_id=session, root_session_id="s", run_id=run)


def test_unknown_output_retains_reservation_and_replay_is_idempotent():
    events = [
        fact(
            "run.requested",
            {
                "run_id": "r",
                "budget": {"policy_version": "2", "max_steps": 3, "max_output_tokens": 6},
            },
        )
    ]
    reserved = reserve("m")
    events.append(fact(reserved.type, reserved.payload))
    events.append(fact("model.turn.completed", {"model_call_id": "m", "usage": {}}))
    assert usage(events, "r")["output_tokens_reserved"] == 5
    assert admit(events, [reserved]) == [reserved]
    with pytest.raises(RuntimeOutputTokenBudgetExceededError):
        admit(events, [reserve("m2", tokens=2)])
    events[-1] = fact("model.turn.completed", {"model_call_id": "m", "usage": {"output_tokens": 2}})
    assert usage(events, "r")["output_tokens_reserved"] == 0
    admit(events, [reserve("m2", tokens=4)])


def test_batch_reservations_cannot_overspend_steps():
    events = [
        fact(
            "run.requested",
            {
                "run_id": "r",
                "budget": {"policy_version": "2", "max_steps": 1, "max_output_tokens": 6},
            },
        )
    ]
    with pytest.raises(RuntimeStepBudgetExceededError):
        admit(events, [reserve("a", "tool", 0), reserve("b", "tool", 0)])


def test_child_cannot_forge_scope_or_bypass_tree_with_another_run():
    events = [
        fact(
            "run.requested",
            {
                "run_id": "r",
                "budget": {
                    "policy_version": "2",
                    "max_steps": 4,
                    "max_output_tokens": 10,
                    "tree_max_steps": 8,
                    "tree_max_output_tokens": 20,
                },
            },
        )
    ]
    proposed = [
        NewEvent(
            type="child.created",
            payload={
                "parent_session_id": "s",
                "runtime_budget": {"max_steps": 4, "max_output_tokens": 10, "scope_id": "forged"},
            },
        ),
        NewEvent(type="run.requested", payload={"run_id": "c1"}),
    ]
    child = admit(events, proposed, session="child", run="c1")
    assert child[1].payload["budget"]["scope_id"] == "r"
    events.extend(fact(e.type, e.payload, "c1", "child") for e in child)
    with pytest.raises(CollaborationValidationError):
        admit(
            events,
            [
                NewEvent(
                    type="run.requested",
                    payload={"run_id": "c2", "budget": {"max_steps": 1, "max_output_tokens": 1}},
                )
            ],
            session="child",
            run="c2",
        )


PROFILE = {
    "provider": "fixture",
    "model": "fixture-model",
    "currency": "TEST",
    "version": "v1",
    "max_input_tokens": 8192,
    "input_per_million": "1",
    "output_per_million": "2",
}


def test_trusted_price_uses_decimal_and_rejects_unknown_consumption():
    reservation = quote(PROFILE, [], [], 100, 1)
    assert Decimal(reservation["reserved"]) == Decimal("0.008392")
    assert settled_cost(
        reservation,
        {"input_tokens": 10, "output_tokens": 20},
        provider="fixture",
        model="fixture-model",
    ) == Decimal("0.00005")
    with pytest.raises(RuntimeCostReservationUnavailableError):
        settled_cost(reservation, {"output_tokens": 20})
    with pytest.raises(RuntimeCostReservationUnavailableError):
        settled_cost(
            reservation,
            {"input_tokens": 10, "output_tokens": 20},
            provider="other",
            model="fixture-model",
        )
    assert (
        quote({**PROFILE, "input_per_million": 0, "output_per_million": 0}, [], [], 5, 1)[
            "reserved"
        ]
        == "0"
    )


def test_projection_rebuild_deduplicates_and_omits_business_output():
    view = {}
    InMemoryTaskProjection._apply(
        view, fact("run.requested", {"run_id": "r", "budget": {"max_steps": 4}})
    )
    event = reserve("m")
    InMemoryTaskProjection._apply(view, fact(event.type, event.payload))
    InMemoryTaskProjection._apply(view, fact(event.type, event.payload))
    InMemoryTaskProjection._apply(
        view,
        fact(
            "model.turn.completed",
            {
                "model_call_id": "m",
                "usage": {"output_tokens": 2},
                "output": "secret business output",
            },
        ),
    )
    assert view["runtime_budget"]["usage"]["steps_used"] == 1
    assert view["runtime_budget"]["usage"]["output_tokens_reserved"] == 0
    assert "secret business output" not in str(view)
