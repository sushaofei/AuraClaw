"""Pure, rebuildable Runtime reservation and task-tree admission rules."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from auraclaw.contracts.errors import (
    CollaborationValidationError,
    RuntimeOutputTokenBudgetExceededError,
    RuntimeStepBudgetExceededError,
    VersionConflictError,
)
from auraclaw.contracts.events import NewEvent

RESERVATION = "runtime.budget.reserved"
PROGRESS = "runtime.progress.recorded"


def usage(events: Sequence[Any], run_id: str) -> dict[str, Any]:
    reservations = {
        e.payload["reservation_id"]: e.payload
        for e in events
        if e.run_id == run_id and e.type == RESERVATION
    }
    models = {
        e.payload["model_call_id"]: e.payload
        for e in events
        if e.run_id == run_id and e.type == "model.turn.completed"
    }
    tools = {
        e.payload["tool_invocation_id"]: e.payload.get("result", {})
        for e in events
        if e.run_id == run_id and e.type == "tool.call.completed"
    }
    model_ids = {k for k, v in reservations.items() if v["kind"] == "model"} | models.keys()
    tool_ids = {k for k, v in reservations.items() if v["kind"] == "tool"}
    output = sum(int(m.get("usage", {}).get("output_tokens", 0)) for m in models.values())
    outstanding = sum(
        int(v.get("output_tokens", 0))
        for k, v in reservations.items()
        if v["kind"] == "model" and "output_tokens" not in models.get(k, {}).get("usage", {})
    )
    costs = [m.get("usage", {}).get("cost") for m in models.values()]
    return {
        "steps_used": len(model_ids) + len(tool_ids),
        "model_turns": len(model_ids),
        "tool_attempts": len(tool_ids),
        "output_tokens": output,
        "output_tokens_reserved": outstanding,
        "tool_dispatches": sum(
            r.get("metadata", {}).get("dispatch_started") is True for r in tools.values()
        ),
        "cost": sum(float(c) for c in costs)
        if costs and all(c is not None for c in costs)
        else None,
        "suppressed_attempts": sum(
            r.get("error_code") == "tool_repeat_suppressed" for r in tools.values()
        ),
    }


def run_budgets(events: Sequence[Any]) -> dict[str, dict[str, Any]]:
    return {
        str(e.payload["run_id"]): dict(e.payload["budget"])
        for e in events
        if e.type == "run.requested" and isinstance(e.payload.get("budget"), dict)
    }


def govern(
    existing: Sequence[Any],
    proposed: Sequence[NewEvent],
    *,
    session_id: str,
    root_session_id: str,
    run_id: str | None,
) -> list[NewEvent]:
    """Called inside the EventStore's root transaction, before any canonical write."""
    budgets = run_budgets(existing)
    result = list(proposed)
    child = next((e for e in result if e.type == "child.created"), None)
    if (
        child is None
        and session_id != root_session_id
        and any(e.type == "run.requested" for e in result)
    ):
        original = next(
            (e for e in existing if e.session_id == session_id and e.type == "child.created"), None
        )
        if original is not None:
            requested = next(e for e in result if e.type == "run.requested")
            child = NewEvent(
                type="child.created",
                payload={**original.payload, "runtime_budget": requested.payload.get("budget", {})},
            )
    if child is not None:
        parent = str(child.payload.get("parent_session_id", root_session_id))
        parent_run = next(
            (
                e.payload["run_id"]
                for e in reversed(existing)
                if e.session_id == parent and e.type == "run.requested"
            ),
            None,
        )
        inherited = budgets.get(str(parent_run), {})
        if inherited.get("policy_version") == "2":
            configured = dict(child.payload.get("runtime_budget", {}))
            scope = str(inherited.get("scope_id", parent_run))
            root_budget = budgets.get(scope, inherited)
            configured = {
                "max_steps": 48,
                "max_output_tokens": 8192,
                **configured,
                "policy_version": "2",
                "scope_id": scope,
            }
            # A child cannot enlarge the tree or forge its own scope/prices.
            for key in ("tree_max_steps", "tree_max_output_tokens", "tree_max_cost"):
                configured.pop(key, None)
            allocations = [b for k, b in budgets.items() if b.get("scope_id", k) == scope]
            for key, default in (("max_steps", 480), ("max_output_tokens", 81920)):
                value = configured.get(key)
                if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                    raise CollaborationValidationError(f"child {key} must be a positive integer")
                total = (
                    sum(int(b.get(key, 48 if key == "max_steps" else 8192)) for b in allocations)
                    + value
                )
                if total > int(root_budget.get("tree_" + key, default)):
                    raise CollaborationValidationError(f"root task tree {key} allocation exhausted")
            if root_budget.get("tree_max_cost") is not None:
                amount = configured.get("max_cost")
                if (
                    amount is None
                    or not math.isfinite(float(amount))
                    or float(amount) <= 0
                    or any(b.get("max_cost") is None for b in allocations)
                ):
                    raise CollaborationValidationError(
                        "cost-limited tree requires child cost limits"
                    )
                if sum(float(b["max_cost"]) for b in allocations) + float(amount) > float(
                    root_budget["tree_max_cost"]
                ):
                    raise CollaborationValidationError("root task tree cost allocation exhausted")
            result = [
                replace(
                    e,
                    payload={
                        **e.payload,
                        **(
                            {"runtime_budget": configured}
                            if e.type == "child.created"
                            else {"budget": configured}
                            if e.type == "run.requested"
                            else {}
                        ),
                    },
                )
                for e in result
            ]
    budget = budgets.get(str(run_id), {})
    if budget.get("policy_version") != "2":
        return result
    used = usage(existing, str(run_id))
    prior = {
        e.payload["reservation_id"]: e.payload
        for e in existing
        if e.run_id == run_id and e.type == RESERVATION
    }
    for event in result:
        if event.type != RESERVATION:
            continue
        payload = event.payload
        key = str(payload["reservation_id"])
        if key in prior:
            if prior[key] != payload:
                raise VersionConflictError("reservation identity reused with different allocation")
            continue
        if payload.get("kind") not in {"model", "tool"}:
            raise CollaborationValidationError("unsupported reservation kind")
        tokens = payload.get("output_tokens", 0)
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
            raise CollaborationValidationError("invalid token reservation")
        if used["steps_used"] + 1 > int(budget.get("max_steps", 48)):
            raise RuntimeStepBudgetExceededError("persisted step reservations exhausted")
        if used["output_tokens"] + used["output_tokens_reserved"] + tokens > int(
            budget.get("max_output_tokens", 8192)
        ):
            raise RuntimeOutputTokenBudgetExceededError("persisted output reservations exhausted")
        used["steps_used"] += 1
        used["output_tokens_reserved"] += tokens
        prior[key] = payload
    return result
