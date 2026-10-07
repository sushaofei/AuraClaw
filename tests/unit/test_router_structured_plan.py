from __future__ import annotations

from datetime import UTC, datetime

import pytest

from auraclaw.contracts.errors import RoutingPlanValidationError
from auraclaw.contracts.routing import (
    PlanAssignment,
    PlanBudget,
    PlanOutputContract,
    PlanRiskClass,
    RouteKind,
    RouteTarget,
    RoutingPlan,
    RoutingPlanStep,
)
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
from auraclaw.runtime.route_planner import (
    StructuredPlanValidator,
    build_read_only_sequential_plan,
)


def _assignment(**resource_profile: object) -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant",
        root_session_id="root",
        session_id="root",
        run_id="run",
        runtime_id="runtime",
        lease_id="lease",
        fencing_token=1,
        role="root",
        resource_profile=dict(resource_profile),
        deadline=datetime.now(UTC),
        budget=RuntimeBudget(max_steps=8, max_output_tokens=4096, max_cost=4.0),
    )


def _target(name: str, index: int) -> RouteTarget:
    return RouteTarget(
        kind="tool",
        name=name,
        version="1.0.0",
        capability_id=f"cap-{index}",
        content_digest=f"sha256:{str(index) * 64}",
    )


def _step(
    task_key: str,
    *,
    dependencies: tuple[str, ...] = (),
    fraction: float = 0.5,
    assignment: PlanAssignment | None = None,
) -> RoutingPlanStep:
    return RoutingPlanStep(
        task_key=task_key,
        goal=f"complete {task_key}",
        dependencies=dependencies,
        output_contract=PlanOutputContract(
            result_kind="tool_result",
            required_fields=("status",),
        ),
        assignment=assignment
        or PlanAssignment(execution_scope="current", role="root"),
        budget=PlanBudget(fraction=fraction),
        risk_class=PlanRiskClass.LOW,
    )


def test_read_only_plan_is_stable_bounded_and_acyclic() -> None:
    assignment = _assignment()
    targets = (_target("system.time.now", 1), _target("inventory.insight.query", 2))

    first = build_read_only_sequential_plan(assignment, targets)
    second = build_read_only_sequential_plan(assignment, targets)
    validation = StructuredPlanValidator().validate(first, assignment)

    assert first.plan_digest == second.plan_digest
    assert validation.step_count == 2
    assert validation.depth == 2
    assert validation.width == 1
    assert first.steps[1].dependencies == (first.steps[0].task_key,)


def test_plan_validator_rejects_cycles() -> None:
    plan = RoutingPlan.create(
        route_kind=RouteKind.SEQUENTIAL_PLAN,
        success_criteria=("complete",),
        risk_class=PlanRiskClass.LOW,
        steps=(
            _step("one", dependencies=("two",)),
            _step("two", dependencies=("one",)),
        ),
    )

    with pytest.raises(RoutingPlanValidationError, match="acyclic"):
        StructuredPlanValidator().validate(plan, _assignment())


def test_plan_validator_rejects_unmanaged_profile_assignment() -> None:
    plan = RoutingPlan.create(
        route_kind=RouteKind.SEQUENTIAL_PLAN,
        success_criteria=("complete",),
        risk_class=PlanRiskClass.LOW,
        steps=(
            _step(
                "one",
                fraction=1.0,
                assignment=PlanAssignment(
                    execution_scope="current",
                    role="root",
                    profile_id="invented-profile",
                ),
            ),
        ),
    )

    with pytest.raises(RoutingPlanValidationError, match="managed assignment registry"):
        StructuredPlanValidator().validate(
            plan,
            _assignment(agent_profile_ids=["managed-profile"]),
        )


def test_plan_validator_rejects_budget_expansion() -> None:
    plan = RoutingPlan.create(
        route_kind=RouteKind.SEQUENTIAL_PLAN,
        success_criteria=("complete",),
        risk_class=PlanRiskClass.LOW,
        steps=(
            _step("one", fraction=0.6),
            _step("two", dependencies=("one",), fraction=0.6),
        ),
    )

    with pytest.raises(RoutingPlanValidationError, match="fractions exceed"):
        StructuredPlanValidator().validate(plan, _assignment())


def test_plan_validator_rejects_child_capability_outside_root_grant() -> None:
    child = _step(
        "child",
        fraction=1.0,
        assignment=PlanAssignment(execution_scope="child", role="worker"),
    ).model_copy(update={"capability": _target("inventory.insight.query", 2)})
    plan = RoutingPlan.create(
        route_kind=RouteKind.COORDINATOR_DAG,
        success_criteria=("complete",),
        risk_class=PlanRiskClass.LOW,
        steps=(child,),
    )

    with pytest.raises(RoutingPlanValidationError, match="Root tool permission"):
        StructuredPlanValidator().validate(
            plan,
            _assignment(tool_permissions=["system.time.now"]),
        )
