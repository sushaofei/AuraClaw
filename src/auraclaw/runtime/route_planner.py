from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from auraclaw.contracts.errors import RoutingPlanValidationError
from auraclaw.contracts.routing import (
    PLAN_VERSION,
    PlanAssignment,
    PlanBudget,
    PlanOutputContract,
    PlanRiskClass,
    RouteKind,
    RouteTarget,
    RoutingPlan,
    RoutingPlanStep,
    SemanticPlanProposal,
)
from auraclaw.control.ports import RuntimeAssignment


@dataclass(frozen=True)
class PlanValidation:
    depth: int
    width: int
    step_count: int


class StructuredPlanValidator:
    """Fail-closed validator shared by deterministic and future model planners."""

    def __init__(self, *, max_steps: int = 8, max_depth: int = 4, max_width: int = 8) -> None:
        if min(max_steps, max_depth, max_width) < 1:
            raise ValueError("plan limits must be positive")
        self._max_steps = max_steps
        self._max_depth = max_depth
        self._max_width = max_width

    def validate(self, plan: RoutingPlan, assignment: RuntimeAssignment) -> PlanValidation:
        self._validate_digest(plan)
        if len(plan.steps) > self._max_steps:
            raise RoutingPlanValidationError("routing plan step limit exceeded")
        by_key = {step.task_key: step for step in plan.steps}
        if len(by_key) != len(plan.steps):
            raise RoutingPlanValidationError("routing plan task keys must be unique")
        for step in plan.steps:
            if len(set(step.dependencies)) != len(step.dependencies):
                raise RoutingPlanValidationError("routing plan dependencies must be unique")
            if step.task_key in step.dependencies:
                raise RoutingPlanValidationError("routing plan step cannot depend on itself")
            unknown = sorted(set(step.dependencies) - set(by_key))
            if unknown:
                raise RoutingPlanValidationError(
                    "routing plan references unknown dependencies: " + ", ".join(unknown)
                )
            self._validate_assignment(step, assignment)
        depths = self._depths(by_key)
        depth = max(depths.values(), default=0)
        width = max(
            (
                sum(1 for value in depths.values() if value == level)
                for level in set(depths.values())
            ),
            default=0,
        )
        if depth > self._max_depth:
            raise RoutingPlanValidationError("routing plan depth limit exceeded")
        if width > self._max_width:
            raise RoutingPlanValidationError("routing plan width limit exceeded")
        self._validate_shape(plan, assignment)
        self._validate_budget(plan, assignment)
        return PlanValidation(depth=depth, width=width, step_count=len(plan.steps))

    @staticmethod
    def _validate_digest(plan: RoutingPlan) -> None:
        payload = {
            "plan_version": PLAN_VERSION,
            "route_kind": plan.route_kind.value,
            "success_criteria": list(plan.success_criteria),
            "constraints": list(plan.constraints),
            "risk_class": plan.risk_class.value,
            "steps": [step.model_dump(mode="json") for step in plan.steps],
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        expected = "sha256:" + hashlib.sha256(encoded).hexdigest()
        if plan.plan_digest != expected:
            raise RoutingPlanValidationError("routing plan digest does not match its content")

    @staticmethod
    def _depths(by_key: dict[str, RoutingPlanStep]) -> dict[str, int]:
        depths: dict[str, int] = {}
        visiting: set[str] = set()

        def visit(task_key: str) -> int:
            if task_key in depths:
                return depths[task_key]
            if task_key in visiting:
                raise RoutingPlanValidationError("routing plan dependency graph must be acyclic")
            visiting.add(task_key)
            dependencies = by_key[task_key].dependencies
            value = 1 + max((visit(item) for item in dependencies), default=0)
            visiting.remove(task_key)
            depths[task_key] = value
            return value

        for key in by_key:
            visit(key)
        return depths

    @staticmethod
    def _validate_assignment(step: RoutingPlanStep, assignment: RuntimeAssignment) -> None:
        selected = step.assignment
        if selected.execution_scope == "current" and selected.role != assignment.role:
            raise RoutingPlanValidationError("current-scope plan role must match the assignment")
        if selected.execution_scope == "child" and assignment.role not in {"root", "coordinator"}:
            raise RoutingPlanValidationError("only a coordinator can assign child plan steps")
        allowlists = {
            "profile_id": "agent_profile_ids",
            "model": "allowed_models",
            "harness": "allowed_harnesses",
        }
        for field_name, profile_key in allowlists.items():
            value = getattr(selected, field_name)
            if value is None:
                continue
            allowed = {
                str(item) for item in assignment.resource_profile.get(profile_key, ()) if item
            }
            if value not in allowed:
                raise RoutingPlanValidationError(
                    f"routing plan {field_name} is not in the managed assignment registry"
                )
        if selected.execution_scope == "child" and step.capability is not None:
            allowed_tools = {
                str(item)
                for item in assignment.resource_profile.get("tool_permissions", ())
                if item
            }
            if step.capability.name not in allowed_tools:
                raise RoutingPlanValidationError(
                    "child plan capability exceeds the Root tool permission grant"
                )
        if selected.role == "reviewer" and step.output_contract.result_kind != "review":
            raise RoutingPlanValidationError("reviewer steps require a review output contract")
        if (
            step.capability is not None
            and step.capability.permission not in {None, "read-only"}
            and (
                not step.requires_review
                or step.risk_class not in {PlanRiskClass.HIGH, PlanRiskClass.CRITICAL}
            )
        ):
            raise RoutingPlanValidationError(
                "write-capable plan steps require high risk and an explicit reviewer gate"
            )

    @staticmethod
    def _validate_shape(plan: RoutingPlan, assignment: RuntimeAssignment) -> None:
        child_steps = [
            step for step in plan.steps if step.assignment.execution_scope == "child"
        ]
        if plan.route_kind is RouteKind.SEQUENTIAL_PLAN:
            if child_steps:
                raise RoutingPlanValidationError(
                    "sequential plans must execute in the current Session"
                )
            if any(step.parallel_group is not None for step in plan.steps):
                raise RoutingPlanValidationError(
                    "sequential plans cannot declare parallel groups"
                )
        elif plan.route_kind is RouteKind.COORDINATOR_DAG:
            if assignment.role not in {"root", "coordinator"} or not child_steps:
                raise RoutingPlanValidationError(
                    "coordinator DAG plans require managed child assignments"
                )
        if any(step.requires_review for step in plan.steps) and not any(
            step.assignment.role == "reviewer" for step in plan.steps
        ):
            raise RoutingPlanValidationError("review-gated plans require a reviewer step")

    @staticmethod
    def _validate_budget(plan: RoutingPlan, assignment: RuntimeAssignment) -> None:
        if sum(step.budget.fraction for step in plan.steps) > 1.000001:
            raise RoutingPlanValidationError("routing plan budget fractions exceed one")
        steps = [step.budget.max_steps for step in plan.steps]
        assigned_steps = [int(value) for value in steps if value is not None]
        if len(assigned_steps) == len(steps) and sum(assigned_steps) > int(
            assignment.budget.max_steps
        ):
            raise RoutingPlanValidationError("routing plan step budget exceeds the Run budget")
        tokens = [step.budget.max_output_tokens for step in plan.steps]
        assigned_tokens = [int(value) for value in tokens if value is not None]
        if len(assigned_tokens) == len(tokens) and sum(assigned_tokens) > int(
            assignment.budget.max_output_tokens
        ):
            raise RoutingPlanValidationError("routing plan output budget exceeds the Run budget")
        if assignment.budget.max_cost is not None:
            costs = [step.budget.max_cost for step in plan.steps]
            total = sum(float(value) for value in costs if value is not None)
            if any(value is None for value in costs) or total > float(
                assignment.budget.max_cost
            ):
                raise RoutingPlanValidationError("routing plan cost budget exceeds the Run budget")


def compile_semantic_plan(
    proposal: SemanticPlanProposal,
    *,
    targets: dict[str, RouteTarget],
) -> RoutingPlan:
    """Bind an untrusted proposal only to targets resolved by the governed Runtime."""
    steps: list[RoutingPlanStep] = []
    for proposed in proposal.steps:
        target = None
        if proposed.capability_name is not None:
            target = targets.get(proposed.capability_name)
            if target is None:
                raise RoutingPlanValidationError(
                    "semantic planner selected a capability outside the governed candidates"
                )
        steps.append(
            RoutingPlanStep(
                task_key=proposed.task_key,
                goal=proposed.goal,
                dependencies=proposed.dependencies,
                input_refs=proposed.input_refs,
                output_contract=proposed.output_contract,
                assignment=proposed.assignment,
                budget=proposed.budget,
                capability=target,
                parallel_group=proposed.parallel_group,
                risk_class=proposed.risk_class,
                requires_review=proposed.requires_review,
            )
        )
    return RoutingPlan.create(
        route_kind=proposal.route_kind,
        success_criteria=proposal.success_criteria,
        constraints=proposal.constraints,
        risk_class=proposal.risk_class,
        steps=tuple(steps),
    )


def build_read_only_sequential_plan(
    assignment: RuntimeAssignment,
    targets: tuple[RouteTarget, ...],
) -> RoutingPlan:
    """Compile exact governed read-only targets into a bounded current-Session plan."""
    if not 1 < len(targets) <= 8:
        raise RoutingPlanValidationError("read-only sequential plan requires 2-8 targets")
    fraction = 1.0 / len(targets)
    steps: list[RoutingPlanStep] = []
    for index, target in enumerate(targets, start=1):
        task_key = f"step-{index:02d}-" + target.name.replace("_", "-").replace(".", "-")
        steps.append(
            RoutingPlanStep(
                task_key=task_key[:128],
                goal=f"Use governed capability {target.name} for its part of the root intent.",
                dependencies=((steps[-1].task_key,) if steps else ()),
                output_contract=PlanOutputContract(
                    result_kind="tool_result",
                    required_fields=("status",),
                ),
                assignment=PlanAssignment(
                    execution_scope="current",
                    role=assignment.role,
                ),
                budget=PlanBudget(
                    fraction=fraction,
                    max_steps=max(1, assignment.budget.max_steps // len(targets)),
                    max_output_tokens=max(
                        1, assignment.budget.max_output_tokens // len(targets)
                    ),
                    max_cost=(
                        assignment.budget.max_cost * fraction
                        if assignment.budget.max_cost is not None
                        else None
                    ),
                ),
                capability=target,
                risk_class=PlanRiskClass.LOW,
            )
        )
    return RoutingPlan.create(
        route_kind=RouteKind.SEQUENTIAL_PLAN,
        success_criteria=tuple(
            f"{target.name} returns a governed successful result" for target in targets
        ),
        constraints=(
            "recheck_policy_and_binding_at_execution",
            "do_not_create_child_sessions",
            "read_only_capabilities_only",
        ),
        risk_class=PlanRiskClass.LOW,
        steps=tuple(steps),
    )
