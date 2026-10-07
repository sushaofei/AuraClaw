from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any, Literal

from pydantic import Field, model_validator

from auraclaw.contracts.internal import ContractModel

ROUTER_VERSION = "semantic-submit-v6"
PLAN_VERSION = "bounded-plan-v1"


class RouterMode(StrEnum):
    OFF = "off"
    SHADOW = "shadow"
    ASSIST = "assist"
    ENFORCE = "enforce"


class RouteKind(StrEnum):
    DIRECT_ANSWER = "direct_answer"
    SINGLE_CAPABILITY = "single_capability"
    SEQUENTIAL_PLAN = "sequential_plan"
    COORDINATOR_DAG = "coordinator_dag"
    CLARIFY = "clarify"
    FALLBACK = "fallback"


class RouteOutcome(StrEnum):
    DISABLED = "disabled"
    SHADOWED = "shadowed"
    ADOPTED = "adopted"
    FALLBACK = "fallback"


class PlanRiskClass(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RouteTarget(ContractModel):
    kind: str
    name: str = Field(min_length=1, max_length=256)
    permission: str | None = Field(default=None, min_length=1, max_length=64)
    publisher: str | None = Field(default=None, min_length=1, max_length=256)
    version: str = Field(default="*", min_length=1, max_length=128)
    capability_id: str | None = Field(default=None, min_length=1, max_length=256)
    binding_id: str | None = Field(default=None, min_length=1, max_length=256)
    server_id: str | None = Field(default=None, min_length=1, max_length=128)
    config_revision: int | None = Field(default=None, ge=0)
    content_digest: str | None = Field(default=None, min_length=1, max_length=256)


class PlanOutputContract(ContractModel):
    result_kind: Literal["tool_result", "child_result", "review"]
    required_fields: tuple[str, ...] = Field(default=(), max_length=16)
    require_artifacts: bool = False
    require_evidence: bool = False

    @model_validator(mode="after")
    def validate_fields(self) -> PlanOutputContract:
        if len(set(self.required_fields)) != len(self.required_fields):
            raise ValueError("plan output fields must be unique")
        if any(
            not field_name
            or len(field_name) > 64
            or not field_name.replace("_", "").isalnum()
            for field_name in self.required_fields
        ):
            raise ValueError("plan output fields must be bounded identifiers")
        return self


class PlanAssignment(ContractModel):
    execution_scope: Literal["current", "child"]
    role: Literal["root", "coordinator", "worker", "reviewer", "repair"]
    profile_id: str | None = Field(default=None, min_length=1, max_length=128)
    model: str | None = Field(default=None, min_length=1, max_length=128)
    harness: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def validate_scope(self) -> PlanAssignment:
        if self.execution_scope == "child" and self.role in {"root", "coordinator"}:
            raise ValueError("child plan steps require a child role")
        return self


class PlanBudget(ContractModel):
    fraction: float = Field(gt=0.0, le=1.0)
    max_steps: int | None = Field(default=None, ge=1)
    max_output_tokens: int | None = Field(default=None, ge=1)
    max_cost: float | None = Field(default=None, gt=0.0)


class RoutingPlanStep(ContractModel):
    task_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    goal: str = Field(min_length=1, max_length=4_000)
    dependencies: tuple[str, ...] = Field(default=(), max_length=32)
    input_refs: tuple[str, ...] = Field(default=(), max_length=32)
    output_contract: PlanOutputContract
    assignment: PlanAssignment
    budget: PlanBudget
    capability: RouteTarget | None = None
    parallel_group: str | None = Field(default=None, min_length=1, max_length=128)
    risk_class: PlanRiskClass = PlanRiskClass.LOW
    requires_review: bool = False


class SemanticPlanStep(ContractModel):
    """Untrusted planner proposal before governed capability binding."""

    task_key: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9][a-z0-9._-]*$",
    )
    goal: str = Field(min_length=1, max_length=4_000)
    dependencies: tuple[str, ...] = Field(default=(), max_length=32)
    input_refs: tuple[str, ...] = Field(default=(), max_length=32)
    output_contract: PlanOutputContract
    assignment: PlanAssignment
    budget: PlanBudget
    capability_name: str = Field(min_length=1, max_length=256)
    parallel_group: str | None = Field(default=None, min_length=1, max_length=128)
    risk_class: PlanRiskClass = PlanRiskClass.LOW
    requires_review: bool = False


class SemanticPlanProposal(ContractModel):
    """Schema-constrained model output; it never carries authority identifiers."""

    route_kind: Literal[RouteKind.SEQUENTIAL_PLAN, RouteKind.COORDINATOR_DAG]
    success_criteria: tuple[str, ...] = Field(min_length=1, max_length=16)
    constraints: tuple[str, ...] = Field(default=(), max_length=32)
    risk_class: PlanRiskClass
    steps: tuple[SemanticPlanStep, ...] = Field(min_length=1, max_length=8)


class RoutingPlan(ContractModel):
    plan_version: str = PLAN_VERSION
    route_kind: Literal[RouteKind.SEQUENTIAL_PLAN, RouteKind.COORDINATOR_DAG]
    success_criteria: tuple[str, ...] = Field(min_length=1, max_length=16)
    constraints: tuple[str, ...] = Field(default=(), max_length=32)
    risk_class: PlanRiskClass
    steps: tuple[RoutingPlanStep, ...] = Field(min_length=1, max_length=32)
    plan_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @classmethod
    def create(
        cls,
        *,
        route_kind: Literal[RouteKind.SEQUENTIAL_PLAN, RouteKind.COORDINATOR_DAG],
        success_criteria: tuple[str, ...],
        constraints: tuple[str, ...] = (),
        risk_class: PlanRiskClass,
        steps: tuple[RoutingPlanStep, ...],
    ) -> RoutingPlan:
        payload = {
            "plan_version": PLAN_VERSION,
            "route_kind": route_kind.value,
            "success_criteria": list(success_criteria),
            "constraints": list(constraints),
            "risk_class": risk_class.value,
            "steps": [step.model_dump(mode="json") for step in steps],
        }
        return cls(**payload, plan_digest=_digest(payload))


class RoutingRequest(ContractModel):
    intent: str = Field(max_length=100_000)
    explicit_capability_refs: tuple[str, ...] = Field(default=(), max_length=8)
    trusted_skill_names: tuple[str, ...] = Field(default=(), max_length=64)
    role: str = Field(min_length=1, max_length=64)
    budget_policy_version: str = Field(min_length=1, max_length=64)

    @property
    def intent_digest(self) -> str:
        return _digest(self.intent)


class RoutingDecision(ContractModel):
    router_version: str = ROUTER_VERSION
    mode: RouterMode
    route_kind: RouteKind
    outcome: RouteOutcome
    confidence: float = Field(ge=0.0, le=1.0)
    reason_code: str = Field(min_length=1, max_length=128)
    intent_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    decision_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    target: RouteTarget | None = None
    plan: RoutingPlan | None = None
    evidence: tuple[str, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        mode: RouterMode,
        route_kind: RouteKind,
        outcome: RouteOutcome,
        confidence: float,
        reason_code: str,
        intent: str,
        target: RouteTarget | None = None,
        plan: RoutingPlan | None = None,
        evidence: tuple[str, ...] = (),
    ) -> RoutingDecision:
        intent_digest = _digest(intent)
        payload: dict[str, Any] = {
            "router_version": ROUTER_VERSION,
            "mode": mode.value,
            "route_kind": route_kind.value,
            "outcome": outcome.value,
            "confidence": confidence,
            "reason_code": reason_code,
            "intent_digest": intent_digest,
            "target": target.model_dump(mode="json") if target is not None else None,
            "plan": plan.model_dump(mode="json") if plan is not None else None,
            "evidence": list(evidence),
        }
        return cls(
            **payload,
            decision_digest=_digest(payload),
        )


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()
