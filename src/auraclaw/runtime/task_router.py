from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Literal

from auraclaw.contracts.errors import AuraClawError, RoutingPlanValidationError
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.routing import (
    RouteKind,
    RouteOutcome,
    RouterMode,
    RouteTarget,
    RoutingDecision,
    RoutingPlan,
    RoutingRequest,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.route_planner import (
    StructuredPlanValidator,
    build_read_only_sequential_plan,
    compile_semantic_plan,
)
from auraclaw.runtime.semantic_planner import (
    SUBMIT_PLAN_TOOL,
    ModelReservation,
    SemanticPlanOutputError,
    SemanticPlanProposer,
)

_CANONICAL_REF = re.compile(r"(?<![\w/@-])([a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)+)(?![\w@/-])")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RoutingExecution:
    decision: RoutingDecision
    capability_state: dict[str, Any]
    events: tuple[NewEvent, ...] = ()
    metrics: dict[str, float] | None = None
    planner_usage: dict[str, int | float] | None = None


class RuntimeTaskRouter:
    """Conservative pre-model router for exact, governed capability references."""

    def __init__(
        self,
        controller: RuntimeCapabilityController,
        *,
        mode: RouterMode | str = RouterMode.ASSIST,
        plan_validator: StructuredPlanValidator | None = None,
        semantic_planner: SemanticPlanProposer | None = None,
        semantic_planner_mode: Literal["off", "shadow", "submit"] = "shadow",
    ) -> None:
        self._controller = controller
        self._mode = RouterMode(mode)
        self._plan_validator = plan_validator or StructuredPlanValidator()
        self._semantic_planner = semantic_planner
        self._semantic_planner_mode = semantic_planner_mode

    @property
    def mode(self) -> RouterMode:
        return self._mode

    async def route(
        self,
        assignment: RuntimeAssignment,
        events: list[Any],
        capability_state: dict[str, Any],
        *,
        reserve_model: ModelReservation | None = None,
    ) -> RoutingExecution:
        started = time.monotonic()
        request, payload = self._normalize(assignment, events)
        base_metrics = {"router.requests.count": 1.0}
        if self._mode is RouterMode.OFF:
            return self._decision(
                request,
                capability_state,
                base_metrics,
                started,
                route_kind=RouteKind.FALLBACK,
                outcome=RouteOutcome.DISABLED,
                confidence=0.0,
                reason_code="router_disabled",
            )

        selected_skills = tuple(
            name
            for name in request.trusted_skill_names
            if self._mentions_exact(request.intent, name)
            or self._mentions_exact(request.intent, self._skill_identity(name)[1])
        )
        explicit_input = "skill_input" in payload
        if explicit_input and not selected_skills:
            if len(request.trusted_skill_names) == 1:
                selected_skills = request.trusted_skill_names
            elif request.trusted_skill_names:
                return self._decision(
                    request,
                    capability_state,
                    base_metrics,
                    started,
                    route_kind=RouteKind.FALLBACK,
                    outcome=RouteOutcome.FALLBACK,
                    confidence=0.0,
                    reason_code="ambiguous_trusted_skill_hints",
                    evidence=("trusted_task_scope", "explicit_skill_input"),
                    candidate_count=len(request.trusted_skill_names),
                )
        if len(selected_skills) > 1:
            fallback = self._decision(
                request,
                capability_state,
                base_metrics,
                started,
                route_kind=RouteKind.SEQUENTIAL_PLAN,
                outcome=RouteOutcome.FALLBACK,
                confidence=0.0,
                reason_code="multiple_explicit_skills_need_planner",
                evidence=("trusted_task_scope", "multiple_exact_skill_references"),
                candidate_count=len(selected_skills),
            )
            return await self._shadow_semantic_plan(
                assignment,
                request,
                capability_state,
                base_metrics,
                started,
                candidate_names=selected_skills,
                fallback=fallback,
                reserve_model=reserve_model,
            )
        if len(selected_skills) == 1:
            return await self._route_skill(
                assignment,
                request,
                payload,
                capability_state,
                base_metrics,
                started,
                selected_skills[0],
                explicit_input=explicit_input,
            )

        refs = request.explicit_capability_refs
        if len(refs) > 1:
            return await self._route_explicit_plan(
                assignment,
                request,
                capability_state,
                base_metrics,
                started,
                reserve_model=reserve_model,
            )
        if len(refs) == 1:
            return await self._route_explicit_capability(
                assignment,
                request,
                capability_state,
                base_metrics,
                started,
                refs[0],
            )

        reason = (
            "ambiguous_trusted_skill_hints"
            if len(request.trusted_skill_names) > 1
            else (
                "trusted_skill_not_selected_by_intent"
                if request.trusted_skill_names
                else "no_explicit_capability_reference"
            )
        )
        return self._decision(
            request,
            capability_state,
            base_metrics,
            started,
            route_kind=RouteKind.FALLBACK,
            outcome=RouteOutcome.FALLBACK,
            confidence=0.0,
            reason_code=reason,
            evidence=(
                ("trusted_task_scope", "skill_scope_is_allowlist")
                if request.trusted_skill_names
                else ("normalized_intent",)
            ),
        )

    async def _route_skill(
        self,
        assignment: RuntimeAssignment,
        request: RoutingRequest,
        payload: dict[str, Any],
        capability_state: dict[str, Any],
        metrics: dict[str, float],
        started: float,
        skill_name: str,
        *,
        explicit_input: bool,
    ) -> RoutingExecution:
        publisher, name = self._skill_identity(skill_name)
        required_skill = next(
            (
                item
                for item in assignment.resource_profile.get("required_skills", ())
                if isinstance(item, dict)
                and item.get("name") == name
                and (publisher is None or item.get("publisher") == publisher)
            ),
            None,
        )
        required_version = (
            str(required_skill.get("version") or "*")
            if isinstance(required_skill, dict)
            else "*"
        )
        target = RouteTarget(
            kind="skill",
            publisher=publisher,
            name=name,
            version=required_version,
            binding_id=(
                str(required_skill["binding_id"])
                if isinstance(required_skill, dict) and required_skill.get("binding_id")
                else None
            ),
            content_digest=(
                str(required_skill["package_digest"])
                if isinstance(required_skill, dict) and required_skill.get("package_digest")
                else None
            ),
        )
        evidence = (
            "trusted_task_scope",
            "explicit_skill_input" if explicit_input else "exact_skill_reference",
            "policy_rechecked_at_activation",
        )
        if self._mode is RouterMode.SHADOW:
            return self._decision(
                request,
                capability_state,
                {**metrics, "router.shadow.count": 1.0},
                started,
                route_kind=RouteKind.SINGLE_CAPABILITY,
                outcome=RouteOutcome.SHADOWED,
                confidence=1.0,
                reason_code="unique_trusted_skill_intent_match",
                target=target,
                evidence=evidence,
                candidate_count=1,
            )
        skill_input = payload.get("skill_input", {})
        if not isinstance(skill_input, dict):
            return self._decision(
                request,
                capability_state,
                metrics,
                started,
                route_kind=RouteKind.FALLBACK,
                outcome=RouteOutcome.FALLBACK,
                confidence=0.0,
                reason_code="trusted_skill_input_invalid",
                target=target,
                evidence=("trusted_task_scope",),
                candidate_count=1,
            )
        activation = await self._controller.activate_trusted_skill(
            assignment,
            capability_state,
            publisher=publisher,
            name=name,
            inputs=dict(skill_input),
            activation_key=f"router:{assignment.run_id}:{name}",
            version=required_version,
            activation_source="agent_router",
        )
        if activation.result.get("status") != "activated":
            return self._decision(
                request,
                capability_state,
                metrics,
                started,
                route_kind=RouteKind.FALLBACK,
                outcome=RouteOutcome.FALLBACK,
                confidence=0.0,
                reason_code="trusted_skill_activation_failed",
                target=target,
                evidence=("trusted_task_scope", "activation_failed_closed"),
                candidate_count=1,
            )
        if isinstance(required_skill, dict):
            active_binding = next(
                (
                    item.get("binding")
                    for item in activation.state.get("active_skills", ())
                    if isinstance(item, dict)
                    and isinstance(item.get("binding"), dict)
                    and item["binding"].get("skill_name") == name
                    and (publisher is None or item["binding"].get("publisher") == publisher)
                ),
                None,
            )
            if (
                not isinstance(active_binding, dict)
                or active_binding.get("skill_version") != required_version
                or active_binding.get("package_digest") != required_skill.get("package_digest")
            ):
                return self._decision(
                    request,
                    capability_state,
                    metrics,
                    started,
                    route_kind=RouteKind.FALLBACK,
                    outcome=RouteOutcome.FALLBACK,
                    confidence=0.0,
                    reason_code="required_skill_binding_mismatch",
                    target=target,
                    evidence=("trusted_task_scope", "binding_failed_closed"),
                    candidate_count=1,
                )
        return self._decision(
            request,
            activation.state,
            {**metrics, "router.fast_path.count": 1.0},
            started,
            route_kind=RouteKind.SINGLE_CAPABILITY,
            outcome=RouteOutcome.ADOPTED,
            confidence=1.0,
            reason_code="unique_trusted_skill_intent_match",
            target=target,
            evidence=evidence,
            candidate_count=1,
            events=activation.events,
        )

    async def _route_explicit_capability(
        self,
        assignment: RuntimeAssignment,
        request: RoutingRequest,
        capability_state: dict[str, Any],
        metrics: dict[str, float],
        started: float,
        canonical_name: str,
    ) -> RoutingExecution:
        prepared = await self._controller.prepare_explicit_capability(
            assignment,
            capability_state,
            canonical_name=canonical_name,
        )
        candidate_count = int(prepared.result.get("candidate_count", 0))
        candidate = prepared.result.get("candidate")
        if prepared.result.get("status") not in {"prepared", "activated"} or not isinstance(
            candidate, dict
        ):
            return self._decision(
                request,
                prepared.state,
                metrics,
                started,
                route_kind=RouteKind.FALLBACK,
                outcome=RouteOutcome.FALLBACK,
                confidence=0.0,
                reason_code=str(
                    prepared.result.get("error_code", "explicit_capability_prepare_failed")
                )[:128],
                evidence=("normalized_intent", "policy_visible_catalog"),
                candidate_count=candidate_count,
            )
        skill = candidate.get("skill")
        publisher = (
            str(skill.get("publisher"))
            if isinstance(skill, dict) and skill.get("publisher")
            else None
        )
        target = RouteTarget(
            kind=str(candidate["kind"]),
            name=str(candidate.get("canonical_name") or canonical_name),
            permission=(str(candidate["permission"]) if candidate.get("permission") else None),
            publisher=publisher,
            version=str(candidate.get("version") or "*"),
            capability_id=str(candidate["capability_id"]),
            server_id=(str(candidate["server_id"]) if candidate.get("server_id") else None),
            config_revision=(
                int(candidate["config_revision"])
                if candidate.get("config_revision") is not None
                else None
            ),
            content_digest=(
                str(candidate["content_digest"]) if candidate.get("content_digest") else None
            ),
        )
        outcome = RouteOutcome.SHADOWED if self._mode is RouterMode.SHADOW else RouteOutcome.ADOPTED
        return self._decision(
            request,
            capability_state if outcome is RouteOutcome.SHADOWED else prepared.state,
            {
                **metrics,
                (
                    "router.shadow.count"
                    if outcome is RouteOutcome.SHADOWED
                    else "router.fast_path.count"
                ): 1.0,
            },
            started,
            route_kind=RouteKind.SINGLE_CAPABILITY,
            outcome=outcome,
            confidence=1.0,
            reason_code="unique_exact_capability_reference",
            target=target,
            evidence=(
                "normalized_intent",
                "exact_capability_reference",
                "policy_visible_catalog",
                "current_binding_loaded",
            ),
            candidate_count=candidate_count,
            events=() if outcome is RouteOutcome.SHADOWED else prepared.events,
        )

    async def _route_explicit_plan(
        self,
        assignment: RuntimeAssignment,
        request: RoutingRequest,
        capability_state: dict[str, Any],
        metrics: dict[str, float],
        started: float,
        *,
        reserve_model: ModelReservation | None,
    ) -> RoutingExecution:
        current = capability_state
        targets: list[RouteTarget] = []
        for canonical_name in request.explicit_capability_refs:
            inspected = await self._controller.inspect_explicit_capability(
                assignment,
                current,
                canonical_name=canonical_name,
            )
            candidate = inspected.result.get("candidate")
            if inspected.result.get("status") != "prepared" or not isinstance(candidate, dict):
                return self._decision(
                    request,
                    capability_state,
                    metrics,
                    started,
                    route_kind=RouteKind.SEQUENTIAL_PLAN,
                    outcome=RouteOutcome.FALLBACK,
                    confidence=0.0,
                    reason_code=str(
                        inspected.result.get(
                            "error_code", "structured_plan_candidate_prepare_failed"
                        )
                    )[:128],
                    evidence=(
                        "normalized_intent",
                        "multiple_exact_capability_references",
                        "plan_validation_failed_closed",
                    ),
                    candidate_count=len(targets),
                )
            if candidate.get("kind") != "tool" or candidate.get("permission") != "read-only":
                fallback = self._decision(
                    request,
                    capability_state,
                    metrics,
                    started,
                    route_kind=RouteKind.SEQUENTIAL_PLAN,
                    outcome=RouteOutcome.FALLBACK,
                    confidence=0.0,
                    reason_code="structured_plan_requires_read_only_tools",
                    evidence=(
                        "normalized_intent",
                        "current_binding_loaded",
                        "non_read_only_or_skill_requires_semantic_planner",
                    ),
                    candidate_count=len(targets) + 1,
                )
                return await self._shadow_semantic_plan(
                    assignment,
                    request,
                    capability_state,
                    metrics,
                    started,
                    candidate_names=request.explicit_capability_refs,
                    fallback=fallback,
                    reserve_model=reserve_model,
                )
            targets.append(
                RouteTarget(
                    kind="tool",
                    name=str(candidate.get("canonical_name") or canonical_name),
                    permission="read-only",
                    version=str(candidate.get("version") or "*"),
                    capability_id=str(candidate["capability_id"]),
                    server_id=(str(candidate["server_id"]) if candidate.get("server_id") else None),
                    config_revision=(
                        int(candidate["config_revision"])
                        if candidate.get("config_revision") is not None
                        else None
                    ),
                    content_digest=(
                        str(candidate["content_digest"])
                        if candidate.get("content_digest")
                        else None
                    ),
                )
            )
            current = inspected.state
        try:
            plan = build_read_only_sequential_plan(assignment, tuple(targets))
            validation = self._plan_validator.validate(plan, assignment)
        except (ValueError, RoutingPlanValidationError):
            return self._decision(
                request,
                capability_state,
                metrics,
                started,
                route_kind=RouteKind.SEQUENTIAL_PLAN,
                outcome=RouteOutcome.FALLBACK,
                confidence=0.0,
                reason_code="structured_plan_validation_failed",
                evidence=(
                    "normalized_intent",
                    "policy_visible_catalog",
                    "plan_validation_failed_closed",
                ),
                candidate_count=len(targets),
            )
        outcome = RouteOutcome.SHADOWED if self._mode is RouterMode.SHADOW else RouteOutcome.ADOPTED
        execution = self._decision(
            request,
            capability_state if outcome is RouteOutcome.SHADOWED else current,
            {
                **metrics,
                (
                    "router.shadow.count"
                    if outcome is RouteOutcome.SHADOWED
                    else "router.fast_path.count"
                ): 1.0,
                "router.plan.steps": float(validation.step_count),
                "router.plan.depth": float(validation.depth),
                "router.plan.width": float(validation.width),
            },
            started,
            route_kind=RouteKind.SEQUENTIAL_PLAN,
            outcome=outcome,
            confidence=1.0,
            reason_code="validated_exact_read_only_plan",
            plan=plan,
            evidence=(
                "normalized_intent",
                "multiple_exact_capability_references",
                "policy_visible_catalog",
                "current_bindings_loaded",
                "bounded_plan_validated",
            ),
            candidate_count=len(targets),
        )
        if self._semantic_planner is None or self._semantic_planner_mode == "off":
            return execution
        return await self._shadow_semantic_plan(
            assignment,
            request,
            capability_state,
            metrics,
            started,
            candidate_names=request.explicit_capability_refs,
            fallback=execution,
            reserve_model=reserve_model,
        )

    async def _shadow_semantic_plan(
        self,
        assignment: RuntimeAssignment,
        request: RoutingRequest,
        capability_state: dict[str, Any],
        metrics: dict[str, float],
        started: float,
        *,
        candidate_names: tuple[str, ...],
        fallback: RoutingExecution,
        reserve_model: ModelReservation | None,
    ) -> RoutingExecution:
        if self._semantic_planner is None:
            return fallback
        if assignment.budget.policy_version == "2" and reserve_model is None:
            return fallback
        planner_metrics = {**metrics, "router.semantic_planner.calls": 1.0}
        planner_usage: dict[str, int | float] | None = None
        planner_event: NewEvent | None = None
        planner_purpose = (
            "router_semantic_plan_submit"
            if self._semantic_planner_mode == "submit"
            else "router_semantic_plan_shadow"
        )
        try:
            result = await self._semantic_planner.propose(
                assignment,
                request,
                candidate_names=candidate_names,
                reserve_model=reserve_model,
            )
            planner_metrics["router.semantic_planner.latency.seconds"] = result.latency_seconds
            planner_usage = result.usage
            planner_event = self._semantic_model_event(
                model_call_id=result.model_call_id,
                provider=result.provider,
                model=result.model,
                usage=result.usage,
                accepted=True,
                purpose=planner_purpose,
            )
            target_names = tuple(
                dict.fromkeys(
                    step.capability_name
                    for step in result.proposal.steps
                    if step.capability_name is not None
                )
            )
            allowed_names = set(candidate_names)
            if any(name not in allowed_names for name in target_names):
                raise RoutingPlanValidationError(
                    "semantic planner selected a capability outside the governed candidates"
                )
            targets: dict[str, RouteTarget] = {}
            current = capability_state
            for canonical_name in target_names:
                if canonical_name in request.trusted_skill_names:
                    publisher, skill_name = self._skill_identity(canonical_name)
                    inspected = await self._controller.inspect_trusted_skill(
                        assignment,
                        current,
                        publisher=publisher,
                        name=skill_name,
                    )
                else:
                    inspected = await self._controller.inspect_explicit_capability(
                        assignment,
                        current,
                        canonical_name=canonical_name,
                    )
                candidate = inspected.result.get("candidate")
                if inspected.result.get("status") != "prepared" or not isinstance(candidate, dict):
                    raise RoutingPlanValidationError(
                        "semantic planner capability binding could not be resolved"
                    )
                skill = candidate.get("skill")
                targets[canonical_name] = RouteTarget(
                    kind=str(candidate["kind"]),
                    name=str(candidate.get("canonical_name") or canonical_name),
                    permission=(
                        str(candidate["permission"]) if candidate.get("permission") else None
                    ),
                    publisher=(
                        str(skill["publisher"])
                        if isinstance(skill, dict) and skill.get("publisher")
                        else None
                    ),
                    version=str(candidate.get("version") or "*"),
                    capability_id=(
                        str(candidate["capability_id"])
                        if candidate.get("capability_id")
                        else None
                    ),
                    binding_id=(
                        str(candidate["binding_id"]) if candidate.get("binding_id") else None
                    ),
                    server_id=(str(candidate["server_id"]) if candidate.get("server_id") else None),
                    config_revision=(
                        int(candidate["config_revision"])
                        if candidate.get("config_revision") is not None
                        else None
                    ),
                    content_digest=(
                        str(candidate["content_digest"])
                        if candidate.get("content_digest")
                        else None
                    ),
                )
                current = inspected.state
            plan = compile_semantic_plan(result.proposal, targets=targets)
            validation = self._plan_validator.validate(plan, assignment)
        except (AuraClawError, RuntimeError, ValueError, TypeError, KeyError) as exc:
            if isinstance(exc, SemanticPlanOutputError):
                planner_usage = exc.usage
                planner_metrics["router.semantic_planner.latency.seconds"] = exc.latency_seconds
                planner_event = self._semantic_model_event(
                    model_call_id=exc.model_call_id,
                    provider=exc.provider,
                    model=exc.model,
                    usage=exc.usage,
                    accepted=False,
                    purpose=planner_purpose,
                )
            elif planner_event is not None:
                planner_event = NewEvent(
                    type=planner_event.type,
                    payload={**planner_event.payload, "proposal_accepted": False},
                    visibility=planner_event.visibility,
                )
            logger.info(
                "semantic planner proposal rejected run=%s error=%s",
                assignment.run_id,
                type(exc).__name__,
            )
            rejected_metrics = {
                **dict(fallback.metrics or {}),
                **planner_metrics,
                "router.semantic_planner.rejected.count": 1.0,
                "router.latency.seconds": time.monotonic() - started,
            }
            return RoutingExecution(
                decision=fallback.decision,
                capability_state=fallback.capability_state,
                events=(*fallback.events, *((planner_event,) if planner_event else ())),
                metrics=rejected_metrics,
                planner_usage=planner_usage,
            )
        submit_ineligibility = self._submit_ineligibility_reasons(plan)
        submit = (
            self._semantic_planner_mode == "submit"
            and plan.route_kind == RouteKind.COORDINATOR_DAG
            and assignment.role in {"root", "coordinator"}
            and not submit_ineligibility
        )
        if planner_event is not None:
            gate_reasons = tuple(
                reason
                for reason in (
                    (
                        "semantic_planner_mode_not_submit"
                        if self._semantic_planner_mode != "submit"
                        else None
                    ),
                    (
                        "route_kind_not_coordinator_dag"
                        if plan.route_kind != RouteKind.COORDINATOR_DAG
                        else None
                    ),
                    (
                        "assignment_role_not_coordinator"
                        if assignment.role not in {"root", "coordinator"}
                        else None
                    ),
                    *submit_ineligibility,
                )
                if reason is not None
            )
            planner_event = NewEvent(
                type=planner_event.type,
                payload={
                    **planner_event.payload,
                    "route_kind": plan.route_kind.value,
                    "plan_digest": plan.plan_digest,
                    "submit_eligible": submit,
                    "submit_gate_reasons": list(gate_reasons),
                },
                visibility=planner_event.visibility,
            )
            if gate_reasons and self._semantic_planner_mode == "submit":
                logger.info(
                    "semantic planner submit gate rejected run=%s reasons=%s",
                    assignment.run_id,
                    ",".join(gate_reasons),
                )
        execution = self._decision(
            request,
            capability_state,
            {
                **planner_metrics,
                "router.semantic_planner.accepted.count": 1.0,
                (
                    "router.semantic_planner.submit_candidate.count"
                    if submit
                    else "router.shadow.count"
                ): 1.0,
                "router.plan.steps": float(validation.step_count),
                "router.plan.depth": float(validation.depth),
                "router.plan.width": float(validation.width),
            },
            started,
            route_kind=plan.route_kind,
            outcome=RouteOutcome.ADOPTED if submit else RouteOutcome.SHADOWED,
            confidence=1.0 if submit else 0.0,
            reason_code=(
                "semantic_plan_validated_for_submit"
                if submit
                else "semantic_plan_validated_shadow"
            ),
            plan=plan,
            evidence=(
                "schema_constrained_model_proposal",
                "governed_candidate_allowlist",
                "managed_registry_validation",
                "bounded_plan_validated",
                "atomic_collaboration_submission"
                if submit
                else "shadow_no_child_submission",
            ),
            candidate_count=len(candidate_names),
            events=((planner_event,) if planner_event else ()),
        )
        return RoutingExecution(
            decision=execution.decision,
            capability_state=execution.capability_state,
            events=execution.events,
            metrics=execution.metrics,
            planner_usage=planner_usage,
        )

    @staticmethod
    def _submit_ineligibility_reasons(plan: RoutingPlan) -> tuple[str, ...]:
        """Explain why the first live gate cannot submit a governed child DAG."""
        reasons: list[str] = []
        if plan.risk_class.value not in {"low", "medium"}:
            reasons.append("plan_risk_not_live_safe")
        for step in plan.steps:
            prefix = f"step:{step.task_key}:"
            if step.assignment.execution_scope != "child":
                reasons.append(prefix + "execution_scope_not_child")
            if step.assignment.role != "worker":
                reasons.append(prefix + "role_not_worker")
            if step.requires_review:
                reasons.append(prefix + "review_required")
            if step.risk_class.value not in {"low", "medium"}:
                reasons.append(prefix + "risk_not_live_safe")
            if step.capability is None:
                reasons.append(prefix + "capability_missing")
            elif step.capability.kind == "skill":
                if (
                    step.capability.binding_id is None
                    or step.capability.publisher is None
                    or step.capability.version == "*"
                    or step.capability.content_digest is None
                ):
                    reasons.append(prefix + "skill_not_exact_bound")
            elif step.capability.capability_id is None:
                reasons.append(prefix + "capability_not_exact_bound")
            elif step.capability.permission not in {None, "read-only"}:
                reasons.append(prefix + "capability_not_read_only")
        return tuple(reasons)

    @staticmethod
    def _semantic_model_event(
        *,
        model_call_id: str,
        provider: str,
        model: str,
        usage: dict[str, int | float],
        accepted: bool,
        purpose: str,
    ) -> NewEvent:
        return NewEvent(
            type="model.turn.completed",
            payload={
                "model_call_id": model_call_id,
                "turn_index": -1,
                "output": "",
                "finish_reason": "tool_calls",
                "usage": dict(usage),
                # This is an internal, non-executable proposal call. Keep it out of
                # the canonical executable tool-call shape so replay cannot mistake
                # it for an outstanding capability invocation.
                "proposal_tool": SUBMIT_PLAN_TOOL,
                "purpose": purpose,
                "proposal_accepted": accepted,
            },
        )

    @classmethod
    def _normalize(
        cls, assignment: RuntimeAssignment, events: list[Any]
    ) -> tuple[RoutingRequest, dict[str, Any]]:
        intent, payload = cls._latest_task_scope(events)
        skill_names = tuple(
            dict.fromkeys(
                str(item).strip() for item in payload.get("skill_names", ()) if str(item).strip()
            )
        )
        refs = tuple(dict.fromkeys(match.group(1) for match in _CANONICAL_REF.finditer(intent)))
        return (
            RoutingRequest(
                intent=intent,
                explicit_capability_refs=refs[:8],
                trusted_skill_names=skill_names[:64],
                role=assignment.role,
                budget_policy_version=assignment.budget.policy_version,
            ),
            payload,
        )

    @staticmethod
    def _latest_task_scope(events: list[Any]) -> tuple[str, dict[str, Any]]:
        for event in reversed(events):
            if event.type not in {"session.created", "user.message.appended", "child.created"}:
                continue
            payload = dict(event.payload)
            intent = str(
                payload.get("message")
                or payload.get("goal")
                or payload.get("output_contract")
                or ""
            )
            return intent, payload
        return "", {}

    @staticmethod
    def _mentions_exact(intent: str, value: str) -> bool:
        if not value:
            return False
        return bool(
            re.search(
                rf"(?<![\w.-]){re.escape(value)}(?![\w.-])",
                intent,
                flags=re.IGNORECASE,
            )
        )

    @staticmethod
    def _skill_identity(value: str) -> tuple[str | None, str]:
        if value.count("/") == 1:
            publisher, name = (part.strip() for part in value.split("/", 1))
            if publisher and name:
                return publisher, name
        return None, value

    def _decision(
        self,
        request: RoutingRequest,
        capability_state: dict[str, Any],
        metrics: dict[str, float],
        started: float,
        *,
        route_kind: RouteKind,
        outcome: RouteOutcome,
        confidence: float,
        reason_code: str,
        target: RouteTarget | None = None,
        plan: RoutingPlan | None = None,
        evidence: tuple[str, ...] = (),
        candidate_count: int = 0,
        events: tuple[NewEvent, ...] = (),
    ) -> RoutingExecution:
        decision = RoutingDecision.create(
            mode=self._mode,
            route_kind=route_kind,
            outcome=outcome,
            confidence=confidence,
            reason_code=reason_code,
            intent=request.intent,
            target=target,
            plan=plan,
            evidence=evidence,
        )
        result_metrics = {
            **metrics,
            "router.candidate.count": float(candidate_count),
            "router.confidence": confidence,
            "router.latency.seconds": time.monotonic() - started,
        }
        if outcome is RouteOutcome.FALLBACK:
            result_metrics["router.fallback.count"] = 1.0
        return RoutingExecution(
            decision=decision,
            capability_state=capability_state,
            events=events,
            metrics=result_metrics,
        )
