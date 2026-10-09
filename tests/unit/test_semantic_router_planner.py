from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from auraclaw.contracts.errors import RoutingPlanValidationError
from auraclaw.contracts.routing import (
    RouteKind,
    RouteOutcome,
    RouteTarget,
    RoutingRequest,
    SemanticPlanProposal,
)
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
from auraclaw.runtime.capability_controller import CapabilityExecution
from auraclaw.runtime.execution_engine import RuntimeExecutionEngine
from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall
from auraclaw.runtime.route_planner import StructuredPlanValidator, compile_semantic_plan
from auraclaw.runtime.semantic_planner import SUBMIT_PLAN_TOOL, SemanticPlanProposer
from auraclaw.runtime.task_router import RuntimeTaskRouter


def _assignment(
    *,
    resource_profile: dict[str, Any] | None = None,
    budget_policy_version: str = "1",
) -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-a",
        root_session_id="root-a",
        session_id="session-a",
        run_id="run-a",
        runtime_id="runtime-a",
        lease_id="lease-a",
        fencing_token=1,
        role="root",
        resource_profile=resource_profile or {},
        budget=RuntimeBudget(
            max_steps=12,
            max_output_tokens=1_000,
            policy_version=budget_policy_version,
        ),
    )


def _proposal(*, cycle: bool = False, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "route_kind": "sequential_plan",
        "success_criteria": ["both governed steps succeed"],
        "constraints": ["preserve policy checks"],
        "risk_class": "low",
        "steps": [
            {
                "task_key": "prepare",
                "goal": "Prepare the release with the governed Skill.",
                "dependencies": ["verify"] if cycle else [],
                "input_refs": [],
                "output_contract": {
                    "result_kind": "tool_result",
                    "required_fields": ["status"],
                    "require_artifacts": False,
                    "require_evidence": False,
                },
                "assignment": {"execution_scope": "current", "role": "root"},
                "budget": {
                    "fraction": 0.5,
                    "max_steps": 6,
                    "max_output_tokens": 500,
                },
                "capability_name": "release.prepare",
                "risk_class": "low",
                "requires_review": False,
            },
            {
                "task_key": "verify",
                "goal": "Verify the release with the governed Skill.",
                "dependencies": ["prepare"],
                "input_refs": [],
                "output_contract": {
                    "result_kind": "tool_result",
                    "required_fields": ["status"],
                    "require_artifacts": False,
                    "require_evidence": False,
                },
                "assignment": {"execution_scope": "current", "role": "root"},
                "budget": {
                    "fraction": 0.5,
                    "max_steps": 6,
                    "max_output_tokens": 500,
                },
                "capability_name": "release.verify",
                "risk_class": "low",
                "requires_review": False,
            },
        ],
    }
    payload.update(extra or {})
    return payload


def _dag_proposal(*, role: str = "worker") -> dict[str, Any]:
    payload = _proposal()
    payload["route_kind"] = "coordinator_dag"
    for step in payload["steps"]:
        step["assignment"] = {"execution_scope": "child", "role": role}
        step["output_contract"] = {
            "result_kind": "child_result",
            "required_fields": ["summary", "result_ref"],
            "require_artifacts": False,
            "require_evidence": False,
        }
    return payload


def test_semantic_plan_schema_requires_capability_name_for_every_step() -> None:
    proposal = _dag_proposal()
    del proposal["steps"][0]["capability_name"]

    with pytest.raises(ValueError):
        SemanticPlanProposal.model_validate(proposal)

    schema = SemanticPlanProposal.model_json_schema()
    step_schema = schema["$defs"]["SemanticPlanStep"]
    assert "capability_name" in step_schema["required"]


class _PlannerModel:
    def __init__(self, arguments: dict[str, Any], *, tool_name: str = SUBMIT_PLAN_TOOL) -> None:
        self.arguments = arguments
        self.tool_name = tool_name
        self.requests: list[ModelRequest] = []

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        return ModelResponse(
            model_call_id=request.model_call_id,
            provider="test-provider",
            model="test-model",
            completed_output="",
            tool_calls=(
                ToolCall(
                    tool_invocation_id="proposal-1",
                    name=self.tool_name,
                    arguments=self.arguments,
                ),
            ),
            finish_reason="tool_calls",
            usage={"input_tokens": 100, "output_tokens": 80},
        )


class _SkillController:
    def __init__(self, *, kind: str = "skill") -> None:
        self.kind = kind

    @staticmethod
    def empty_state() -> dict[str, Any]:
        return {"loaded": {}}

    async def inspect_explicit_capability(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        *,
        canonical_name: str,
    ) -> CapabilityExecution:
        del assignment
        capability_id = "cap-" + canonical_name.rsplit(".", 1)[-1]
        return CapabilityExecution(
            result={
                "status": "prepared",
                "candidate_count": 1,
                "candidate": {
                    "capability_id": capability_id,
                    "kind": self.kind,
                    "canonical_name": canonical_name,
                    "version": "1.0.0",
                    "permission": "read-only" if self.kind == "tool" else None,
                    "content_digest": "sha256:" + "a" * 64,
                    **(
                        {
                            "skill": {
                                "publisher": "platform",
                                "name": canonical_name,
                                "version": "1.0.0",
                            }
                        }
                        if self.kind == "skill"
                        else {}
                    ),
                },
            },
            state={"loaded": {capability_id: canonical_name}},
        )

    async def inspect_trusted_skill(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        *,
        publisher: str | None,
        name: str,
        version: str = "*",
    ) -> CapabilityExecution:
        del assignment, version
        canonical_name = f"{publisher}/{name}" if publisher else name
        return CapabilityExecution(
            result={
                "status": "prepared",
                "candidate_count": 1,
                "candidate": {
                    "kind": "skill",
                    "canonical_name": canonical_name,
                    "version": "1.0.0",
                    "binding_id": "skb-test-" + name,
                    "content_digest": "sha256:" + "a" * 64,
                    "skill": {
                        "publisher": publisher or "platform",
                        "name": name,
                        "version": "1.0.0",
                    },
                },
            },
            state=state,
        )


def test_semantic_planner_uses_one_schema_constrained_virtual_tool_call() -> None:
    async def scenario() -> None:
        model = _PlannerModel(_proposal())
        planner = SemanticPlanProposer(model)
        request = RoutingRequest(
            intent="Use release.prepare and release.verify",
            trusted_skill_names=("release.prepare", "release.verify"),
            role="root",
            budget_policy_version="1",
        )

        first = await planner.propose(
            _assignment(), request, candidate_names=request.trusted_skill_names
        )
        second = await planner.propose(
            _assignment(), request, candidate_names=request.trusted_skill_names
        )

        assert first.proposal.route_kind is RouteKind.SEQUENTIAL_PLAN
        assert model.requests[0].model_call_id == model.requests[1].model_call_id
        assert model.requests[0].tools[0]["function"]["name"] == SUBMIT_PLAN_TOOL
        schema = model.requests[0].tools[0]["function"]["parameters"]
        assert schema["additionalProperties"] is False
        assert model.requests[0].policy.capability == "routing_planner"
        planner_system = model.requests[0].messages[0]["content"]
        assert "every step must use child execution_scope" in planner_system
        assert "fractions must sum to at most 1.0" in planner_system
        assert "summary, result_ref, artifact_refs" in planner_system
        assert first.provider == "test-provider"
        assert second.model == "test-model"

    asyncio.run(scenario())


def test_semantic_planner_rejects_authority_fields_and_wrong_tool() -> None:
    async def scenario() -> None:
        request = RoutingRequest(
            intent="Use release.prepare and release.verify",
            trusted_skill_names=("release.prepare", "release.verify"),
            role="root",
            budget_policy_version="1",
        )
        injected = _proposal(extra={"tenant_id": "other-tenant"})
        with pytest.raises(RoutingPlanValidationError):
            await SemanticPlanProposer(_PlannerModel(injected)).propose(
                _assignment(), request, candidate_names=request.trusted_skill_names
            )
        with pytest.raises(RoutingPlanValidationError):
            await SemanticPlanProposer(
                _PlannerModel(_proposal(), tool_name="auraclaw.collaboration.create_child")
            ).propose(_assignment(), request, candidate_names=request.trusted_skill_names)

    asyncio.run(scenario())


def test_task_router_records_valid_semantic_plan_as_shadow_without_state_change() -> None:
    async def scenario() -> None:
        model = _PlannerModel(_proposal())
        controller = _SkillController()
        router = RuntimeTaskRouter(
            controller,  # type: ignore[arg-type]
            semantic_planner=SemanticPlanProposer(model),
        )
        events = [
            SimpleNamespace(
                type="session.created",
                payload={
                    "goal": "Use release.prepare and release.verify for this release",
                    "skill_names": ["release.prepare", "release.verify"],
                },
            )
        ]
        initial = controller.empty_state()

        routed = await router.route(_assignment(), events, initial)

        assert routed.decision.outcome is RouteOutcome.SHADOWED
        assert routed.decision.reason_code == "semantic_plan_validated_shadow"
        assert routed.decision.plan is not None
        assert [step.capability.name for step in routed.decision.plan.steps] == [
            "release.prepare",
            "release.verify",
        ]
        assert routed.capability_state == initial
        assert [event.type for event in routed.events] == ["model.turn.completed"]
        assert routed.events[0].payload["proposal_accepted"] is True
        assert routed.events[0].payload["proposal_tool"] == SUBMIT_PLAN_TOOL
        assert "tool_calls" not in routed.events[0].payload
        assert routed.planner_usage == {"input_tokens": 100, "output_tokens": 80}
        assert routed.metrics is not None
        assert routed.metrics["router.semantic_planner.accepted.count"] == 1.0
        assert routed.metrics["router.plan.steps"] == 2.0

    asyncio.run(scenario())


def test_task_router_adopts_exact_bound_read_only_worker_dag_in_submit_mode() -> None:
    async def scenario() -> None:
        model = _PlannerModel(_dag_proposal())
        controller = _SkillController()
        router = RuntimeTaskRouter(
            controller,  # type: ignore[arg-type]
            semantic_planner=SemanticPlanProposer(model),
            semantic_planner_mode="submit",
        )
        events = [
            SimpleNamespace(
                type="session.created",
                payload={
                    "goal": "Use release.prepare and release.verify for this release",
                    "skill_names": ["release.prepare", "release.verify"],
                },
            )
        ]

        routed = await router.route(
            _assignment(
                resource_profile={
                    "tool_permissions": ["release.prepare", "release.verify"]
                }
            ),
            events,
            controller.empty_state(),
        )

        assert routed.decision.outcome is RouteOutcome.ADOPTED
        assert routed.decision.route_kind is RouteKind.COORDINATOR_DAG
        assert routed.decision.reason_code == "semantic_plan_validated_for_submit"
        assert routed.decision.plan is not None
        assert all(
            step.capability is not None
            and step.capability.capability_id is None
            and step.capability.binding_id is not None
            and step.capability.version == "1.0.0"
            for step in routed.decision.plan.steps
        )
        assert routed.events[0].payload["purpose"] == "router_semantic_plan_submit"
        assert routed.events[0].payload["submit_eligible"] is True
        assert routed.events[0].payload["submit_gate_reasons"] == []
        assert routed.events[0].payload["plan_digest"].startswith("sha256:")
        assert routed.metrics is not None
        assert routed.metrics["router.semantic_planner.submit_candidate.count"] == 1.0

    asyncio.run(scenario())


def test_task_router_semantically_plans_multiple_exact_read_only_tools() -> None:
    async def scenario() -> None:
        model = _PlannerModel(_dag_proposal())
        controller = _SkillController(kind="tool")
        router = RuntimeTaskRouter(
            controller,  # type: ignore[arg-type]
            semantic_planner=SemanticPlanProposer(model),
            semantic_planner_mode="submit",
        )
        events = [
            SimpleNamespace(
                type="session.created",
                payload={"goal": "Use release.prepare and release.verify in parallel"},
            )
        ]

        routed = await router.route(
            _assignment(
                resource_profile={"tool_permissions": ["release.prepare", "release.verify"]}
            ),
            events,
            controller.empty_state(),
        )

        assert len(model.requests) == 1
        assert routed.decision.outcome is RouteOutcome.ADOPTED
        assert routed.decision.route_kind is RouteKind.COORDINATOR_DAG
        assert routed.decision.reason_code == "semantic_plan_validated_for_submit"
        assert routed.events[0].payload["submit_eligible"] is True

    asyncio.run(scenario())


def test_task_router_keeps_repair_dag_in_shadow_under_submit_mode() -> None:
    async def scenario() -> None:
        model = _PlannerModel(_dag_proposal(role="repair"))
        controller = _SkillController()
        router = RuntimeTaskRouter(
            controller,  # type: ignore[arg-type]
            semantic_planner=SemanticPlanProposer(model),
            semantic_planner_mode="submit",
        )
        events = [
            SimpleNamespace(
                type="session.created",
                payload={
                    "goal": "Use release.prepare and release.verify for this release",
                    "skill_names": ["release.prepare", "release.verify"],
                },
            )
        ]

        routed = await router.route(
            _assignment(
                resource_profile={
                    "tool_permissions": ["release.prepare", "release.verify"]
                }
            ),
            events,
            controller.empty_state(),
        )

        assert routed.decision.outcome is RouteOutcome.SHADOWED
        assert routed.decision.reason_code == "semantic_plan_validated_shadow"
        assert routed.events[0].payload["submit_eligible"] is False
        assert routed.events[0].payload["submit_gate_reasons"] == [
            "step:prepare:role_not_worker",
            "step:verify:role_not_worker",
        ]

    asyncio.run(scenario())


def test_semantic_shadow_model_event_is_not_replayed_into_agent_conversation() -> None:
    events = [
        SimpleNamespace(
            type="session.created",
            run_id="run-a",
            payload={"goal": "Use release.prepare and release.verify"},
        ),
        SimpleNamespace(
            type="model.turn.completed",
            run_id="run-a",
            payload={
                "model_call_id": "router-call",
                "purpose": "router_semantic_plan_shadow",
                "proposal_tool": SUBMIT_PLAN_TOOL,
                "proposal_accepted": True,
                "output": "",
            },
        ),
    ]

    messages = RuntimeExecutionEngine._build_capability_messages(
        events,
        current_run_id="run-a",
    )

    assert messages == ({"role": "user", "content": "Use release.prepare and release.verify"},)


def test_task_router_rejects_semantic_cycle_and_preserves_original_fallback() -> None:
    async def scenario() -> None:
        controller = _SkillController()
        router = RuntimeTaskRouter(
            controller,  # type: ignore[arg-type]
            semantic_planner=SemanticPlanProposer(_PlannerModel(_proposal(cycle=True))),
        )
        events = [
            SimpleNamespace(
                type="session.created",
                payload={
                    "goal": "Use release.prepare and release.verify for this release",
                    "skill_names": ["release.prepare", "release.verify"],
                },
            )
        ]

        routed = await router.route(_assignment(), events, controller.empty_state())

        assert routed.decision.outcome is RouteOutcome.FALLBACK
        assert routed.decision.reason_code == "multiple_explicit_skills_need_planner"
        assert routed.decision.plan is None
        assert routed.events[0].payload["proposal_accepted"] is False
        assert routed.metrics is not None
        assert routed.metrics["router.semantic_planner.rejected.count"] == 1.0

    asyncio.run(scenario())


def test_semantic_plan_validator_rejects_unmanaged_profile_and_unguarded_write() -> None:
    unmanaged = _proposal()
    unmanaged["steps"][0]["assignment"]["profile_id"] = "unmanaged-profile"
    unmanaged_proposal = SemanticPlanProposal.model_validate(unmanaged)
    unmanaged_plan = compile_semantic_plan(
        unmanaged_proposal,
        targets={
            "release.prepare": RouteTarget(kind="skill", name="release.prepare"),
            "release.verify": RouteTarget(kind="skill", name="release.verify"),
        },
    )
    with pytest.raises(RoutingPlanValidationError, match="managed assignment registry"):
        StructuredPlanValidator().validate(
            unmanaged_plan,
            _assignment(resource_profile={"agent_profile_ids": ["managed-profile"]}),
        )

    write = _proposal()
    write_proposal = SemanticPlanProposal.model_validate(write)
    write_plan = compile_semantic_plan(
        write_proposal,
        targets={
            "release.prepare": RouteTarget(
                kind="tool",
                name="release.prepare",
                permission="write-with-approval",
            ),
            "release.verify": RouteTarget(kind="tool", name="release.verify"),
        },
    )
    with pytest.raises(RoutingPlanValidationError, match="reviewer gate"):
        StructuredPlanValidator().validate(write_plan, _assignment())


def test_task_router_does_not_call_v2_shadow_planner_without_reservation_hook() -> None:
    async def scenario() -> None:
        model = _PlannerModel(_proposal())
        controller = _SkillController()
        router = RuntimeTaskRouter(
            controller,  # type: ignore[arg-type]
            semantic_planner=SemanticPlanProposer(model),
        )
        events = [
            SimpleNamespace(
                type="session.created",
                payload={
                    "goal": "Use release.prepare and release.verify for this release",
                    "skill_names": ["release.prepare", "release.verify"],
                },
            )
        ]

        routed = await router.route(
            _assignment(budget_policy_version="2"), events, controller.empty_state()
        )

        assert routed.decision.outcome is RouteOutcome.FALLBACK
        assert routed.decision.reason_code == "multiple_explicit_skills_need_planner"
        assert model.requests == []

    asyncio.run(scenario())


def test_task_router_reserves_v2_planner_before_model_call() -> None:
    async def scenario() -> None:
        order: list[tuple[str, str, int | None]] = []

        class OrderedPlannerModel(_PlannerModel):
            async def generate(self, request: ModelRequest) -> ModelResponse:
                order.append(("generate", request.model_call_id, None))
                return await super().generate(request)

        model = OrderedPlannerModel(_proposal())
        controller = _SkillController()
        router = RuntimeTaskRouter(
            controller,  # type: ignore[arg-type]
            semantic_planner=SemanticPlanProposer(model),
        )
        events = [
            SimpleNamespace(
                type="session.created",
                payload={
                    "goal": "Use release.prepare and release.verify for this release",
                    "skill_names": ["release.prepare", "release.verify"],
                },
            )
        ]

        async def reserve(model_call_id: str, tokens: int) -> None:
            order.append(("reserve", model_call_id, tokens))

        routed = await router.route(
            _assignment(budget_policy_version="2"),
            events,
            controller.empty_state(),
            reserve_model=reserve,
        )

        assert routed.decision.outcome is RouteOutcome.SHADOWED
        assert [item[0] for item in order] == ["reserve", "generate"]
        assert order[0][1] == order[1][1] == model.requests[0].model_call_id
        assert order[0][2] == 1_000
        assert model.requests[0].run_max_cost is None
        assert routed.events[0].payload["proposal_accepted"] is True

    asyncio.run(scenario())
