from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from auraclaw.control.ports import RuntimeAssignment


class CapabilityContext(Protocol):
    async def trusted_messages(
        self, assignment: RuntimeAssignment, state: dict[str, Any]
    ) -> tuple[dict[str, Any], ...]: ...

    def model_tools(self, state: dict[str, Any]) -> tuple[dict[str, Any], ...]: ...


class CollaborationContext(Protocol):
    async def trusted_messages(
        self, assignment: RuntimeAssignment
    ) -> tuple[dict[str, Any], ...]: ...

    def model_tools(self, assignment: RuntimeAssignment) -> tuple[dict[str, Any], ...]: ...


@dataclass(frozen=True)
class RuntimeModelContext:
    trusted_messages: tuple[dict[str, Any], ...]
    tools: tuple[dict[str, Any], ...]


class RuntimeContextBuilder:
    """Assemble trusted instructions and visible schemas through narrow read ports."""

    _MODEL_DISCOVERY_TOOLS = frozenset(
        {
            "auraclaw.capabilities.search",
            "auraclaw.capabilities.load",
            "auraclaw.skills.activate",
            "auraclaw.skills.resolve_and_activate",
        }
    )

    def __init__(
        self, capabilities: CapabilityContext | None, collaboration: CollaborationContext | None
    ) -> None:
        self._capability_controller = capabilities
        self._collaboration_controller = collaboration

    async def build(
        self, assignment: RuntimeAssignment, state: dict[str, Any], turn_events: list[Any]
    ) -> RuntimeModelContext:
        turn_index = int(state.get("turn_index", 0))
        capability_state = dict(state.get("capability_state", {}))
        trusted: tuple[dict[str, Any], ...] = ()
        if assignment.budget.policy_version == "2":
            grants: list[dict[str, Any]] = next(
                (
                    e.payload.get("read_refresh", [])
                    for e in turn_events
                    if e.type == "run.requested" and e.payload.get("run_id") == assignment.run_id
                ),
                [],
            )
            if grants:
                trusted += (
                    {
                        "role": "system",
                        "content": (
                            "The user explicitly authorized these bounded read refresh grants. "
                            "They do not grant tool access or write permission. "
                            "Gateway authorization "
                            "still applies; the backend enforces call counts, interval and expiry: "
                            + json.dumps(grants, sort_keys=True)
                        ),
                    },
                )
        if self._capability_controller is not None:
            trusted += await self._capability_controller.trusted_messages(
                assignment, capability_state
            )
        routing_decision = state.get("routing_decision")
        if (
            turn_index == 0
            and isinstance(routing_decision, dict)
            and routing_decision.get("outcome") == "adopted"
            and routing_decision.get("route_kind") == "sequential_plan"
            and isinstance(routing_decision.get("plan"), dict)
        ):
            plan = dict(routing_decision["plan"])
            trusted += (
                {
                    "role": "system",
                    "content": (
                        "The pre-model Router validated this bounded current-Session "
                        "plan. Its capability bindings are fixed, but every invocation "
                        "still requires live Policy, Approval, budget and revocation "
                        "checks. Execute the steps in dependency order; do not create "
                        "Child Sessions for this sequential plan:\n"
                        + json.dumps(
                            {
                                "plan_digest": plan.get("plan_digest"),
                                "route_kind": plan.get("route_kind"),
                                "steps": [
                                    {
                                        "task_key": step.get("task_key"),
                                        "dependencies": step.get("dependencies", []),
                                        "capability": dict(step.get("capability") or {}).get(
                                            "name"
                                        ),
                                    }
                                    for step in plan.get("steps", [])
                                    if isinstance(step, dict)
                                ],
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                    ),
                },
            )
        if self._collaboration_controller is not None:
            trusted += await self._collaboration_controller.trusted_messages(assignment)
        model_tools: tuple[dict[str, Any], ...] = ()
        if self._capability_controller is not None:
            model_tools += self._capability_controller.model_tools(capability_state)
        if self._collaboration_controller is not None:
            model_tools += self._collaboration_controller.model_tools(assignment)
        if state.get("router_capability_prepared") or state.get("router_skill_activated"):
            model_tools = tuple(
                tool
                for tool in model_tools
                if tool.get("function", {}).get("name") not in self._MODEL_DISCOVERY_TOOLS
            )
        if state.get("concluding_reason"):
            # Existing collaboration terminals publish success-shaped results.
            # Stop via run.failed until a failure-capable terminal contract exists.
            model_tools = ()
            trusted += (
                {
                    "role": "system",
                    "content": (
                        "Runtime is concluding: " + str(state["concluding_reason"]) + ". "
                        "Do not request business tools or claim the whole task succeeded. "
                        "Summarize confirmed results, unresolved failures and unknown effects. "
                        "Runtime will record the failure through canonical events. "
                        "Do not publish a successful Child result. No further tools are permitted."
                    ),
                },
            )
        return RuntimeModelContext(trusted, model_tools)
