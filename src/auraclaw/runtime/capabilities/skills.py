from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from auraclaw.contracts.errors import (
    SchemaValidationError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.skills import SkillActivation, effective_skill_role
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.authority_queries import authority_request_id, binding_disposition_result
from auraclaw.runtime.capabilities.contracts import (
    CAPABILITY_LOAD,
    SKILL_BINDING_STATUS,
    CapabilityExecution,
    CapabilityProgressCallback,
    digest,
    result_content,
)
from auraclaw.runtime.capabilities.loading import CapabilityLoader
from auraclaw.runtime.capabilities.state import CapabilityState, run_key
from auraclaw.runtime.ports import CapabilityClient, ToolCall
from auraclaw.runtime.skill_workflow import (
    RuntimeSkillWorkflowExecutor,
    WorkflowStepProgress,
)


class SkillActivationService:
    """Own Skill activation, workflow progress, restoration and binding checks."""

    def __init__(self, client: CapabilityClient, loader: CapabilityLoader) -> None:
        self._client = client
        self._loader = loader
        self._workflow_executor = RuntimeSkillWorkflowExecutor(client)
        self._workflow_metrics: dict[tuple[str, str, str], dict[str, float]] = {}

    def metrics(self, assignment: RuntimeAssignment) -> dict[str, float]:
        return dict(self._workflow_metrics.get(run_key(assignment), {}))

    def release_run(self, assignment: RuntimeAssignment) -> None:
        self._workflow_metrics.pop(run_key(assignment), None)

    async def activate(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        snapshot: CapabilityState,
        *,
        progress: CapabilityProgressCallback | None = None,
    ) -> CapabilityExecution:
        # Workflow progress is a snapshot, never a reference to mutable live state.
        state = snapshot.to_checkpoint()

        async def checkpoint(snapshot: dict[str, Any], events: tuple[NewEvent, ...]) -> None:
            if progress is not None:
                await progress(
                    CapabilityState.from_checkpoint(snapshot).to_checkpoint(), copy.deepcopy(events)
                )

        return await self._activate_skill(
            assignment,
            call,
            state,
            progress=checkpoint if progress is not None else None,
        )

    async def activate_trusted_skill(
        self,
        assignment: RuntimeAssignment,
        snapshot: CapabilityState,
        *,
        publisher: str | None,
        name: str,
        inputs: dict[str, Any],
        activation_key: str,
        version: str = "*",
        activation_source: str = "trusted_runtime_context",
    ) -> CapabilityExecution:
        """Activate an exact Skill selected by trusted Runtime context."""
        current = snapshot.to_checkpoint()
        active = [item for item in current.get("active_skills", ()) if isinstance(item, dict)]
        existing = next(
            (
                item
                for item in active
                if isinstance(item.get("binding"), dict)
                and (publisher is None or item["binding"].get("publisher") == publisher)
                and item["binding"].get("skill_name") == name
                and (version == "*" or item["binding"].get("skill_version") == version)
            ),
            None,
        )
        if existing is not None:
            existing_activation = dict(existing.get("activation", {}))
            return CapabilityExecution(
                result={
                    "status": "activated",
                    "skill_activation_id": existing_activation.get("skill_activation_id"),
                    "skill_name": name,
                    "skill_version": existing["binding"].get("skill_version"),
                    "already_active": True,
                },
                state=current,
            )

        resolution = await self._client.resolve_skill(
            assignment,
            name=name,
            version=version,
            publisher=publisher,
            active_skill_names=tuple(
                str(item["binding"]["skill_name"])
                for item in active
                if isinstance(item.get("binding"), dict)
            ),
        )
        if resolution.status != "success" or resolution.binding is None:
            return CapabilityExecution(
                result={
                    "status": resolution.status,
                    "error_code": resolution.error_code or "skill_resolver_failed",
                    "summary": resolution.summary,
                },
                state=current,
            )
        binding = resolution.binding
        dependency_ids = [
            *(item.capability_id for item in binding.resolved_tools),
            *(item.capability_id for item in binding.resolved_resources),
            *(item.capability_id for item in binding.resolved_skills),
        ]
        missing = await self._load_skill_dependencies(
            assignment,
            current,
            dependency_ids,
            activation_call_id=activation_key,
        )
        if missing:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "skill_dependency_load_failed",
                    "missing_capability_ids": missing,
                },
                state=current,
            )
        activation = SkillActivation(
            skill_activation_id=_activation_id(assignment, activation_key),
            activation_key=activation_key,
            binding=binding,
            input_digest=f"sha256:{digest(inputs)}",
        )
        active.append(
            {
                "activation": activation.model_dump(mode="json"),
                "binding": binding.model_dump(mode="json"),
                "reference_requirements": [],
                "model_reference_paths": [],
            }
        )
        current["active_skills"] = active
        event = NewEvent(
            type="skill.activated",
            payload={
                "skill_activation_id": activation.skill_activation_id,
                "activation_key": activation.activation_key,
                "skill_name": binding.skill_name,
                "skill_version": binding.skill_version,
                "package_digest": binding.package_digest,
                "policy_version": binding.policy_version,
                "policy_decision_id": binding.policy_decision_id,
                "workflow_digest": (
                    binding.resolved_workflow.workflow_digest
                    if binding.resolved_workflow is not None
                    else None
                ),
                "activation": activation.model_dump(mode="json"),
                "reference_requirements": [],
                "model_reference_paths": [],
                "activation_source": activation_source,
            },
        )
        return CapabilityExecution(
            result={
                "status": "activated",
                "skill_activation_id": activation.skill_activation_id,
                "skill_name": binding.skill_name,
                "skill_version": binding.skill_version,
                "loaded_dependency_ids": dependency_ids,
            },
            state=current,
            events=(event,),
        )

    async def binding_disposition(
        self,
        assignment: RuntimeAssignment,
        snapshot: CapabilityState,
    ) -> dict[str, Any] | None:
        """Return the strongest current action for fixed active Skill bindings."""
        state = snapshot.to_checkpoint()
        strongest: dict[str, Any] | None = None
        precedence = {"continue": 0, "pause": 1, "cancel": 2}
        checked: set[tuple[str, str, str, str]] = set()
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict):
                continue
            if item.get("workflow_status") in {"completed", "failed", "cancelled"}:
                continue
            activation = item.get("activation")
            binding = item.get("binding")
            if not isinstance(activation, dict) or not isinstance(binding, dict):
                continue
            references = [
                {
                    "publisher": binding.get("publisher"),
                    "name": binding.get("skill_name"),
                    "version": binding.get("skill_version"),
                    "package_digest": binding.get("package_digest"),
                },
                *(
                    reference
                    for reference in binding.get("resolved_skills", ())
                    if isinstance(reference, dict)
                ),
            ]
            for reference in references:
                identity = (
                    str(reference.get("publisher", "")),
                    str(reference.get("name") or reference.get("skill_name") or ""),
                    str(reference.get("version") or reference.get("skill_version") or ""),
                    str(reference.get("package_digest", "")),
                )
                if not all(identity) or identity in checked:
                    continue
                checked.add(identity)
                result = await self._client.execute(
                    assignment,
                    ToolCall(
                        tool_invocation_id=authority_request_id(assignment, "binding_status"),
                        name=SKILL_BINDING_STATUS,
                        version="1",
                        arguments={
                            "publisher": identity[0],
                            "name": identity[1],
                            "version": identity[2],
                            "package_digest": identity[3],
                        },
                        expected_side_effect="read",
                    ),
                )
                payload = binding_disposition_result(result)
                action = str(payload.get("action", "cancel"))
                if action not in precedence:
                    action = "cancel"
                candidate = {
                    **payload,
                    "action": action,
                    "skill_activation_id": activation.get("skill_activation_id"),
                    "binding": {
                        "publisher": identity[0],
                        "name": identity[1],
                        "version": identity[2],
                        "package_digest": identity[3],
                    },
                }
                if strongest is None or precedence[action] > precedence[str(strongest["action"])]:
                    strongest = candidate
        return strongest

    def restore_skill_events(self, snapshot: CapabilityState, events: list[Any]) -> dict[str, Any]:
        state = snapshot.to_checkpoint()
        active = list(state.get("active_skills", ()))
        known = {
            item.get("activation", {}).get("skill_activation_id")
            for item in active
            if isinstance(item, dict)
        }
        for event in events:
            activation = event.payload.get("activation")
            if (
                event.type == "skill.activated"
                and isinstance(activation, dict)
                and activation.get("skill_activation_id") not in known
            ):
                active.append(
                    {
                        "activation": activation,
                        "binding": activation["binding"],
                        "reference_requirements": list(
                            event.payload.get("reference_requirements", ())
                        ),
                        "model_reference_paths": list(
                            event.payload.get("model_reference_paths", ())
                        ),
                    }
                )
                known.add(activation.get("skill_activation_id"))
        state["active_skills"] = active
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict) or not isinstance(item.get("activation"), dict):
                continue
            activation = item["activation"]
            relevant = [
                event
                for event in events
                if event.payload.get("skill_activation_id") == activation.get("skill_activation_id")
            ]
            activated = next((event for event in relevant if event.type == "skill.activated"), None)
            if not item.get("workflow_deadline") and activated is not None:
                item["workflow_deadline"] = (
                    activated.occurred_at
                    + timedelta(seconds=activation["binding"]["timeout_seconds"])
                ).isoformat()
            terminal = next(
                (
                    event
                    for event in relevant
                    if event.type
                    in {
                        "skill.completed",
                        "skill.failed",
                        "skill.cancelled",
                    }
                ),
                None,
            )
            if terminal is not None:
                item["workflow_status"] = terminal.type.removeprefix("skill.")
                item["workflow_error_code"] = terminal.payload.get("error")
                if terminal.type == "skill.completed":
                    try:
                        output = json.loads(terminal.payload.get("output_summary", "{}"))
                    except (ValueError, TypeError):
                        output = {}
                    saved_output = terminal.payload.get("workflow_output", output)
                    item["workflow_output"] = saved_output if isinstance(saved_output, dict) else {}

        return CapabilityState.from_checkpoint(state).to_checkpoint()

    def terminal_events(self, snapshot: CapabilityState, output: str) -> tuple[NewEvent, ...]:
        state = snapshot.to_checkpoint()
        events: list[NewEvent] = []
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict):
                continue
            activation = item.get("activation")
            if not isinstance(activation, dict):
                continue
            if (
                item.get("workflow_status") is not None
                or activation.get("binding", {}).get("resolved_workflow") is not None
            ):
                continue
            events.append(
                NewEvent(
                    type="skill.completed",
                    payload={
                        "skill_activation_id": activation["skill_activation_id"],
                        "activation_key": activation["activation_key"],
                        "skill_name": activation["binding"]["skill_name"],
                        "skill_version": activation["binding"]["skill_version"],
                        "package_digest": activation["binding"]["package_digest"],
                        "policy_version": activation["binding"]["policy_version"],
                        "policy_decision_id": activation["binding"].get("policy_decision_id"),
                        "output_summary": output,
                        "artifact_refs": [],
                    },
                )
            )
        return tuple(events)

    async def _activate_skill(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        state: dict[str, Any],
        *,
        progress: CapabilityProgressCallback | None,
    ) -> CapabilityExecution:
        capability_id = str(call.arguments.get("capability_id", ""))
        loaded = dict(state.get("loaded", {})).get(capability_id)
        if not isinstance(loaded, dict) or loaded.get("kind") != "skill":
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "skill_not_loaded",
                    "summary": "Skill must be loaded before activation.",
                },
                state=state,
            )
        contract = loaded.get("skill")
        if not isinstance(contract, dict):
            return CapabilityExecution(
                result={"status": "error", "error_code": "invalid_skill_contract"},
                state=state,
            )
        inputs = dict(call.arguments.get("inputs", {}))
        _validate_object(inputs, dict(contract.get("input_schema", {})))
        input_digest = f"sha256:{digest(inputs)}"
        active = [item for item in state.get("active_skills", ()) if isinstance(item, dict)]
        existing = next(
            (
                item
                for item in active
                if isinstance(item.get("activation"), dict)
                and item["activation"].get("activation_key") == call.tool_invocation_id
            ),
            None,
        )
        events: list[NewEvent] = []
        if existing is not None:
            activation = SkillActivation.model_validate(existing["activation"])
            if activation.input_digest != input_digest:
                return CapabilityExecution(
                    result={
                        "status": "denied",
                        "error_code": "skill_activation_input_mismatch",
                    },
                    state=state,
                )
            binding = activation.binding
            dependency_ids = [
                *(item.capability_id for item in binding.resolved_tools),
                *(item.capability_id for item in binding.resolved_resources),
                *(item.capability_id for item in binding.resolved_skills),
            ]
            if existing.get("workflow_status") in {"failed", "cancelled"}:
                return CapabilityExecution(
                    result={
                        "status": existing["workflow_status"],
                        "error_code": existing.get("workflow_error_code", "workflow_terminal"),
                        "skill_activation_id": activation.skill_activation_id,
                    },
                    state=state,
                )
            if existing.get("workflow_status") == "completed":
                return CapabilityExecution(
                    result={
                        "status": "completed",
                        "skill_activation_id": activation.skill_activation_id,
                        "workflow_output": dict(existing.get("workflow_output", {})),
                    },
                    state=state,
                )
        else:
            resolution = await self._client.resolve_skill(
                assignment,
                name=str(contract["name"]),
                version=str(contract["version"]),
                publisher=str(contract["publisher"]),
                active_skill_names=tuple(
                    str(item["binding"]["skill_name"])
                    for item in active
                    if isinstance(item.get("binding"), dict)
                ),
            )
            resolve_metrics = self._workflow_metrics.setdefault(run_key(assignment), {})
            resolve_metrics["skill.resolve.count"] = (
                resolve_metrics.get("skill.resolve.count", 0.0) + 1.0
            )
            result_metric = f"skill.resolve.result.{resolution.status}.count"
            resolve_metrics[result_metric] = resolve_metrics.get(result_metric, 0.0) + 1.0
            if effective_skill_role(assignment.role) != assignment.role:
                resolve_metrics["skill.resolve.role_alias.count"] = (
                    resolve_metrics.get("skill.resolve.role_alias.count", 0.0) + 1.0
                )
            if resolution.error_code == "skill_resolver_invalid_response":
                resolve_metrics["skill.resolve.invalid_response.count"] = (
                    resolve_metrics.get("skill.resolve.invalid_response.count", 0.0) + 1.0
                )
            if resolution.status != "success" or resolution.binding is None:
                return CapabilityExecution(
                    result={
                        "status": resolution.status,
                        "error_code": (resolution.error_code or "skill_resolver_failed"),
                        "summary": resolution.summary,
                    },
                    state=state,
                )
            binding = resolution.binding
            dependency_ids = [
                *(item.capability_id for item in binding.resolved_tools),
                *(item.capability_id for item in binding.resolved_resources),
                *(item.capability_id for item in binding.resolved_skills),
            ]
            if dependency_ids:
                missing = await self._load_skill_dependencies(
                    assignment,
                    state,
                    dependency_ids,
                    activation_call_id=call.tool_invocation_id,
                )
                if missing:
                    # Catalog reconciliation replaces one generation atomically.  A
                    # Skill can therefore resolve against generation N immediately
                    # before its dependency ids are retired by generation N+1.  Resolve
                    # once more and hydrate the fresh binding instead of exposing this
                    # normal race to the model as a permanent dependency failure.
                    refreshed = await self._client.resolve_skill(
                        assignment,
                        name=str(contract["name"]),
                        version=str(contract["version"]),
                        publisher=str(contract["publisher"]),
                        active_skill_names=tuple(
                            str(item["binding"]["skill_name"])
                            for item in active
                            if isinstance(item.get("binding"), dict)
                        ),
                    )
                    resolve_metrics["skill.resolve.retry.count"] = (
                        resolve_metrics.get("skill.resolve.retry.count", 0.0) + 1.0
                    )
                    if refreshed.status == "success" and refreshed.binding is not None:
                        binding = refreshed.binding
                        dependency_ids = [
                            *(item.capability_id for item in binding.resolved_tools),
                            *(item.capability_id for item in binding.resolved_resources),
                            *(item.capability_id for item in binding.resolved_skills),
                        ]
                        missing = await self._load_skill_dependencies(
                            assignment,
                            state,
                            dependency_ids,
                            activation_call_id=f"{call.tool_invocation_id}:refresh",
                        )
                if missing:
                    return CapabilityExecution(
                        result={
                            "status": "denied",
                            "error_code": "skill_dependency_load_failed",
                            "missing_capability_ids": missing,
                        },
                        state=state,
                    )
            activation = SkillActivation(
                skill_activation_id=_activation_id(assignment, call.tool_invocation_id),
                activation_key=call.tool_invocation_id,
                binding=binding,
                input_digest=input_digest,
            )
            existing = {
                "activation": activation.model_dump(mode="json"),
                "binding": binding.model_dump(mode="json"),
                "reference_requirements": [
                    dict(item)
                    for item in contract.get("required_references", ())
                    if isinstance(item, dict)
                ],
                "model_reference_paths": [
                    str(item["path"])
                    for item in contract.get("required_references", ())
                    if isinstance(item, dict) and item.get("preload") is True
                ],
            }
            active.append(existing)
            state["active_skills"] = active
            events.append(
                NewEvent(
                    type="skill.activated",
                    payload={
                        "skill_activation_id": activation.skill_activation_id,
                        "activation_key": activation.activation_key,
                        "skill_name": binding.skill_name,
                        "skill_version": binding.skill_version,
                        "package_digest": binding.package_digest,
                        "policy_version": binding.policy_version,
                        "policy_decision_id": binding.policy_decision_id,
                        "workflow_digest": (
                            binding.resolved_workflow.workflow_digest
                            if binding.resolved_workflow is not None
                            else None
                        ),
                        "activation": activation.model_dump(mode="json"),
                        "reference_requirements": existing["reference_requirements"],
                        "model_reference_paths": existing["model_reference_paths"],
                    },
                )
            )

        if not events and progress is not None and binding.resolved_workflow is not None:
            events.append(
                NewEvent(
                    type="skill.activated",
                    payload={
                        "skill_activation_id": activation.skill_activation_id,
                        "activation_key": activation.activation_key,
                        "skill_name": binding.skill_name,
                        "skill_version": binding.skill_version,
                        "package_digest": binding.package_digest,
                        "policy_version": binding.policy_version,
                        "policy_decision_id": binding.policy_decision_id,
                        "workflow_digest": binding.resolved_workflow.workflow_digest,
                        "activation": activation.model_dump(mode="json"),
                        "reference_requirements": existing.get("reference_requirements", ()),
                        "model_reference_paths": existing.get("model_reference_paths", ()),
                    },
                )
            )
        if binding.resolved_workflow is None:
            return CapabilityExecution(
                result={
                    "status": "activated",
                    "skill_activation_id": activation.skill_activation_id,
                    "skill_name": binding.skill_name,
                    "skill_version": binding.skill_version,
                    "loaded_dependency_ids": dependency_ids,
                },
                state=state,
                events=tuple(events),
            )

        assert existing is not None
        absent = [key for key in dependency_ids if key not in state.get("loaded", {})]
        if absent:
            missing = await self._load_skill_dependencies(
                assignment, state, absent, activation_call_id=call.tool_invocation_id
            )
            if missing:
                return CapabilityExecution(
                    result={
                        "status": "denied",
                        "error_code": "skill_dependency_load_failed",
                        "missing_capability_ids": missing,
                    },
                    state=state,
                    events=tuple(events),
                )
        if not existing.get("workflow_deadline"):
            existing["workflow_deadline"] = (
                datetime.now(UTC) + timedelta(seconds=binding.timeout_seconds)
            ).isoformat()
        # Commit activation first, then checkpoint its deadline before any I/O.
        if progress is not None:
            await progress(state, tuple(events))
            events.clear()

        async def checkpoint_workflow(step: WorkflowStepProgress) -> None:
            assert existing is not None
            existing["workflow_state"] = step.state
            existing["workflow_next_step_index"] = step.next_step_index
            existing["workflow_completed_steps"] = list(step.completed_steps)
            step_events: tuple[NewEvent, ...] = ()
            invocation_id = step.pending_invocation_id or step.settled_invocation_id
            if invocation_id:
                pending = step.pending_invocation_id is not None
                if pending:
                    existing["workflow_invocation_cycle"] = (
                        int(existing.get("workflow_invocation_cycle", 0)) + 1
                    )
                existing["workflow_status"] = "unknown" if pending else "running"
                existing["workflow_pending_invocation_id"] = invocation_id if pending else None
                step_events = (
                    NewEvent(
                        type="skill.invocation.requested"
                        if pending
                        else "skill.invocation.settled",
                        payload={
                            "skill_activation_id": activation.skill_activation_id,
                            "tool_invocation_id": invocation_id,
                            "invocation_cycle": existing.get("workflow_invocation_cycle", 0),
                            "package_digest": binding.package_digest,
                        },
                    ),
                )
            if progress is not None:
                await progress(state, step_events)
            else:
                events.extend(step_events)

        async def before_workflow_step() -> dict[str, Any] | None:
            disposition = await self.binding_disposition(
                assignment, CapabilityState.from_checkpoint(state)
            )
            if progress is not None:
                await progress(state, ())
            return disposition

        workflow_result = await self._workflow_executor.execute(
            assignment,
            activation,
            inputs=inputs,
            loaded_capabilities={
                key: value
                for key, value in dict(state.get("loaded", {})).items()
                if isinstance(value, dict)
            },
            approval_id=call.approval_id,
            approval_step_id=(
                str(existing.get("workflow_pending_step"))
                if existing.get("workflow_pending_step")
                else None
            ),
            resume_state=(
                dict(existing.get("workflow_state", {}))
                if isinstance(existing.get("workflow_state"), dict)
                else None
            ),
            start_step_index=int(existing.get("workflow_next_step_index", 0)),
            on_progress=checkpoint_workflow,
            before_step=before_workflow_step,
            completed_steps=tuple(existing.get("workflow_completed_steps", ())),
            deadline=datetime.fromisoformat(str(existing["workflow_deadline"])),
            pending_invocation_id=(
                existing.get("workflow_pending_invocation_id")
                if existing.get("workflow_status") == "unknown"
                else None
            ),
        )
        metrics = self._workflow_metrics.setdefault(run_key(assignment), {})
        metrics["skill.workflow.activation.count"] = (
            metrics.get("skill.workflow.activation.count", 0.0) + 1.0
        )
        metrics["skill.workflow.step.count"] = float(len(workflow_result.completed_steps))
        metrics[f"skill.workflow.result.{workflow_result.status}.count"] = (
            metrics.get(
                f"skill.workflow.result.{workflow_result.status}.count",
                0.0,
            )
            + 1.0
        )
        existing["workflow_status"] = workflow_result.status
        existing["workflow_completed_steps"] = list(workflow_result.completed_steps)
        if workflow_result.status == "waiting_for_approval":
            existing["workflow_pending_step"] = workflow_result.pending_step_id
            existing["workflow_pending_invocation_id"] = workflow_result.pending_invocation_id
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "approval_required",
                    "approval_id": workflow_result.approval_id,
                    "metadata": {"approval_request": workflow_result.approval_request or {}},
                    "skill_activation_id": activation.skill_activation_id,
                    "workflow_step_id": workflow_result.pending_step_id,
                },
                state=state,
                events=tuple(events),
            )
        existing["workflow_error_code"] = workflow_result.error_code
        if workflow_result.status in {"unknown", "paused"}:
            existing["workflow_pending_step"] = workflow_result.pending_step_id
            existing["workflow_pending_invocation_id"] = workflow_result.pending_invocation_id
            return CapabilityExecution(
                result={
                    "status": workflow_result.status,
                    "error_code": workflow_result.error_code,
                    "skill_activation_id": activation.skill_activation_id,
                    "pending_invocation_id": workflow_result.pending_invocation_id,
                },
                state=state,
                events=tuple(events),
            )
        existing.pop("workflow_pending_step", None)
        existing.pop("workflow_pending_invocation_id", None)
        if workflow_result.status == "completed":
            try:
                _validate_object(workflow_result.output, dict(contract.get("output_schema", {})))
            except (SchemaValidationError, ValueError):
                existing["workflow_status"] = "failed"
                existing["workflow_error_code"] = "workflow_output_invalid"
        if existing["workflow_status"] in {"failed", "cancelled"}:
            events.append(
                NewEvent(
                    type=f"skill.{existing['workflow_status']}",
                    payload={
                        "skill_activation_id": activation.skill_activation_id,
                        "skill_name": binding.skill_name,
                        "package_digest": binding.package_digest,
                        "workflow_digest": binding.resolved_workflow.workflow_digest,
                        "error": existing["workflow_error_code"],
                        "steps_completed": len(workflow_result.completed_steps),
                    },
                )
            )
            return CapabilityExecution(
                result={
                    "status": "error",
                    "error_code": existing["workflow_error_code"],
                    "skill_activation_id": activation.skill_activation_id,
                },
                state=state,
                events=tuple(events),
            )
        existing["workflow_output"] = workflow_result.output
        existing.pop("workflow_state", None)
        existing.pop("workflow_next_step_index", None)
        events.append(
            NewEvent(
                type="skill.completed",
                payload={
                    "skill_activation_id": activation.skill_activation_id,
                    "activation_key": activation.activation_key,
                    "skill_name": binding.skill_name,
                    "skill_version": binding.skill_version,
                    "package_digest": binding.package_digest,
                    "policy_version": binding.policy_version,
                    "policy_decision_id": binding.policy_decision_id,
                    "workflow_digest": binding.resolved_workflow.workflow_digest,
                    "workflow_output": workflow_result.output,
                    "output_summary": json.dumps(
                        workflow_result.output,
                        sort_keys=True,
                        separators=(",", ":"),
                    )[:4096],
                    "artifact_refs": [],
                    "steps_completed": len(workflow_result.completed_steps),
                },
            )
        )
        return CapabilityExecution(
            result={
                "status": "completed",
                "skill_activation_id": activation.skill_activation_id,
                "skill_name": binding.skill_name,
                "skill_version": binding.skill_version,
                "workflow_output": workflow_result.output,
                "completed_steps": list(workflow_result.completed_steps),
            },
            state=state,
            events=tuple(events),
        )

    async def _load_skill_dependencies(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        dependency_ids: list[str],
        *,
        activation_call_id: str,
    ) -> list[str]:
        missing: list[str] = []
        for batch_index, start in enumerate(range(0, len(dependency_ids), 8)):
            batch = dependency_ids[start : start + 8]
            invocation_id = f"dep_{digest({'activation': activation_call_id})[:24]}_{batch_index}"
            result = await self._client.execute(
                assignment,
                ToolCall(
                    tool_invocation_id=invocation_id,
                    name=CAPABILITY_LOAD,
                    version="1",
                    arguments={"capability_ids": batch},
                    expected_side_effect="read",
                    idempotency_key=invocation_id,
                ),
            )
            payload = result_content(result)
            hydrated = self._loader.merge_loaded(
                CapabilityState.from_checkpoint(state),
                payload.get("capabilities", ()),
                allowed_ids=set(batch),
            )
            state["loaded"] = hydrated
            missing.extend(sorted(set(batch).difference(hydrated)))
        return missing


def _validate_object(value: dict[str, Any], schema: dict[str, Any]) -> None:
    if schema and schema.get("type") != "object":
        raise ValueError("Skill input schema must describe an object")
    missing = set(schema.get("required", ())).difference(value)
    if missing:
        raise ValueError(f"Skill inputs are missing required fields: {sorted(missing)}")
    if schema.get("additionalProperties") is False:
        extras = set(value).difference(dict(schema.get("properties", {})))
        if extras:
            raise ValueError(f"Skill inputs contain unexpected fields: {sorted(extras)}")
    properties = schema.get("properties", {})
    if isinstance(properties, dict):
        for name, child_schema in properties.items():
            if (
                name in value
                and isinstance(child_schema, dict)
                and isinstance(child_schema.get("type"), str)
                and not _matches_json_type(value[name], str(child_schema["type"]))
            ):
                raise ValueError(f"Skill input {name} must be {child_schema['type']}")


def _matches_json_type(value: object, expected: str) -> bool:
    return {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }.get(expected, False)


def _activation_id(assignment: RuntimeAssignment, activation_key: str) -> str:
    value = f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}:{activation_key}"
    return f"ska_{hashlib.sha256(value.encode()).hexdigest()[:32]}"
