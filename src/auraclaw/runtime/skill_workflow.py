from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from auraclaw.contracts.errors import SchemaValidationError
from auraclaw.contracts.skill_workflows import SkillWorkflow, WorkflowStep
from auraclaw.contracts.skills import SkillActivation
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.domain.skill_workflows import resolve_workflow_value
from auraclaw.runtime.ports import CapabilityClient, ToolCall

_TEMPLATE_FIELD = re.compile(r"\{([A-Za-z0-9_.-]+)\}")
_MAX_STATE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class WorkflowExecutionResult:
    status: str
    output: dict[str, Any]
    completed_steps: tuple[str, ...]
    pending_step_id: str | None = None
    pending_invocation_id: str | None = None
    approval_id: str | None = None
    approval_request: dict[str, Any] | None = None
    error_code: str | None = None


@dataclass(frozen=True)
class WorkflowStepProgress:
    next_step_index: int
    completed_steps: tuple[str, ...]
    state: dict[str, Any]


WorkflowProgressCallback = Callable[[WorkflowStepProgress], Awaitable[None]]


class RuntimeSkillWorkflowExecutor:
    """Execute signed declarative Skill workflows through governed Runtime ports."""

    def __init__(self, client: CapabilityClient) -> None:
        self._client = client

    async def execute(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
        *,
        inputs: dict[str, Any],
        loaded_capabilities: dict[str, dict[str, Any]],
        approval_id: str | None = None,
        approval_step_id: str | None = None,
        resume_state: dict[str, Any] | None = None,
        start_step_index: int = 0,
        on_progress: WorkflowProgressCallback | None = None,
        deadline: datetime | None = None,
        pending_invocation_id: str | None = None,
        completed_steps: tuple[str, ...] = (),
    ) -> WorkflowExecutionResult:
        deadline = deadline or datetime.now(UTC) + timedelta(
            seconds=activation.binding.timeout_seconds
        )
        if assignment.deadline is not None:
            deadline = min(deadline, assignment.deadline)
        remaining = (deadline - datetime.now(UTC)).total_seconds()
        context: dict[str, Any] = {"completed": completed_steps}
        if remaining <= 0:
            return WorkflowExecutionResult(
                status="unknown" if pending_invocation_id else "failed", output={},
                completed_steps=completed_steps, pending_invocation_id=pending_invocation_id,
                error_code="workflow_budget_exhausted",
            )
        try:
            async with asyncio.timeout(remaining):
                return await self._execute(
                    assignment, activation, inputs=inputs, loaded_capabilities=loaded_capabilities,
                    approval_id=approval_id, approval_step_id=approval_step_id,
                    resume_state=resume_state, start_step_index=start_step_index,
                    on_progress=on_progress, context=context,
                    pending_invocation_id=pending_invocation_id,
                )
        except TimeoutError:
            return WorkflowExecutionResult(
                status="unknown" if context.get("write_in_flight") else "failed",
                output={}, completed_steps=tuple(context["completed"]),
                pending_step_id=context.get("step_id"),
                pending_invocation_id=context.get("invocation_id"),
                error_code="workflow_budget_exhausted",
            )
        except SchemaValidationError as exc:
            return WorkflowExecutionResult(
                status="failed", output={}, completed_steps=tuple(context["completed"]),
                error_code=exc.code,
            )

    async def _execute(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
        *,
        inputs: dict[str, Any],
        loaded_capabilities: dict[str, dict[str, Any]],
        approval_id: str | None = None,
        approval_step_id: str | None = None,
        resume_state: dict[str, Any] | None = None,
        start_step_index: int = 0,
        on_progress: WorkflowProgressCallback | None = None,
        context: dict[str, Any],
        pending_invocation_id: str | None = None,
    ) -> WorkflowExecutionResult:
        resolved = activation.binding.resolved_workflow
        if resolved is None:
            return WorkflowExecutionResult(
                status="not_configured", output={}, completed_steps=()
            )
        document = await self._load_workflow(assignment, activation)
        references = await self._load_references(assignment, activation, document)
        if start_step_index < 0 or start_step_index > len(document.steps):
            raise SchemaValidationError("Workflow resume cursor is invalid")
        state: dict[str, Any] = _copy_object(resume_state or {})
        completed: list[str] = [step.id for step in document.steps[:start_step_index]]
        context["completed"] = completed
        tools = {
            item.canonical_name: item for item in activation.binding.resolved_tools
        }
        resources = {
            item.uri_template: item for item in activation.binding.resolved_resources
        }
        for step_index, step in enumerate(
            document.steps[start_step_index:], start=start_step_index
        ):
            arguments = {
                name: resolve_workflow_value(
                    expression,
                    inputs=inputs,
                    state=state,
                    references=references,
                )
                for name, expression in step.arguments.items()
            }
            invocation_id = _invocation_id(
                activation.skill_activation_id,
                resolved.workflow_digest,
                step.id,
            )
            context.update(step_id=step.id, invocation_id=invocation_id, write_in_flight=False)
            step_approval = approval_id if approval_step_id == step.id else None
            if step.operation == "tool.call":
                tool_dependency = tools[step.capability]
                context["write_in_flight"] = tool_dependency.expected_side_effect != "read"
                if pending_invocation_id == invocation_id:
                    lookup = getattr(self._client, "invocation_status", None)
                    observed = await lookup(assignment, invocation_id) if callable(lookup) else {}
                    if (observed.get("status") in {"error", "denied", "cancelled"}
                            and observed.get("side_effect_status") != "unknown"):
                        return WorkflowExecutionResult(
                            status="cancelled" if observed["status"] == "cancelled" else "failed",
                            output={}, completed_steps=tuple(completed),
                            error_code=observed.get("error_code") or "workflow_step_failed",
                        )
                    if observed.get("status") != "success":
                        return WorkflowExecutionResult(
                            status="unknown", output={}, completed_steps=tuple(completed),
                            pending_step_id=step.id, pending_invocation_id=invocation_id,
                            error_code="workflow_result_pending",
                        )
                result = await self._call_tool(
                    assignment,
                    step,
                    invocation_id=invocation_id,
                    arguments=arguments,
                    version=tool_dependency.version,
                    expected_side_effect=tool_dependency.expected_side_effect,
                    approval_id=step_approval,
                )
            else:
                resource_dependency = resources[step.capability]
                loaded = loaded_capabilities.get(resource_dependency.capability_id)
                if not isinstance(loaded, dict):
                    raise SchemaValidationError("Workflow Resource binding is not loaded")
                result = await self._read_resource(
                    assignment,
                    loaded,
                    step=step,
                    arguments=arguments,
                )
            if result.get("error_code") == "approval_required":
                approval_request = _approval_request(result)
                return WorkflowExecutionResult(
                    status="waiting_for_approval",
                    output={},
                    completed_steps=tuple(completed),
                    pending_step_id=step.id,
                    pending_invocation_id=invocation_id,
                    approval_id=(
                        _optional_string(approval_request.get("approval_id"))
                        or _optional_string(result.get("approval_id"))
                    ),
                    approval_request=approval_request,
                    error_code="approval_required",
                )
            context["write_in_flight"] = False
            status = result.get("status")
            if (status in {"timeout", "cancelled"} and step.operation == "tool.call"
                    and tools[step.capability].expected_side_effect != "read"
                    and result.get("side_effect_status") != "not_started"):
                status = "unknown"
            if status != "success":
                return WorkflowExecutionResult(
                    status=("unknown" if status == "unknown" else
                            "cancelled" if status == "cancelled" else "failed"),
                    output={},
                    completed_steps=tuple(completed),
                    pending_step_id=step.id,
                    pending_invocation_id=invocation_id,
                    error_code=_optional_string(result.get("error_code"))
                    or "workflow_step_failed",
                )
            state[step.result] = _result_content(result)
            _guard_state_size(state)
            completed.append(step.id)
            if on_progress is not None:
                await on_progress(
                    WorkflowStepProgress(
                        next_step_index=step_index + 1,
                        completed_steps=tuple(completed),
                        state=_copy_object(state),
                    )
                )
        output = {
            name: resolve_workflow_value(
                expression,
                inputs=inputs,
                state=state,
                references=references,
            )
            for name, expression in document.outputs.items()
        }
        _guard_state_size(output)
        return WorkflowExecutionResult(
            status="completed",
            output=output,
            completed_steps=tuple(completed),
        )

    async def _load_workflow(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
    ) -> SkillWorkflow:
        resolved = activation.binding.resolved_workflow
        assert resolved is not None
        parts = await self._client.load_skill_part(
            assignment,
            publisher=activation.binding.publisher,
            name=activation.binding.skill_name,
            version=activation.binding.skill_version,
            path=resolved.entrypoint,
        )
        text = _single_text(parts, "Skill workflow")
        try:
            document = SkillWorkflow.model_validate_json(text)
        except ValueError as exc:
            raise SchemaValidationError("Skill workflow is invalid at Runtime") from exc
        canonical = json.dumps(
            document.model_dump(mode="json", by_alias=True),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        digest = f"sha256:{hashlib.sha256(canonical).hexdigest()}"
        if digest != resolved.workflow_digest:
            raise SchemaValidationError("Skill workflow digest does not match binding")
        return document

    async def _load_references(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
        document: SkillWorkflow,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for reference in document.references:
            parts = await self._client.load_skill_part(
                assignment,
                publisher=activation.binding.publisher,
                name=activation.binding.skill_name,
                version=activation.binding.skill_version,
                path=reference.path,
            )
            text = _single_text(parts, f"Skill reference {reference.path}")
            try:
                result[reference.id] = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SchemaValidationError("Executor references must be JSON") from exc
        _guard_state_size(result)
        return result

    async def _call_tool(
        self,
        assignment: RuntimeAssignment,
        step: WorkflowStep,
        *,
        invocation_id: str,
        arguments: dict[str, Any],
        version: str,
        expected_side_effect: str,
        approval_id: str | None,
    ) -> dict[str, Any]:
        attempts = step.retry.max_attempts
        for attempt in range(1, attempts + 1):
            try:
                async with asyncio.timeout(step.timeout_seconds):
                    result = await self._client.execute(
                        assignment,
                        ToolCall(
                            tool_invocation_id=(invocation_id if attempt == 1
                                                else f"{invocation_id}_r{attempt}"),
                            name=step.capability,
                            version=version,
                            arguments=arguments,
                            expected_side_effect=expected_side_effect,
                            approval_id=approval_id,
                            idempotency_key=(invocation_id if attempt == 1
                                             else f"{invocation_id}_r{attempt}"),
                        ),
                    )
            except TimeoutError:
                result = {"status": "unknown" if expected_side_effect != "read" else "timeout",
                          "error_code": "timeout", "side_effect_status": "unknown"}
            error_code = _optional_string(result.get("error_code"))
            if (
                expected_side_effect != "read"
                or result.get("status") in {"unknown", "cancelled"}
                or attempt >= attempts
                or error_code not in set(step.retry.retry_on)
                or error_code == "approval_required"
            ):
                return result
            if step.retry.strategy == "exponential":
                await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))
        raise RuntimeError("unreachable Workflow retry state")

    async def _read_resource(
        self,
        assignment: RuntimeAssignment,
        loaded: dict[str, Any],
        *,
        arguments: dict[str, Any],
        step: WorkflowStep,
    ) -> dict[str, Any]:
        resource = loaded.get("resource")
        if not isinstance(resource, dict):
            raise SchemaValidationError("Workflow Resource contract is invalid")
        if loaded.get("kind") == "resource":
            uri = str(resource["uri"])
        else:
            uri = _expand_uri_template(str(resource["uri_template"]), arguments)
        for attempt in range(1, step.retry.max_attempts + 1):
            try:
                async with asyncio.timeout(step.timeout_seconds):
                    contents = await self._client.read_resource(assignment, uri)
                return {"status": "success", "content": {"uri": uri, "contents": contents}}
            except TimeoutError:
                error_code = "timeout"
            except Exception as exc:
                error_code = str(getattr(exc, "code", "resource_read_failed"))
            if attempt >= step.retry.max_attempts or error_code not in step.retry.retry_on:
                return {"status": "error", "error_code": error_code}
            if step.retry.strategy == "exponential":
                await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))
        raise RuntimeError("unreachable Resource retry state")


def _invocation_id(activation_id: str, workflow_digest: str, step_id: str) -> str:
    payload = "\0".join((activation_id, workflow_digest, step_id)).encode()
    return f"wfi_{hashlib.sha256(payload).hexdigest()[:40]}"


def _single_text(parts: list[dict[str, Any]], label: str) -> str:
    texts = [
        str(part["text"])
        for part in parts
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    ]
    if len(texts) != 1:
        raise SchemaValidationError(f"{label} must contain exactly one text part")
    return texts[0]


def _result_content(result: dict[str, Any]) -> Any:
    return result.get("content", result)


def _guard_state_size(value: Any) -> None:
    try:
        size = len(json.dumps(value, separators=(",", ":")).encode())
    except (TypeError, ValueError) as exc:
        raise SchemaValidationError("Workflow state is not JSON compatible") from exc
    if size > _MAX_STATE_BYTES:
        raise SchemaValidationError("Workflow state exceeds the maximum size")


def _expand_uri_template(template: str, arguments: dict[str, Any]) -> str:
    required = set(_TEMPLATE_FIELD.findall(template))
    if required != set(arguments):
        raise SchemaValidationError("Workflow Resource arguments do not match URI template")
    return _TEMPLATE_FIELD.sub(
        lambda match: quote(str(arguments[match.group(1)]), safe=""),
        template,
    )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _copy_object(value: dict[str, Any]) -> dict[str, Any]:
    try:
        copied = json.loads(json.dumps(value, separators=(",", ":")))
    except (TypeError, ValueError) as exc:
        raise SchemaValidationError("Workflow state is not JSON compatible") from exc
    if not isinstance(copied, dict):
        raise SchemaValidationError("Workflow state must be an object")
    return copied


def _approval_request(result: dict[str, Any]) -> dict[str, Any]:
    metadata = result.get("metadata")
    if not isinstance(metadata, dict):
        return {}
    request = metadata.get("approval_request")
    return dict(request) if isinstance(request, dict) else {}
