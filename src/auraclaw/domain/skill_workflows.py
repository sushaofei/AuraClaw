from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from auraclaw.contracts.errors import SchemaValidationError
from auraclaw.contracts.skill_workflows import SkillWorkflow
from auraclaw.contracts.skills import SkillManifest

_SELECTOR = re.compile(
    r"^\$(input|state|references)(?:\.([A-Za-z][A-Za-z0-9_-]*))*$"
)
_MAX_WORKFLOW_BYTES = 256 * 1024
_MAX_JSON_DEPTH = 32


@dataclass(frozen=True)
class CompiledSkillWorkflow:
    document: SkillWorkflow
    digest: str
    entrypoint: str
    reference_paths: tuple[str, ...]


def compile_skill_workflow(
    manifest: SkillManifest,
    files: Mapping[str, bytes],
) -> CompiledSkillWorkflow | None:
    descriptor = manifest.workflow
    if descriptor is None:
        if any(path.startswith("scripts/") for path in files):
            raise SchemaValidationError("Skill scripts require a workflow entrypoint")
        return None
    raw = files.get(descriptor.entrypoint)
    if raw is None:
        raise SchemaValidationError("Skill workflow entrypoint is missing")
    if len(raw) > _MAX_WORKFLOW_BYTES:
        raise SchemaValidationError("Skill workflow exceeds the maximum size")
    try:
        payload = json.loads(raw)
        _validate_json_depth(payload)
        document = SkillWorkflow.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError) as exc:
        raise SchemaValidationError("Skill workflow is invalid") from exc
    if document.api_version != descriptor.api_version:
        raise SchemaValidationError("Skill workflow apiVersion does not match manifest")
    if len(document.steps) > manifest.max_steps:
        raise SchemaValidationError("Skill workflow exceeds manifest max_steps")

    declared_tools = {item.name for item in manifest.required_tools}
    declared_resources = {item.uri_template for item in manifest.required_resources}
    required_references = {item.path: item for item in manifest.required_references}
    reference_ids = {item.id for item in document.references}
    available_results: set[str] = set()
    total_timeout = 0
    for step in document.steps:
        if step.operation == "tool.call" and step.capability not in declared_tools:
            raise SchemaValidationError(
                f"Workflow Tool is not declared by the Skill: {step.capability}"
            )
        if step.operation == "resource.read" and step.capability not in declared_resources:
            raise SchemaValidationError(
                f"Workflow Resource is not declared by the Skill: {step.capability}"
            )
        for expression in step.arguments.values():
            _validate_expression(expression, available_results, reference_ids)
        available_results.add(step.result)
        total_timeout += step.timeout_seconds * step.retry.max_attempts
    if total_timeout > manifest.timeout_seconds:
        raise SchemaValidationError("Skill workflow step timeouts exceed manifest timeout")

    reference_paths: list[str] = []
    for reference in document.references:
        requirement = required_references.get(reference.path)
        if requirement is None:
            raise SchemaValidationError(
                f"Workflow reference is not declared by the Skill: {reference.path}"
            )
        content = files.get(reference.path)
        if content is None:
            if reference.required:
                raise SchemaValidationError(
                    f"Required Skill reference is missing: {reference.path}"
                )
            continue
        if len(content) > requirement.max_bytes:
            raise SchemaValidationError(
                f"Skill reference exceeds its maximum size: {reference.path}"
            )
        if requirement.media_type == "application/json":
            try:
                value = json.loads(content)
                _validate_json_depth(value)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                raise SchemaValidationError(
                    f"Skill reference is not valid JSON: {reference.path}"
                ) from exc
        reference_paths.append(reference.path)

    for expression in document.outputs.values():
        _validate_expression(expression, available_results, reference_ids)
    canonical = json.dumps(
        document.model_dump(mode="json", by_alias=True),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return CompiledSkillWorkflow(
        document=document,
        digest=f"sha256:{hashlib.sha256(canonical).hexdigest()}",
        entrypoint=descriptor.entrypoint,
        reference_paths=tuple(reference_paths),
    )


def resolve_workflow_value(
    expression: Mapping[str, Any],
    *,
    inputs: Mapping[str, Any],
    state: Mapping[str, Any],
    references: Mapping[str, Any],
) -> Any:
    if set(expression) == {"literal"}:
        return _copy_json(expression["literal"])
    selector = expression.get("from")
    if not isinstance(selector, str):
        raise SchemaValidationError("Workflow expression is invalid")
    parts = selector.removeprefix("$").split(".")
    roots: dict[str, Any] = {
        "input": inputs,
        "state": state,
        "references": references,
    }
    current = roots.get(parts[0])
    if current is None:
        raise SchemaValidationError("Workflow selector root is invalid")
    for part in parts[1:]:
        if not isinstance(current, Mapping) or part not in current:
            raise SchemaValidationError(f"Workflow selector did not resolve: {selector}")
        current = current[part]
    return _copy_json(current)


def _validate_expression(
    expression: object,
    available_results: set[str],
    reference_ids: set[str],
) -> None:
    if not isinstance(expression, dict) or len(expression) != 1:
        raise SchemaValidationError("Workflow expression must contain exactly one operator")
    if "literal" in expression:
        _validate_json_depth(expression["literal"])
        return
    selector = expression.get("from")
    if not isinstance(selector, str) or _SELECTOR.fullmatch(selector) is None:
        raise SchemaValidationError("Workflow selector is invalid")
    parts = selector.removeprefix("$").split(".")
    if parts[0] == "state" and (len(parts) < 2 or parts[1] not in available_results):
        raise SchemaValidationError("Workflow selector references unavailable state")
    if parts[0] == "references":
        if len(parts) < 2:
            raise SchemaValidationError("Workflow reference selector requires an id")
        if parts[1] not in reference_ids:
            raise SchemaValidationError("Workflow selector references an unknown reference id")


def _validate_json_depth(value: Any, depth: int = 0) -> None:
    if depth > _MAX_JSON_DEPTH:
        raise ValueError("JSON nesting is too deep")
    if isinstance(value, dict):
        if len(value) > 10_000:
            raise ValueError("JSON object is too large")
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("JSON object key is invalid")
            _validate_json_depth(child, depth + 1)
    elif isinstance(value, list):
        if len(value) > 10_000:
            raise ValueError("JSON array is too large")
        for child in value:
            _validate_json_depth(child, depth + 1)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError("Value is not JSON compatible")


def _copy_json(value: Any) -> Any:
    return json.loads(json.dumps(value, separators=(",", ":")))
