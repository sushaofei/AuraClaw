from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from auraclaw.contracts.capabilities import RequiredCapabilityRef
from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.skills import SkillActivation
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import CapabilityClient, ToolCall

CAPABILITY_SEARCH = "auraclaw.capabilities.search"
CAPABILITY_LOAD = "auraclaw.capabilities.load"
SKILL_ACTIVATE = "auraclaw.skills.activate"
RESOURCE_READ = "auraclaw.resources.read"
SKILL_BINDING_STATUS = "auraclaw.skills.binding-status"
_TEMPLATE_FIELD = re.compile(r"\{([A-Za-z0-9_.-]+)\}")


@dataclass(frozen=True)
class CapabilityExecution:
    result: dict[str, Any]
    state: dict[str, Any]
    events: tuple[NewEvent, ...] = ()


class CapabilityAdmissionError(RuntimeError):
    """Raised before the first model call when a fixed capability cannot be loaded."""


class RuntimeCapabilityController:
    """Owns model-visible capability selection while the Gateway owns authority."""

    def __init__(
        self,
        client: CapabilityClient,
        *,
        max_candidates: int = 20,
        max_loaded: int = 24,
        max_searches: int = 4,
        max_loads: int = 4,
        max_schema_bytes: int = 64 * 1024,
    ) -> None:
        self._client = client
        self._max_candidates = max_candidates
        self._max_loaded = max_loaded
        self._max_searches = max_searches
        self._max_loads = max_loads
        self._max_schema_bytes = max_schema_bytes

    @staticmethod
    def empty_state() -> dict[str, Any]:
        return {
            "candidates": {},
            "loaded": {},
            "active_skills": [],
            "search_count": 0,
            "load_count": 0,
        }

    def model_tools(self, state: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        tools = [
            _function_tool(
                CAPABILITY_SEARCH,
                "Search the policy-visible capability catalog when the task needs "
                "external data, an action, or a governed Skill.",
                {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "maxLength": 1024},
                        "capability_id": {"type": "string", "maxLength": 256},
                        "canonical_name": {"type": "string", "maxLength": 256},
                        "server_id": {"type": "string", "maxLength": 128},
                        "kinds": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": [
                                    "resource",
                                    "resource_template",
                                    "tool",
                                    "skill",
                                ],
                            },
                        },
                        "required_permissions": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self._max_candidates,
                        },
                    },
                    "additionalProperties": False,
                },
            ),
            _function_tool(
                CAPABILITY_LOAD,
                "Load authoritative contracts for a small set of capability ids "
                "returned by capability search.",
                {
                    "type": "object",
                    "properties": {
                        "capability_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": self._max_loaded,
                        }
                    },
                    "required": ["capability_ids"],
                    "additionalProperties": False,
                },
            ),
            _function_tool(
                SKILL_ACTIVATE,
                "Request activation of one loaded Skill. Runtime and Policy make "
                "the authoritative decision.",
                {
                    "type": "object",
                    "properties": {
                        "capability_id": {"type": "string"},
                        "inputs": {"type": "object"},
                    },
                    "required": ["capability_id", "inputs"],
                    "additionalProperties": False,
                },
            ),
            _function_tool(
                RESOURCE_READ,
                "Read one loaded Resource or Resource Template through the "
                "governed Resource Gateway.",
                {
                    "type": "object",
                    "properties": {
                        "capability_id": {"type": "string"},
                        "arguments": {"type": "object"},
                    },
                    "required": ["capability_id"],
                    "additionalProperties": False,
                },
            ),
        ]
        for loaded in dict(state.get("loaded", {})).values():
            if not isinstance(loaded, dict):
                continue
            model_tool = loaded.get("model_tool")
            if isinstance(model_tool, dict):
                tools.append(copy.deepcopy(model_tool))
        return tuple(tools)

    async def preload_required(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        """Deterministically load Assignment refs before the model can run."""
        raw_refs = assignment.resource_profile.get("required_capabilities", ())
        if not raw_refs:
            return copy.deepcopy(state)
        try:
            refs = tuple(RequiredCapabilityRef.model_validate(item) for item in raw_refs)
        except Exception as exc:
            raise CapabilityAdmissionError("required capability refs are invalid") from exc
        if len(refs) > self._max_loaded:
            raise CapabilityAdmissionError("required capability count exceeds runtime limit")
        ids = tuple(dict.fromkeys(item.capability_id for item in refs))
        result = await self._client.execute(
            assignment,
            ToolCall(
                tool_invocation_id=(
                    "required_capabilities_"
                    + hashlib.sha256("\0".join(ids).encode()).hexdigest()[:24]
                ),
                name=CAPABILITY_LOAD,
                version="1",
                arguments={"capability_ids": list(ids)},
                expected_side_effect="read",
            ),
        )
        payload = _result_content(result)
        returned = {
            str(item.get("capability_id")): dict(item)
            for item in payload.get("capabilities", ())
            if isinstance(item, dict) and item.get("capability_id")
        }
        failures: list[str] = []
        for ref in refs:
            descriptor = returned.get(ref.capability_id)
            if descriptor is None:
                failures.append(f"{ref.capability_id}:missing")
                continue
            if ref.version is not None and descriptor.get("version") != ref.version:
                failures.append(f"{ref.capability_id}:version_mismatch")
            if (
                ref.content_digest is not None
                and descriptor.get("content_digest") != ref.content_digest
            ):
                failures.append(f"{ref.capability_id}:content_digest_mismatch")
        if failures:
            raise CapabilityAdmissionError(
                "required capability admission failed: " + ", ".join(failures)
            )
        current = copy.deepcopy(state)
        current["candidates"] = {**dict(current.get("candidates", {})), **returned}
        current["loaded"] = self._merge_loaded(
            current,
            tuple(returned.values()),
            allowed_ids=set(ids),
        )
        omitted = [item for item in ids if item not in current["loaded"]]
        if omitted:
            raise CapabilityAdmissionError(
                "required capability admission failed: runtime load limit for "
                + ", ".join(omitted)
            )
        current["required_capabilities_preloaded"] = True
        return current

    async def binding_disposition(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Return the strongest current action for fixed active Skill bindings."""
        strongest: dict[str, Any] | None = None
        precedence = {"continue": 0, "pause": 1, "cancel": 2}
        checked: set[tuple[str, str, str, str]] = set()
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict):
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
                        tool_invocation_id=(
                            "binding_status_"
                            + hashlib.sha256("\0".join(identity).encode()).hexdigest()[:24]
                        ),
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
                payload = _result_content(result)
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

    async def trusted_messages(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        messages: list[dict[str, Any]] = []
        emitted: set[tuple[str, str, str, str]] = set()
        pending: list[tuple[str, str, str, str]] = []
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict):
                continue
            binding = item.get("binding")
            if not isinstance(binding, dict):
                continue
            dependencies = binding.get("resolved_skills", ())
            skill_refs = [dependency for dependency in dependencies if isinstance(dependency, dict)]
            skill_refs.append(
                {
                    "publisher": binding["publisher"],
                    "skill_name": binding["skill_name"],
                    "skill_version": binding["skill_version"],
                    "package_digest": binding["package_digest"],
                }
            )
            for skill_ref in skill_refs:
                identity = (
                    str(skill_ref["publisher"]),
                    str(skill_ref["skill_name"]),
                    str(skill_ref["skill_version"]),
                    str(skill_ref["package_digest"]),
                )
                if identity in emitted:
                    continue
                emitted.add(identity)
                pending.append(identity)

        if not pending:
            return tuple(messages)

        loaded = await asyncio.gather(
            *(
                self._client.load_skill_part(
                    assignment,
                    publisher=identity[0],
                    name=identity[1],
                    version=identity[2],
                    path="SKILL.md",
                )
                for identity in pending
            )
        )
        for identity, parts in zip(pending, loaded, strict=True):
            text = next(
                (
                    str(part["text"])
                    for part in parts
                    if isinstance(part, dict) and isinstance(part.get("text"), str)
                ),
                "",
            )
            if text:
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "Activated signed AuraClaw Skill "
                            f"{identity[0]}/{identity[1]}@{identity[2]} "
                            f"(digest {identity[3]}):\n{text}"
                        ),
                    }
                )
        return tuple(messages)

    async def execute(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        state: dict[str, Any],
    ) -> CapabilityExecution:
        current = copy.deepcopy(state)
        if call.name == CAPABILITY_SEARCH:
            search_count = int(current.get("search_count", 0))
            if search_count >= self._max_searches:
                return CapabilityExecution(
                    result={
                        "status": "denied",
                        "error_code": "capability_search_budget_exhausted",
                    },
                    state=current,
                )
            bounded = ToolCall(
                **{
                    **call.__dict__,
                    "version": "1",
                    "arguments": {
                        **call.arguments,
                        "limit": min(
                            int(call.arguments.get("limit", 10)),
                            self._max_candidates,
                        ),
                    },
                }
            )
            result = await self._client.execute(assignment, bounded)
            payload = _result_content(result)
            candidates = {}
            for item in payload.get("capabilities", ()):
                if isinstance(item, dict) and item.get("capability_id"):
                    candidates[str(item["capability_id"])] = dict(item)
            current["candidates"] = dict(list(candidates.items())[: self._max_candidates])
            current["search_count"] = search_count + 1
            return CapabilityExecution(result=result, state=current)

        if call.name == CAPABILITY_LOAD:
            load_count = int(current.get("load_count", 0))
            if load_count >= self._max_loads:
                return CapabilityExecution(
                    result={
                        "status": "denied",
                        "error_code": "capability_load_budget_exhausted",
                    },
                    state=current,
                )
            candidates = dict(current.get("candidates", {}))
            requested = [
                str(value) for value in call.arguments.get("capability_ids", ()) if str(value)
            ][: self._max_loaded]
            result = await self._client.execute(
                assignment,
                ToolCall(
                    **{
                        **call.__dict__,
                        "version": "1",
                        "arguments": {"capability_ids": requested},
                    }
                ),
            )
            payload = _result_content(result)
            current["loaded"] = self._merge_loaded(
                current,
                payload.get("capabilities", ()),
                allowed_ids=set(requested),
            )
            hydrated = {
                str(item["capability_id"]): dict(item)
                for item in payload.get("capabilities", ())
                if isinstance(item, dict) and item.get("capability_id")
            }
            current["candidates"] = {**candidates, **hydrated}
            current["load_count"] = load_count + 1
            return CapabilityExecution(result=result, state=current)

        if call.name == SKILL_ACTIVATE:
            return await self._activate_skill(assignment, call, current)

        if call.name == RESOURCE_READ:
            return await self._read_resource(assignment, call, current)

        loaded_tool = next(
            (
                item
                for item in dict(current.get("loaded", {})).values()
                if isinstance(item, dict)
                and item.get("kind") == "tool"
                and item.get("canonical_name") == call.name
                and isinstance(item.get("model_tool"), dict)
            ),
            None,
        )
        if loaded_tool is None:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "capability_not_loaded",
                    "summary": "Tool must be loaded from the capability catalog first.",
                },
                state=current,
            )
        invocation = ToolCall(
            **{
                **call.__dict__,
                "version": str(loaded_tool["version"]),
                "expected_side_effect": (
                    "read" if loaded_tool.get("permission") == "read-only" else "write"
                ),
            }
        )
        return CapabilityExecution(
            result=await self._client.execute(assignment, invocation),
            state=current,
        )

    def terminal_events(self, state: dict[str, Any], output: str) -> tuple[NewEvent, ...]:
        events: list[NewEvent] = []
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict):
                continue
            activation = item.get("activation")
            if not isinstance(activation, dict):
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
        active = [item for item in state.get("active_skills", ()) if isinstance(item, dict)]
        binding = await self._client.resolve_skill(
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
        dependency_ids = [
            *(item.capability_id for item in binding.resolved_tools),
            *(item.capability_id for item in binding.resolved_resources),
            *(item.capability_id for item in binding.resolved_skills),
        ]
        if dependency_ids:
            missing: list[str] = []
            for batch_index, start in enumerate(range(0, len(dependency_ids), 8)):
                batch = dependency_ids[start : start + 8]
                dependency_invocation_id = (
                    f"dep_{_digest({'activation': call.tool_invocation_id})[:24]}_{batch_index}"
                )
                dependency_result = await self._client.execute(
                    assignment,
                    ToolCall(
                        tool_invocation_id=dependency_invocation_id,
                        name=CAPABILITY_LOAD,
                        version="1",
                        arguments={"capability_ids": batch},
                        expected_side_effect="read",
                        idempotency_key=dependency_invocation_id,
                    ),
                )
                dependency_payload = _result_content(dependency_result)
                hydrated = self._merge_loaded(
                    state,
                    dependency_payload.get("capabilities", ()),
                    allowed_ids=set(batch),
                )
                state["loaded"] = hydrated
                missing.extend(sorted(set(batch).difference(hydrated)))
            if missing:
                return CapabilityExecution(
                    result={
                        "status": "denied",
                        "error_code": "skill_dependency_load_failed",
                        "summary": (
                            "Skill dependencies could not be loaded within the "
                            "Runtime capability budget."
                        ),
                        "missing_capability_ids": missing,
                    },
                    state=state,
                )
        activation_key = call.tool_invocation_id
        activation = SkillActivation(
            skill_activation_id=_activation_id(assignment, activation_key),
            activation_key=activation_key,
            binding=binding,
            input_digest=f"sha256:{_digest(inputs)}",
        )
        state["active_skills"] = [
            *active,
            {
                "activation": activation.model_dump(mode="json"),
                "binding": binding.model_dump(mode="json"),
            },
        ]
        payload = {
            "skill_activation_id": activation.skill_activation_id,
            "activation_key": activation.activation_key,
            "skill_name": binding.skill_name,
            "skill_version": binding.skill_version,
            "package_digest": binding.package_digest,
            "policy_version": binding.policy_version,
            "policy_decision_id": binding.policy_decision_id,
            "activation": activation.model_dump(mode="json"),
        }
        return CapabilityExecution(
            result={
                "status": "activated",
                "skill_activation_id": activation.skill_activation_id,
                "skill_name": binding.skill_name,
                "skill_version": binding.skill_version,
                "loaded_dependency_ids": dependency_ids,
            },
            state=state,
            events=(NewEvent(type="skill.activated", payload=payload),),
        )

    def _merge_loaded(
        self,
        state: dict[str, Any],
        capabilities: object,
        *,
        allowed_ids: set[str],
    ) -> dict[str, Any]:
        loaded = dict(state.get("loaded", {}))
        schema_bytes = sum(
            _model_tool_size(item) for item in loaded.values() if isinstance(item, dict)
        )
        items = capabilities if isinstance(capabilities, (list, tuple)) else ()
        for item in items:
            if not isinstance(item, dict):
                continue
            capability_id = str(item.get("capability_id", ""))
            if capability_id not in allowed_ids:
                continue
            if capability_id not in loaded and len(loaded) >= self._max_loaded:
                continue
            model_tool = item.get("model_tool")
            if isinstance(model_tool, dict):
                previous = loaded.get(capability_id)
                previous_size = _model_tool_size(previous) if isinstance(previous, dict) else 0
                size = _model_tool_size(item)
                if schema_bytes - previous_size + size > self._max_schema_bytes:
                    continue
                schema_bytes = schema_bytes - previous_size + size
            loaded[capability_id] = dict(item)
        return loaded

    async def _read_resource(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        state: dict[str, Any],
    ) -> CapabilityExecution:
        capability_id = str(call.arguments.get("capability_id", ""))
        loaded = dict(state.get("loaded", {})).get(capability_id)
        if not isinstance(loaded, dict) or loaded.get("kind") not in {
            "resource",
            "resource_template",
        }:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "resource_not_loaded",
                    "summary": "Resource must be loaded before reading.",
                },
                state=state,
            )
        resource = loaded.get("resource")
        if not isinstance(resource, dict):
            raise ValueError("Loaded Resource contract is invalid")
        if loaded["kind"] == "resource":
            uri = str(resource["uri"])
        else:
            uri = _expand_uri_template(
                str(resource["uri_template"]),
                dict(call.arguments.get("arguments", {})),
            )
        try:
            contents = await self._client.read_resource(assignment, uri)
        except (KeyError, NotFoundError):
            loaded_capabilities = dict(state.get("loaded", {}))
            loaded_capabilities.pop(capability_id, None)
            state["loaded"] = loaded_capabilities
            candidates = dict(state.get("candidates", {}))
            candidates.pop(capability_id, None)
            state["candidates"] = candidates
            return CapabilityExecution(
                result={
                    "status": "error",
                    "error_code": "resource_not_found",
                    "summary": (
                        "The Resource disappeared after it was loaded. Search the "
                        "capability catalog again or continue without this Resource."
                    ),
                    "capability_id": capability_id,
                    "retryable": True,
                },
                state=state,
            )
        evidence = _resource_evidence(capability_id, uri, contents)
        contextualized = _contextualize_contents(contents)
        return CapabilityExecution(
            result={"status": "success", "contents": contextualized},
            state=state,
            events=(NewEvent(type="context.resource.used", payload=evidence),),
        )


def _function_tool(name: str, description: str, parameters: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters,
        },
    }


def _result_content(result: dict[str, Any]) -> dict[str, Any]:
    content = result.get("content")
    return dict(content) if isinstance(content, dict) else result


def _model_tool_size(item: dict[str, Any]) -> int:
    model_tool = item.get("model_tool")
    if not isinstance(model_tool, dict):
        return 0
    return len(
        json.dumps(
            model_tool,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )


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


def _expand_uri_template(template: str, arguments: dict[str, Any]) -> str:
    fields = set(_TEMPLATE_FIELD.findall(template))
    if fields != set(arguments):
        raise ValueError("Resource Template arguments do not match the URI fields")
    result = template
    for name in fields:
        result = result.replace(f"{{{name}}}", quote(str(arguments[name]), safe=""))
    if "{" in result or "}" in result:
        raise ValueError("Resource Template expansion is incomplete")
    return result


def _resource_evidence(
    capability_id: str, uri: str, contents: list[dict[str, Any]]
) -> dict[str, Any]:
    first = contents[0] if contents and isinstance(contents[0], dict) else {}
    governance = dict(first["_governance"]) if isinstance(first.get("_governance"), dict) else {}
    meta = dict(first["_meta"]) if isinstance(first.get("_meta"), dict) else {}
    auraclaw = dict(meta["auraclaw"]) if isinstance(meta.get("auraclaw"), dict) else {}
    return {
        "capability_id": capability_id,
        "uri": uri,
        "content_digest": (
            governance.get("contentDigest")
            or first.get("content_digest")
            or auraclaw.get("contentDigest")
        ),
        "source_revision": (
            governance.get("sourceRevision")
            or first.get("source_revision")
            or auraclaw.get("sourceRevision")
        ),
        "classification": (
            governance.get("classification")
            or first.get("classification")
            or auraclaw.get("classification")
            or "internal"
        ),
        "policy_decision_id": (
            governance.get("policyDecisionId")
            or first.get("policy_decision_id")
            or auraclaw.get("policyDecisionId")
        ),
        "artifact_ref": (
            governance.get("artifactRef")
            or first.get("artifact_ref")
            or auraclaw.get("artifactRef")
        ),
    }


def _contextualize_contents(
    contents: list[dict[str, Any]],
    *,
    max_text_chars: int = 32_768,
) -> list[dict[str, Any]]:
    contextualized: list[dict[str, Any]] = []
    remaining = max_text_chars
    for raw in contents:
        item = copy.deepcopy(raw)
        raw_meta = item.get("_governance", item.get("_meta", {}))
        meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}
        auraclaw_meta = meta.get("auraclaw")
        auraclaw = dict(auraclaw_meta) if isinstance(auraclaw_meta, dict) else dict(meta)
        findings = {
            str(value)
            for value in auraclaw.get("securityFindings", item.get("security_findings", ()))
        }
        if "prompt_injection" in findings:
            item.pop("text", None)
            item.pop("blob", None)
            item["text"] = "[Resource content withheld: prompt-injection indicators detected]"
            auraclaw["contextPolicy"] = "withheld"
        elif isinstance(item.get("text"), str):
            text = str(item["text"])
            item["text"] = text[:remaining]
            remaining = max(0, remaining - len(str(item["text"])))
            if len(text) > len(str(item["text"])):
                auraclaw["contextPolicy"] = "truncated"
        meta["auraclaw"] = auraclaw
        item["_meta"] = meta
        contextualized.append(item)
    return contextualized


def _activation_id(assignment: RuntimeAssignment, activation_key: str) -> str:
    value = f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}:{activation_key}"
    return f"ska_{hashlib.sha256(value.encode()).hexdigest()[:32]}"


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(payload).hexdigest()
