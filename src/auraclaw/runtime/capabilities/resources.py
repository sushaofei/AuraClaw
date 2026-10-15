from __future__ import annotations

import copy
import hashlib
import re
from typing import Any
from urllib.parse import quote

from auraclaw.contracts.errors import (
    NotFoundError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.capabilities.content_cache import SkillContentCache
from auraclaw.runtime.capabilities.contracts import (
    CapabilityExecution,
)
from auraclaw.runtime.capabilities.state import CapabilityState
from auraclaw.runtime.ports import CapabilityClient, ToolCall
from auraclaw.runtime.tool_argument_guidance import (
    tool_argument_guidance,
)


class CapabilityResourceReader:
    """Read declared references and governed resources from owned snapshots."""

    def __init__(
        self, client: CapabilityClient, cache: SkillContentCache, *, reference_max_bytes: int
    ) -> None:
        self._client = client
        self._cache = cache
        self._skill_reference_read_max_bytes = reference_max_bytes

    async def read_skill_reference(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        snapshot: CapabilityState,
    ) -> CapabilityExecution:
        state = snapshot.to_checkpoint()
        activation_id = str(call.arguments.get("skill_activation_id", ""))
        path = str(call.arguments.get("path", ""))
        active = next(
            (
                item
                for item in state.get("active_skills", ())
                if isinstance(item, dict)
                and isinstance(item.get("activation"), dict)
                and item["activation"].get("skill_activation_id") == activation_id
            ),
            None,
        )
        if active is None:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "skill_activation_not_active",
                    "summary": "Reference reads are limited to a currently active Skill.",
                },
                state=state,
            )
        requirement = next(
            (
                item
                for item in active.get("reference_requirements", ())
                if isinstance(item, dict) and item.get("path") == path
            ),
            None,
        )
        if requirement is None:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "skill_reference_not_declared",
                    "summary": "The active Skill manifest does not declare this reference path.",
                },
                state=state,
            )
        if requirement.get("preload") is True:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "skill_reference_already_preloaded",
                    "summary": "This reference is already present in trusted Skill context.",
                },
                state=state,
            )
        binding = active.get("binding")
        if not isinstance(binding, dict):
            raise ValueError("Active Skill binding is invalid")
        text, cache_hit = await self._cache.load(
            assignment,
            publisher=str(binding["publisher"]),
            name=str(binding["skill_name"]),
            version=str(binding["skill_version"]),
            package_digest=str(binding["package_digest"]),
            path=path,
        )
        size = len(text.encode())
        declared_limit = int(requirement.get("max_bytes", 64 * 1024))
        effective_limit = min(declared_limit, self._skill_reference_read_max_bytes)
        if size > effective_limit:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "skill_reference_too_large",
                    "summary": "The declared Skill reference exceeds the Runtime read limit.",
                    "metadata": {
                        "path": path,
                        "size_bytes": size,
                        "max_bytes": effective_limit,
                    },
                },
                state=state,
            )
        content_digest = hashlib.sha256(text.encode()).hexdigest()
        return CapabilityExecution(
            result={
                "status": "success",
                "content": {
                    "skill_activation_id": activation_id,
                    "path": path,
                    "media_type": str(requirement.get("media_type", "text/markdown")),
                    "text": text,
                    "size_bytes": size,
                    "sha256": content_digest,
                },
                "metadata": {"cache_hit": cache_hit},
            },
            state=state,
            events=(
                NewEvent(
                    type="context.skill.reference.used",
                    payload={
                        "skill_activation_id": activation_id,
                        "package_digest": str(binding["package_digest"]),
                        "path": path,
                        "media_type": str(requirement.get("media_type", "text/markdown")),
                        "size_bytes": size,
                        "sha256": content_digest,
                    },
                ),
            ),
        )

    async def read_resource(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        snapshot: CapabilityState,
    ) -> CapabilityExecution:
        state = snapshot.to_checkpoint()
        capability_id = str(call.arguments.get("capability_id", ""))
        loaded = CapabilityState.from_checkpoint(state).loaded.get(capability_id)
        if not isinstance(loaded, dict) or loaded.get("kind") not in {
            "resource",
            "resource_template",
        }:
            tool = loaded.get("model_tool") if isinstance(loaded, dict) else None
            guidance = tool_argument_guidance(tool) if isinstance(tool, dict) else None
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "resource_not_loaded",
                    "summary": (
                        "This capability is a Tool. Call its exact loaded Tool function directly."
                        if guidance
                        else "Resource must be loaded before reading."
                    ),
                    **({"metadata": {"argument_guidance": guidance}} if guidance else {}),
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
            loaded_capabilities = CapabilityState.from_checkpoint(state).loaded
            loaded_capabilities.pop(capability_id, None)
            state["loaded"] = loaded_capabilities
            candidates = CapabilityState.from_checkpoint(state).candidates
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


_TEMPLATE_FIELD = re.compile(r"\{([A-Za-z0-9_.-]+)\}")


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
