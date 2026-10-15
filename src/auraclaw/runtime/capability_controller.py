from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import time
from typing import Any

from auraclaw.contracts.capabilities import RequiredCapabilityRef
from auraclaw.contracts.events import NewEvent
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.authority_queries import authority_request_id
from auraclaw.runtime.capabilities.content_cache import SkillContentCache
from auraclaw.runtime.capabilities.contracts import (
    CAPABILITY_LOAD,
    CAPABILITY_SEARCH,
    RESOURCE_READ,
    SKILL_ACTIVATE,
    SKILL_BINDING_STATUS,
    SKILL_REFERENCE_READ,
    SKILL_RESOLVE_AND_ACTIVATE,
    CapabilityAdmissionError,
    CapabilityExecution,
    CapabilityProgressCallback,
    result_content,
)
from auraclaw.runtime.capabilities.loading import CapabilityLoader
from auraclaw.runtime.capabilities.model_tools import model_tools
from auraclaw.runtime.capabilities.prompts import SkillPromptBuilder
from auraclaw.runtime.capabilities.resources import CapabilityResourceReader
from auraclaw.runtime.capabilities.skills import SkillActivationService
from auraclaw.runtime.capabilities.state import CapabilityState, run_key
from auraclaw.runtime.ports import CapabilityClient, ToolCall
from auraclaw.runtime.tool_argument_guidance import (
    tool_argument_guidance,
)


class RuntimeCapabilityController:
    """Dispatch model-visible operations; components own their state and lifecycle."""

    def __init__(
        self,
        client: CapabilityClient,
        *,
        max_candidates: int = 20,
        max_loaded: int = 24,
        max_searches: int = 4,
        max_loads: int = 4,
        max_schema_bytes: int = 64 * 1024,
        skill_content_cache_max_bytes: int = 16 * 1024 * 1024,
        skill_content_cache_max_entries: int = 1024,
        skill_content_cache_ttl_seconds: float = 900.0,
        skill_prompt_max_bytes: int = 256 * 1024,
        skill_prompt_max_estimated_tokens: int = 65_536,
        skill_reference_read_max_bytes: int = 256 * 1024,
    ) -> None:
        if (
            skill_content_cache_max_bytes < 1
            or skill_content_cache_max_entries < 1
            or skill_content_cache_ttl_seconds <= 0
            or skill_prompt_max_bytes < 1
            or skill_prompt_max_estimated_tokens < 1
            or skill_reference_read_max_bytes < 1
        ):
            raise ValueError("Runtime Skill content and prompt limits must be positive")
        self._client = client
        self._max_candidates = max_candidates
        self._max_loaded = max_loaded
        self._max_searches = max_searches
        self._max_loads = max_loads
        self._loader = CapabilityLoader(max_loaded=max_loaded, max_schema_bytes=max_schema_bytes)
        self._content = SkillContentCache(
            client,
            max_bytes=skill_content_cache_max_bytes,
            max_entries=skill_content_cache_max_entries,
            ttl_seconds=skill_content_cache_ttl_seconds,
        )
        self._prompts = SkillPromptBuilder(
            self._content,
            max_bytes=skill_prompt_max_bytes,
            max_estimated_tokens=skill_prompt_max_estimated_tokens,
        )
        self._skills = SkillActivationService(client, self._loader)
        self._resources = CapabilityResourceReader(
            client, self._content, reference_max_bytes=skill_reference_read_max_bytes
        )
        self._resolution_metrics: dict[tuple[str, str, str], dict[str, float]] = {}

    @staticmethod
    def empty_state() -> dict[str, Any]:
        return CapabilityState.empty().to_checkpoint()

    def model_tools(self, state: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        return model_tools(
            CapabilityState.from_checkpoint(state),
            max_candidates=self._max_candidates,
            max_loaded=self._max_loaded,
        )

    async def activate_trusted_skill(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        *,
        publisher: str | None,
        name: str,
        inputs: dict[str, Any],
        activation_key: str,
        version: str = "*",
        activation_source: str = "trusted_runtime_context",
    ) -> CapabilityExecution:
        return await self._skills.activate_trusted_skill(
            assignment,
            CapabilityState.from_checkpoint(state),
            publisher=publisher,
            name=name,
            inputs=inputs,
            activation_key=activation_key,
            version=version,
            activation_source=activation_source,
        )

    async def binding_disposition(
        self, assignment: RuntimeAssignment, state: dict[str, Any]
    ) -> dict[str, Any] | None:
        return await self._skills.binding_disposition(
            assignment, CapabilityState.from_checkpoint(state)
        )

    async def trusted_messages(
        self, assignment: RuntimeAssignment, state: dict[str, Any]
    ) -> tuple[dict[str, Any], ...]:
        return await self._prompts.trusted_messages(
            assignment, CapabilityState.from_checkpoint(state)
        )

    def trusted_message_metrics(self, assignment: RuntimeAssignment) -> dict[str, float]:
        return {
            **self._prompts.metrics(assignment),
            **self._skills.metrics(assignment),
            **self._resolution_metrics.get(run_key(assignment), {}),
        }

    def prompt_cache_key(self, assignment: RuntimeAssignment, state: dict[str, Any]) -> str | None:
        return self._prompts.prompt_cache_key(assignment, CapabilityState.from_checkpoint(state))

    async def release_run(self, assignment: RuntimeAssignment) -> None:
        await self._content.release_run(assignment)
        self._prompts.release_run(assignment)
        self._skills.release_run(assignment)
        self._resolution_metrics.pop(run_key(assignment), None)

    def restore_skill_events(self, state: dict[str, Any], events: list[Any]) -> None:
        restored = self._skills.restore_skill_events(CapabilityState.from_checkpoint(state), events)
        state.clear()
        state.update(restored)

    def terminal_events(self, state: dict[str, Any], output: str) -> tuple[NewEvent, ...]:
        return self._skills.terminal_events(CapabilityState.from_checkpoint(state), output)

    async def invocation_status(
        self, assignment: RuntimeAssignment, invocation_id: str
    ) -> dict[str, Any]:
        lookup = getattr(self._client, "invocation_status", None)
        if not callable(lookup):
            return {}
        try:
            return dict(await asyncio.wait_for(lookup(assignment, invocation_id), timeout=5))
        except Exception:
            return {}

    async def preload_required(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        """Deterministically load Assignment refs before the model can run."""
        raw_refs = assignment.resource_profile.get("required_capabilities", ())
        if not raw_refs:
            return CapabilityState.from_checkpoint(state).to_checkpoint()
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
                tool_invocation_id=authority_request_id(assignment, "preload"),
                name=CAPABILITY_LOAD,
                version="1",
                arguments={"capability_ids": list(ids)},
                expected_side_effect="read",
            ),
        )
        payload = result_content(result)
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
        current = CapabilityState.from_checkpoint(state).to_checkpoint()
        current["candidates"] = {**dict(current.get("candidates", {})), **returned}
        current["loaded"] = self._loader.merge_loaded(
            CapabilityState.from_checkpoint(current),
            tuple(returned.values()),
            allowed_ids=set(ids),
        )
        omitted = [item for item in ids if item not in current["loaded"]]
        if omitted:
            raise CapabilityAdmissionError(
                "required capability admission failed: runtime load limit for " + ", ".join(omitted)
            )
        current["required_capabilities_preloaded"] = True
        return current

    async def inspect_explicit_capability(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        *,
        canonical_name: str,
    ) -> CapabilityExecution:
        """Load one exact policy-visible Tool or Skill without activating or executing it."""
        current = CapabilityState.from_checkpoint(state).to_checkpoint()
        search = await self.execute(
            assignment,
            ToolCall(
                tool_invocation_id=authority_request_id(assignment, "router-search"),
                name=CAPABILITY_SEARCH,
                version="1",
                arguments={
                    "canonical_name": canonical_name,
                    "kinds": ["tool", "skill"],
                    "limit": 2,
                },
                expected_side_effect="read",
            ),
            current,
        )
        current = search.state
        if search.result.get("status", "success") != "success":
            return CapabilityExecution(
                result={
                    "status": "error",
                    "error_code": search.result.get("error_code", "router_candidate_search_failed"),
                    "candidate_count": 0,
                },
                state=current,
            )
        candidates = [
            item
            for item in dict(current.get("candidates", {})).values()
            if isinstance(item, dict)
            and item.get("canonical_name") == canonical_name
            and item.get("kind") in {"tool", "skill"}
        ]
        if len(candidates) != 1:
            return CapabilityExecution(
                result={
                    "status": "not_found" if not candidates else "ambiguous",
                    "error_code": (
                        "router_candidate_not_found"
                        if not candidates
                        else "router_candidate_ambiguous"
                    ),
                    "candidate_count": len(candidates),
                },
                state=current,
            )
        capability_id = str(candidates[0]["capability_id"])
        loaded = await self.execute(
            assignment,
            ToolCall(
                tool_invocation_id=authority_request_id(assignment, "router-load"),
                name=CAPABILITY_LOAD,
                version="1",
                arguments={"capability_ids": [capability_id]},
                expected_side_effect="read",
            ),
            current,
        )
        descriptor = dict(loaded.state.get("loaded", {})).get(capability_id)
        if (
            loaded.result.get("status", "success") != "success"
            or not isinstance(descriptor, dict)
            or descriptor.get("kind") not in {"tool", "skill"}
            or (
                descriptor.get("kind") == "tool"
                and not isinstance(descriptor.get("model_tool"), dict)
            )
            or (descriptor.get("kind") == "skill" and not isinstance(descriptor.get("skill"), dict))
        ):
            return CapabilityExecution(
                result={
                    "status": "error",
                    "error_code": loaded.result.get("error_code", "router_candidate_load_failed"),
                    "candidate_count": 1,
                },
                state=loaded.state,
            )
        return CapabilityExecution(
            result={
                "status": "prepared",
                "candidate": descriptor,
                "candidate_count": 1,
            },
            state=loaded.state,
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
        """Resolve an exact Skill identity from the authoritative Skill resolver.

        Skill packages are not catalog capabilities.  Keeping this read-only
        inspection on the resolver path prevents stale catalog generations or
        duplicate catalog rows from changing Router authority decisions.
        """
        resolution = await self._client.resolve_skill(
            assignment,
            name=name,
            version=version,
            publisher=publisher,
            active_skill_names=(),
        )
        if resolution.status != "success" or resolution.binding is None:
            return CapabilityExecution(
                result={
                    "status": resolution.status,
                    "error_code": resolution.error_code or "router_skill_resolve_failed",
                    "candidate_count": 0,
                    "summary": resolution.summary,
                },
                state=CapabilityState.from_checkpoint(state).to_checkpoint(),
            )
        binding = resolution.binding
        identity = {
            "publisher": binding.publisher,
            "name": binding.skill_name,
            "version": binding.skill_version,
            "package_digest": binding.package_digest,
        }
        binding_id = (
            "skb_"
            + hashlib.sha256(
                json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        return CapabilityExecution(
            result={
                "status": "prepared",
                "candidate_count": 1,
                "candidate": {
                    "kind": "skill",
                    "canonical_name": (
                        f"{binding.publisher}/{binding.skill_name}"
                        if publisher is not None
                        else binding.skill_name
                    ),
                    "version": binding.skill_version,
                    "content_digest": binding.package_digest,
                    "binding_id": binding_id,
                    "skill": {
                        "publisher": binding.publisher,
                        "name": binding.skill_name,
                        "version": binding.skill_version,
                    },
                },
            },
            state=CapabilityState.from_checkpoint(state).to_checkpoint(),
        )

    async def prepare_explicit_capability(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
        *,
        canonical_name: str,
    ) -> CapabilityExecution:
        """Resolve one exact policy-visible Tool or Skill before the first model turn."""
        inspected = await self.inspect_explicit_capability(
            assignment,
            state,
            canonical_name=canonical_name,
        )
        candidate = inspected.result.get("candidate")
        if inspected.result.get("status") != "prepared" or not isinstance(candidate, dict):
            return inspected
        if candidate.get("kind") != "skill":
            return inspected
        skill = candidate.get("skill")
        if not isinstance(skill, dict):
            return CapabilityExecution(
                result={
                    "status": "error",
                    "error_code": "router_skill_descriptor_invalid",
                    "candidate_count": 1,
                },
                state=inspected.state,
            )
        capability_id = str(candidate["capability_id"])
        activated = await self.activate_trusted_skill(
            assignment,
            inspected.state,
            publisher=(str(skill["publisher"]) if skill.get("publisher") else None),
            name=str(skill.get("name") or canonical_name),
            version=str(skill.get("version") or candidate.get("version") or "*"),
            inputs={},
            activation_key=f"router:{assignment.run_id}:{capability_id}",
            activation_source="agent_router",
        )
        return CapabilityExecution(
            result={
                **activated.result,
                "candidate": candidate,
                "candidate_count": 1,
            },
            state=activated.state,
            events=activated.events,
        )

    async def execute(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        state: dict[str, Any],
        *,
        progress: CapabilityProgressCallback | None = None,
    ) -> CapabilityExecution:
        current = CapabilityState.from_checkpoint(state).to_checkpoint()
        if call.name == SKILL_RESOLVE_AND_ACTIVATE:
            return await self._resolve_and_activate_skill(
                assignment,
                call,
                current,
                progress=progress,
            )

        if call.name == CAPABILITY_SEARCH:
            search_count = CapabilityState.from_checkpoint(current).search_count
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
            payload = result_content(result)
            candidates = {}
            for item in payload.get("capabilities", ()):
                if isinstance(item, dict) and item.get("capability_id"):
                    candidates[str(item["capability_id"])] = dict(item)
            current["candidates"] = dict(list(candidates.items())[: self._max_candidates])
            current["search_count"] = search_count + 1
            return CapabilityExecution(result=result, state=current)

        if call.name == CAPABILITY_LOAD:
            load_count = CapabilityState.from_checkpoint(current).load_count
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
            payload = result_content(result)
            current["loaded"] = self._loader.merge_loaded(
                CapabilityState.from_checkpoint(current),
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
            return await self._skills.activate(
                assignment,
                call,
                CapabilityState.from_checkpoint(current),
                progress=progress,
            )

        if call.name == RESOURCE_READ:
            return await self._resources.read_resource(
                assignment, call, CapabilityState.from_checkpoint(current)
            )

        if call.name == SKILL_REFERENCE_READ:
            return await self._resources.read_skill_reference(
                assignment, call, CapabilityState.from_checkpoint(current)
            )

        matches = [
            item
            for item in dict(current.get("loaded", {})).values()
            if isinstance(item, dict)
            and item.get("kind") == "tool"
            and isinstance(item.get("model_tool"), dict)
            and (
                item.get("model_tool", {}).get("function", {}).get("name") == call.name
                or item.get("canonical_name") == call.name
            )
        ]
        exact = [
            item
            for item in matches
            if item["model_tool"].get("function", {}).get("name") == call.name
        ]
        matches = exact or matches
        if len(matches) > 1:
            return CapabilityExecution(
                result={
                    "status": "denied",
                    "error_code": "ambiguous_capability",
                    "summary": "Use the exact loaded tool alias.",
                },
                state=current,
            )
        loaded_tool = matches[0] if matches else None
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
                "name": str(loaded_tool["model_tool"].get("function", {}).get("name") or call.name),
                "version": str(loaded_tool["version"]),
                "expected_side_effect": (
                    "read" if loaded_tool.get("permission") == "read-only" else "write"
                ),
            }
        )
        result = await self._client.execute(assignment, invocation)
        if (
            result.get("error_code") == "tool_schema_invalid"
            and result.get("side_effect_status") == "not_started"
        ):
            result = copy.deepcopy(result)
            result["metadata"] = {
                **(result.get("metadata") or {}),
                "argument_guidance": tool_argument_guidance(loaded_tool["model_tool"]),
            }
        return CapabilityExecution(result=result, state=current)

    async def _resolve_and_activate_skill(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        state: dict[str, Any],
        *,
        progress: CapabilityProgressCallback | None,
    ) -> CapabilityExecution:
        """Collapse model-driven search, load and activation into one governed call."""
        started = time.monotonic()
        metrics = self._resolution_metrics.setdefault(run_key(assignment), {})
        metrics["skill.resolve_and_activate.count"] = (
            metrics.get("skill.resolve_and_activate.count", 0.0) + 1.0
        )
        outcome = "error"
        try:
            query = str(call.arguments.get("query", "")).strip()
            canonical_name = str(call.arguments.get("canonical_name", "")).strip()
            capability_id = str(call.arguments.get("capability_id", "")).strip()
            if not any((query, canonical_name, capability_id)):
                outcome = "invalid"
                return CapabilityExecution(
                    result={
                        "status": "denied",
                        "error_code": "skill_resolution_query_required",
                        "summary": (
                            "Provide query, canonical_name or capability_id to resolve a Skill."
                        ),
                    },
                    state=state,
                )

            search_arguments: dict[str, Any] = {"kinds": ["skill"], "limit": 3}
            if query:
                search_arguments["query"] = query
            if canonical_name:
                search_arguments["canonical_name"] = canonical_name
            if capability_id:
                search_arguments["capability_id"] = capability_id
            searched = await self.execute(
                assignment,
                ToolCall(
                    tool_invocation_id=f"{call.tool_invocation_id}:search",
                    name=CAPABILITY_SEARCH,
                    version="1",
                    arguments=search_arguments,
                    expected_side_effect="read",
                    idempotency_key=f"{call.tool_invocation_id}:search",
                ),
                state,
                progress=progress,
            )
            if searched.result.get("error_code"):
                outcome = "search_failed"
                return searched

            candidates = [
                item
                for item in dict(searched.state.get("candidates", {})).values()
                if isinstance(item, dict) and item.get("kind") == "skill"
            ]
            if not candidates:
                outcome = "not_found"
                return CapabilityExecution(
                    result={
                        "status": "not_found",
                        "error_code": "skill_not_found",
                        "summary": "No policy-visible Skill matched the request.",
                    },
                    state=searched.state,
                )
            if len(candidates) != 1:
                outcome = "ambiguous"
                return CapabilityExecution(
                    result={
                        "status": "ambiguous",
                        "error_code": "skill_resolution_ambiguous",
                        "summary": "Multiple Skills matched; select one exact capability_id.",
                        "candidates": [
                            {
                                key: item.get(key)
                                for key in (
                                    "capability_id",
                                    "canonical_name",
                                    "version",
                                    "description",
                                )
                            }
                            for item in candidates
                        ],
                    },
                    state=searched.state,
                )

            selected_id = str(candidates[0]["capability_id"])
            loaded = await self.execute(
                assignment,
                ToolCall(
                    tool_invocation_id=f"{call.tool_invocation_id}:load",
                    name=CAPABILITY_LOAD,
                    version="1",
                    arguments={"capability_ids": [selected_id]},
                    expected_side_effect="read",
                    idempotency_key=f"{call.tool_invocation_id}:load",
                ),
                searched.state,
                progress=progress,
            )
            if loaded.result.get("error_code"):
                outcome = "load_failed"
                return loaded
            selected = dict(loaded.state.get("loaded", {})).get(selected_id)
            if not isinstance(selected, dict) or selected.get("kind") != "skill":
                outcome = "load_failed"
                return CapabilityExecution(
                    result={
                        "status": "denied",
                        "error_code": "skill_resolution_load_failed",
                        "summary": "The selected Skill could not be loaded authoritatively.",
                    },
                    state=loaded.state,
                )

            activated = await self._skills.activate(
                assignment,
                ToolCall(
                    tool_invocation_id=call.tool_invocation_id,
                    name=SKILL_ACTIVATE,
                    version="1",
                    arguments={
                        "capability_id": selected_id,
                        "inputs": dict(call.arguments.get("inputs", {})),
                    },
                    expected_side_effect="read",
                    approval_id=call.approval_id,
                    idempotency_key=call.idempotency_key or call.tool_invocation_id,
                ),
                CapabilityState.from_checkpoint(loaded.state),
                progress=progress,
            )
            outcome = str(activated.result.get("status", "error"))
            return CapabilityExecution(
                result={
                    **activated.result,
                    "resolved_capability_id": selected_id,
                    "lifecycle_mode": "resolve_and_activate",
                },
                state=activated.state,
                events=activated.events,
            )
        finally:
            metrics[f"skill.resolve_and_activate.result.{outcome}.count"] = (
                metrics.get(
                    f"skill.resolve_and_activate.result.{outcome}.count",
                    0.0,
                )
                + 1.0
            )
            metrics["skill.resolve_and_activate.latency.seconds"] = time.monotonic() - started


__all__ = [
    "RuntimeCapabilityController",
    "CapabilityExecution",
    "CapabilityProgressCallback",
    "CapabilityAdmissionError",
    "CAPABILITY_SEARCH",
    "CAPABILITY_LOAD",
    "SKILL_ACTIVATE",
    "SKILL_RESOLVE_AND_ACTIVATE",
    "SKILL_REFERENCE_READ",
    "RESOURCE_READ",
    "SKILL_BINDING_STATUS",
]
