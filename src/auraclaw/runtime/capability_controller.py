from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from auraclaw.contracts.capabilities import RequiredCapabilityRef
from auraclaw.contracts.errors import (
    NotFoundError,
    SchemaValidationError,
    SkillPromptBudgetExceededError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.skills import SkillActivation, effective_skill_role
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.authority_queries import authority_request_id, binding_disposition_result
from auraclaw.runtime.ports import CapabilityClient, ToolCall
from auraclaw.runtime.skill_workflow import (
    RuntimeSkillWorkflowExecutor,
    WorkflowStepProgress,
)

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


CapabilityProgressCallback = Callable[[dict[str, Any], tuple[NewEvent, ...]], Awaitable[None]]


class CapabilityAdmissionError(RuntimeError):
    """Raised before the first model call when a fixed capability cannot be loaded."""


@dataclass(frozen=True)
class _RunSkillContentEntry:
    text: str
    size: int
    expires_at: float


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
        skill_content_cache_max_bytes: int = 16 * 1024 * 1024,
        skill_content_cache_max_entries: int = 1024,
        skill_content_cache_ttl_seconds: float = 900.0,
        skill_prompt_max_bytes: int = 256 * 1024,
        skill_prompt_max_estimated_tokens: int = 65_536,
    ) -> None:
        if (
            skill_content_cache_max_bytes < 1
            or skill_content_cache_max_entries < 1
            or skill_content_cache_ttl_seconds <= 0
            or skill_prompt_max_bytes < 1
            or skill_prompt_max_estimated_tokens < 1
        ):
            raise ValueError("Runtime Skill content and prompt limits must be positive")
        self._client = client
        self._workflow_executor = RuntimeSkillWorkflowExecutor(client)
        self._max_candidates = max_candidates
        self._max_loaded = max_loaded
        self._max_searches = max_searches
        self._max_loads = max_loads
        self._max_schema_bytes = max_schema_bytes
        self._skill_content_cache_max_bytes = skill_content_cache_max_bytes
        self._skill_content_cache_max_entries = skill_content_cache_max_entries
        self._skill_content_cache_ttl_seconds = skill_content_cache_ttl_seconds
        self._skill_prompt_max_bytes = skill_prompt_max_bytes
        self._skill_prompt_max_estimated_tokens = skill_prompt_max_estimated_tokens
        self._skill_content_cache: OrderedDict[
            tuple[str, str, str, str, str], _RunSkillContentEntry
        ] = OrderedDict()
        self._skill_content_loads: dict[tuple[str, str, str, str, str], asyncio.Task[str]] = {}
        self._skill_content_cache_bytes = 0
        self._skill_content_lock = asyncio.Lock()
        self._trusted_message_metrics: dict[tuple[str, str, str], dict[str, float]] = {}
        self._workflow_metrics: dict[tuple[str, str, str], dict[str, float]] = {}

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
                "external data, an action, or a governed Skill. "
                "To list Skills use kinds=[\"skill\"] "
                "and query=\"\"; use canonical_name for a known exact name. An empty result "
                "does not prove no Skill is installed; "
                "only currently executable matches are listed.",
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
                tool_invocation_id=authority_request_id(assignment, "preload"),
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
                "required capability admission failed: runtime load limit for " + ", ".join(omitted)
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

    async def trusted_messages(
        self,
        assignment: RuntimeAssignment,
        state: dict[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        messages: list[dict[str, Any]] = []
        active_identities = self._active_skill_identities(state)
        pending = self._active_skill_prompt_parts(state)
        if not pending:
            self._trusted_message_metrics[self._run_key(assignment)] = {
                "skill.runtime.active.count": 0.0,
                "skill.runtime.prompt.bytes": 0.0,
                "skill.runtime.prompt.estimated_tokens": 0.0,
                "skill.runtime.content_cache.hit.count": 0.0,
                "skill.runtime.content_cache.miss.count": 0.0,
                "skill.runtime.trusted_messages.latency.seconds": 0.0,
                "skill.runtime.prompt.rejected.count": 0.0,
            }
            return tuple(messages)

        started = time.monotonic()
        loaded = await asyncio.gather(
            *(
                self._load_skill_text(
                    assignment,
                    publisher=item[0],
                    name=item[1],
                    version=item[2],
                    package_digest=item[3],
                    path=item[4],
                )
                for item in pending
            )
        )
        cache_hits = 0
        contents: list[str] = []
        for item, (text, cache_hit) in zip(pending, loaded, strict=True):
            cache_hits += int(cache_hit)
            if text:
                identity = item[:4]
                content = (
                    "Activated signed AuraClaw Skill "
                    f"{identity[0]}/{identity[1]}@{identity[2]} "
                    f"(digest {identity[3]}, part {item[4]}):\n{text}"
                )
                contents.append(content)
        prompt_bytes = sum(len(content.encode()) for content in contents)
        estimated_tokens = _estimate_tokens(prompt_bytes)
        rejected = (
            prompt_bytes > self._skill_prompt_max_bytes
            or estimated_tokens > self._skill_prompt_max_estimated_tokens
        )
        self._trusted_message_metrics[self._run_key(assignment)] = {
            "skill.runtime.active.count": float(len(active_identities)),
            "skill.runtime.prompt.bytes": float(prompt_bytes),
            "skill.runtime.prompt.estimated_tokens": float(estimated_tokens),
            "skill.runtime.content_cache.hit.count": float(cache_hits),
            "skill.runtime.content_cache.miss.count": float(len(pending) - cache_hits),
            "skill.runtime.trusted_messages.latency.seconds": (time.monotonic() - started),
            "skill.runtime.prompt.rejected.count": float(rejected),
        }
        if rejected:
            raise SkillPromptBudgetExceededError(
                "Activated Skill instructions exceed the Runtime prompt budget",
                detail=(
                    f"prompt_bytes={prompt_bytes},max_bytes={self._skill_prompt_max_bytes},"
                    f"estimated_tokens={estimated_tokens},"
                    f"max_estimated_tokens={self._skill_prompt_max_estimated_tokens}"
                ),
            )
        messages.extend({"role": "system", "content": content} for content in contents)
        return tuple(messages)

    def trusted_message_metrics(self, assignment: RuntimeAssignment) -> dict[str, float]:
        key = self._run_key(assignment)
        return {
            **self._trusted_message_metrics.get(key, {}),
            **self._workflow_metrics.get(key, {}),
        }

    def prompt_cache_key(self, assignment: RuntimeAssignment, state: dict[str, Any]) -> str | None:
        identities = self._active_skill_identities(state)
        if not identities:
            return None
        payload = json.dumps(
            {
                "tenant_id": assignment.tenant_id,
                "skill_digests": [identity[3] for identity in identities],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return f"auraclaw-skill:{hashlib.sha256(payload).hexdigest()[:48]}"

    async def release_run(self, assignment: RuntimeAssignment) -> None:
        run_prefix = self._run_key(assignment)
        async with self._skill_content_lock:
            tasks = [
                task for key, task in self._skill_content_loads.items() if key[:3] == run_prefix
            ]
            for task in tasks:
                task.cancel()
            self._remove_run_entries_locked(run_prefix)
            self._trusted_message_metrics.pop(run_prefix, None)
            self._workflow_metrics.pop(run_prefix, None)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            async with self._skill_content_lock:
                self._remove_run_entries_locked(run_prefix)

    async def _load_skill_text(
        self,
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
        package_digest: str,
        path: str,
    ) -> tuple[str, bool]:
        key = (*self._run_key(assignment), package_digest, path)
        now = time.monotonic()
        async with self._skill_content_lock:
            entry = self._skill_content_cache.get(key)
            if entry is not None and entry.expires_at > now:
                self._skill_content_cache.move_to_end(key)
                return entry.text, True
            if entry is not None:
                self._skill_content_cache.pop(key)
                self._skill_content_cache_bytes -= entry.size
            task = self._skill_content_loads.get(key)
            if task is None:
                task = asyncio.create_task(
                    self._fetch_skill_text(
                        key,
                        assignment,
                        publisher=publisher,
                        name=name,
                        version=version,
                        path=path,
                    )
                )
                self._skill_content_loads[key] = task
        return await asyncio.shield(task), False

    async def _fetch_skill_text(
        self,
        key: tuple[str, str, str, str, str],
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
        path: str,
    ) -> str:
        try:
            parts = await self._client.load_skill_part(
                assignment,
                publisher=publisher,
                name=name,
                version=version,
                path=path,
            )
            text = next(
                (
                    str(part["text"])
                    for part in parts
                    if isinstance(part, dict) and isinstance(part.get("text"), str)
                ),
                "",
            )
            size = len(text.encode())
            async with self._skill_content_lock:
                if size <= self._skill_content_cache_max_bytes:
                    self._skill_content_cache[key] = _RunSkillContentEntry(
                        text=text,
                        size=size,
                        expires_at=(time.monotonic() + self._skill_content_cache_ttl_seconds),
                    )
                    self._skill_content_cache.move_to_end(key)
                    self._skill_content_cache_bytes += size
                    while self._skill_content_cache and (
                        len(self._skill_content_cache) > self._skill_content_cache_max_entries
                        or self._skill_content_cache_bytes > self._skill_content_cache_max_bytes
                    ):
                        _evicted_key, evicted = self._skill_content_cache.popitem(last=False)
                        self._skill_content_cache_bytes -= evicted.size
            return text
        finally:
            async with self._skill_content_lock:
                current = asyncio.current_task()
                if self._skill_content_loads.get(key) is current:
                    self._skill_content_loads.pop(key, None)

    @staticmethod
    def _run_key(assignment: RuntimeAssignment) -> tuple[str, str, str]:
        return assignment.tenant_id, assignment.session_id, assignment.run_id

    def _remove_run_entries_locked(self, run_prefix: tuple[str, str, str]) -> None:
        keys = [key for key in self._skill_content_cache if key[:3] == run_prefix]
        for key in keys:
            entry = self._skill_content_cache.pop(key)
            self._skill_content_cache_bytes -= entry.size

    @staticmethod
    def _active_skill_identities(
        state: dict[str, Any],
    ) -> list[tuple[str, str, str, str]]:
        identities: list[tuple[str, str, str, str]] = []
        emitted: set[tuple[str, str, str, str]] = set()
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
                if identity not in emitted:
                    emitted.add(identity)
                    identities.append(identity)
        return identities

    @classmethod
    def _active_skill_prompt_parts(
        cls,
        state: dict[str, Any],
    ) -> list[tuple[str, str, str, str, str]]:
        parts = [(*identity, "SKILL.md") for identity in cls._active_skill_identities(state)]
        emitted = set(parts)
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict) or not isinstance(item.get("binding"), dict):
                continue
            binding = item["binding"]
            identity = (
                str(binding["publisher"]),
                str(binding["skill_name"]),
                str(binding["skill_version"]),
                str(binding["package_digest"]),
            )
            for path in item.get("model_reference_paths", ()):
                candidate = (*identity, str(path))
                if candidate not in emitted:
                    emitted.add(candidate)
                    parts.append(candidate)
        return parts

    async def execute(
        self,
        assignment: RuntimeAssignment,
        call: ToolCall,
        state: dict[str, Any],
        *,
        progress: CapabilityProgressCallback | None = None,
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
            return await self._activate_skill(
                assignment,
                call,
                current,
                progress=progress,
            )

        if call.name == RESOURCE_READ:
            return await self._read_resource(assignment, call, current)

        matches = [
            item for item in dict(current.get("loaded", {})).values()
            if isinstance(item, dict) and item.get("kind") == "tool"
            and isinstance(item.get("model_tool"), dict)
            and (item.get("model_tool", {}).get("function", {}).get("name") == call.name
                 or item.get("canonical_name") == call.name)
        ]
        exact = [item for item in matches
                 if item["model_tool"].get("function", {}).get("name") == call.name]
        matches = exact or matches
        if len(matches) > 1:
            return CapabilityExecution(
                result={"status": "denied", "error_code": "ambiguous_capability",
                        "summary": "Use the exact loaded tool alias."}, state=current,
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
        return CapabilityExecution(
            result=await self._client.execute(assignment, invocation),
            state=current,
        )

    def restore_skill_events(self, state: dict[str, Any], events: list[Any]) -> None:
        active = list(state.get("active_skills", ()))
        known = {item.get("activation", {}).get("skill_activation_id")
                 for item in active if isinstance(item, dict)}
        for event in events:
            activation = event.payload.get("activation")
            if (event.type == "skill.activated" and isinstance(activation, dict)
                    and activation.get("skill_activation_id") not in known):
                active.append({"activation": activation, "binding": activation["binding"]})
                known.add(activation.get("skill_activation_id"))
        state["active_skills"] = active
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict) or not isinstance(item.get("activation"), dict):
                continue
            activation = item["activation"]
            relevant = [event for event in events if event.payload.get("skill_activation_id")
                        == activation.get("skill_activation_id")]
            activated = next((event for event in relevant if event.type == "skill.activated"), None)
            if not item.get("workflow_deadline") and activated is not None:
                item["workflow_deadline"] = (activated.occurred_at + timedelta(
                    seconds=activation["binding"]["timeout_seconds"]
                )).isoformat()
            terminal = next((event for event in relevant if event.type in {
                "skill.completed", "skill.failed", "skill.cancelled",
            }), None)
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

    def terminal_events(self, state: dict[str, Any], output: str) -> tuple[NewEvent, ...]:
        events: list[NewEvent] = []
        for item in state.get("active_skills", ()):
            if not isinstance(item, dict):
                continue
            activation = item.get("activation")
            if not isinstance(activation, dict):
                continue
            if (item.get("workflow_status") is not None
                    or activation.get("binding", {}).get("resolved_workflow") is not None):
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
        input_digest = f"sha256:{_digest(inputs)}"
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
                    result={"status": existing["workflow_status"],
                            "error_code": existing.get("workflow_error_code", "workflow_terminal"),
                            "skill_activation_id": activation.skill_activation_id},
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
            resolve_metrics = self._workflow_metrics.setdefault(self._run_key(assignment), {})
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
                    },
                )
            )

        if not events and progress is not None and binding.resolved_workflow is not None:
            events.append(NewEvent(type="skill.activated", payload={
                "skill_activation_id": activation.skill_activation_id,
                "activation_key": activation.activation_key,
                "skill_name": binding.skill_name, "skill_version": binding.skill_version,
                "package_digest": binding.package_digest, "policy_version": binding.policy_version,
                "policy_decision_id": binding.policy_decision_id,
                "workflow_digest": binding.resolved_workflow.workflow_digest,
                "activation": activation.model_dump(mode="json"),
            }))
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
                    result={"status": "denied", "error_code": "skill_dependency_load_failed",
                            "missing_capability_ids": missing}, state=state, events=tuple(events),
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
            if progress is not None:
                await progress(state, ())

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
            completed_steps=tuple(existing.get("workflow_completed_steps", ())),
            deadline=datetime.fromisoformat(str(existing["workflow_deadline"])),
            pending_invocation_id=(existing.get("workflow_pending_invocation_id")
                                   if existing.get("workflow_status") == "unknown" else None),
        )
        metrics = self._workflow_metrics.setdefault(self._run_key(assignment), {})
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
        if workflow_result.status == "unknown":
            existing["workflow_pending_step"] = workflow_result.pending_step_id
            existing["workflow_pending_invocation_id"] = workflow_result.pending_invocation_id
            return CapabilityExecution(
                result={"status": "unknown", "error_code": workflow_result.error_code,
                        "skill_activation_id": activation.skill_activation_id,
                        "pending_invocation_id": workflow_result.pending_invocation_id},
                state=state, events=tuple(events),
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
            invocation_id = f"dep_{_digest({'activation': activation_call_id})[:24]}_{batch_index}"
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
            payload = _result_content(result)
            hydrated = self._merge_loaded(
                state,
                payload.get("capabilities", ()),
                allowed_ids=set(batch),
            )
            state["loaded"] = hydrated
            missing.extend(sorted(set(batch).difference(hydrated)))
        return missing

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


def _estimate_tokens(utf8_bytes: int) -> int:
    # Provider-neutral conservative estimate: CJK is commonly close to one token
    # per three UTF-8 bytes, while English/code generally uses more bytes per token.
    return (utf8_bytes + 2) // 3


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(payload).hexdigest()
