from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any

from auraclaw.contracts.errors import (
    SkillPromptBudgetExceededError,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.capabilities.content_cache import SkillContentCache
from auraclaw.runtime.capabilities.state import CapabilityState, run_key


class SkillPromptBuilder:
    """Build governed instructions and own prompt budgets and metrics."""

    def __init__(
        self, cache: SkillContentCache, *, max_bytes: int, max_estimated_tokens: int
    ) -> None:
        self._cache = cache
        self._skill_prompt_max_bytes = max_bytes
        self._skill_prompt_max_estimated_tokens = max_estimated_tokens
        self._trusted_message_metrics: dict[tuple[str, str, str], dict[str, float]] = {}

    def metrics(self, assignment: RuntimeAssignment) -> dict[str, float]:
        return dict(self._trusted_message_metrics.get(run_key(assignment), {}))

    def release_run(self, assignment: RuntimeAssignment) -> None:
        self._trusted_message_metrics.pop(run_key(assignment), None)

    async def trusted_messages(
        self,
        assignment: RuntimeAssignment,
        state: CapabilityState,
    ) -> tuple[dict[str, Any], ...]:
        messages: list[dict[str, Any]] = []
        active_identities = self._active_skill_identities(state)
        pending = self._active_skill_prompt_parts(state)
        if not pending:
            self._trusted_message_metrics[run_key(assignment)] = {
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
                self._cache.load(
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
        self._trusted_message_metrics[run_key(assignment)] = {
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

    def prompt_cache_key(self, assignment: RuntimeAssignment, state: CapabilityState) -> str | None:
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

    @staticmethod
    def _active_skill_identities(
        state: CapabilityState,
    ) -> list[tuple[str, str, str, str]]:
        identities: list[tuple[str, str, str, str]] = []
        emitted: set[tuple[str, str, str, str]] = set()
        for item in state.active_skills:
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
        state: CapabilityState,
    ) -> list[tuple[str, str, str, str, str]]:
        parts = [(*identity, "SKILL.md") for identity in cls._active_skill_identities(state)]
        emitted = set(parts)
        for item in state.active_skills:
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


def _estimate_tokens(utf8_bytes: int) -> int:
    # Provider-neutral conservative estimate: CJK is commonly close to one token
    # per three UTF-8 bytes, while English/code generally uses more bytes per token.
    return (utf8_bytes + 2) // 3
