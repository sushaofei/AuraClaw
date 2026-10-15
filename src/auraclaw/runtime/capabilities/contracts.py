from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from auraclaw.contracts.events import NewEvent

CAPABILITY_SEARCH = "auraclaw.capabilities.search"

CAPABILITY_LOAD = "auraclaw.capabilities.load"

SKILL_ACTIVATE = "auraclaw.skills.activate"

SKILL_RESOLVE_AND_ACTIVATE = "auraclaw.skills.resolve_and_activate"

SKILL_REFERENCE_READ = "auraclaw.skills.reference.read"

RESOURCE_READ = "auraclaw.resources.read"

SKILL_BINDING_STATUS = "auraclaw.skills.binding-status"


@dataclass(frozen=True)
class CapabilityExecution:
    result: dict[str, Any]
    state: dict[str, Any]
    events: tuple[NewEvent, ...] = ()


CapabilityProgressCallback = Callable[[dict[str, Any], tuple[NewEvent, ...]], Awaitable[None]]


class CapabilityAdmissionError(RuntimeError):
    """Raised before the first model call when a fixed capability cannot be loaded."""


def result_content(result: dict[str, Any]) -> dict[str, Any]:
    content = result.get("content")
    return dict(content) if isinstance(content, dict) else result


def digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(payload).hexdigest()
