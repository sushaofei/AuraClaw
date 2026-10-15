from __future__ import annotations

import copy
import json
from typing import Any

from auraclaw.runtime.capabilities.state import CapabilityState


class CapabilityLoader:
    """Apply loaded-capability capacity and schema budgets without shared state."""

    def __init__(self, *, max_loaded: int, max_schema_bytes: int) -> None:
        self._max_loaded = max_loaded
        self._max_schema_bytes = max_schema_bytes

    def merge_loaded(
        self,
        state: CapabilityState,
        capabilities: object,
        *,
        allowed_ids: set[str],
    ) -> dict[str, Any]:
        loaded = state.loaded
        # Skill descriptors are governed instruction packages, not executable
        # business capabilities.  Counting the activated Skill itself against the
        # Tool/Resource load limit made a Skill declaring exactly max_loaded tools
        # impossible to activate (24 declared tools + 1 Skill descriptor).
        capacity_used = sum(
            1
            for value in loaded.values()
            if isinstance(value, dict) and value.get("kind") != "skill"
        )
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
            was_loaded = capability_id in loaded
            consumes_capacity = item.get("kind") != "skill"
            if not was_loaded and consumes_capacity and capacity_used >= self._max_loaded:
                continue
            model_tool = item.get("model_tool")
            if isinstance(model_tool, dict):
                previous = loaded.get(capability_id)
                previous_size = _model_tool_size(previous) if isinstance(previous, dict) else 0
                size = _model_tool_size(item)
                if schema_bytes - previous_size + size > self._max_schema_bytes:
                    continue
                schema_bytes = schema_bytes - previous_size + size
            loaded[capability_id] = copy.deepcopy(item)
            if not was_loaded and consumes_capacity:
                capacity_used += 1
        return loaded


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
