"""Own checkpoint snapshots without changing their persisted wire format."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from auraclaw.control.ports import RuntimeAssignment


def run_key(assignment: RuntimeAssignment) -> tuple[str, str, str]:
    return assignment.tenant_id, assignment.session_id, assignment.run_id


class CapabilityState:
    """Typed reads over an owned legacy checkpoint, preserving unknown fields.

    Each import/export copies nested data. Missing fields remain missing on export;
    defaults apply only to typed reads, so old checkpoints round-trip unchanged.
    """

    def __init__(self, checkpoint: Mapping[str, Any]) -> None:
        self._checkpoint = copy.deepcopy(dict(checkpoint))

    @classmethod
    def from_checkpoint(cls, checkpoint: Mapping[str, Any]) -> CapabilityState:
        return cls(checkpoint)

    @classmethod
    def empty(cls) -> CapabilityState:
        return cls(
            {
                "candidates": {},
                "loaded": {},
                "active_skills": [],
                "search_count": 0,
                "load_count": 0,
            }
        )

    def to_checkpoint(self) -> dict[str, Any]:
        return copy.deepcopy(self._checkpoint)

    @property
    def candidates(self) -> dict[str, Any]:
        return copy.deepcopy(dict(self._checkpoint.get("candidates", {})))

    @property
    def loaded(self) -> dict[str, Any]:
        return copy.deepcopy(dict(self._checkpoint.get("loaded", {})))

    @property
    def active_skills(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            copy.deepcopy(item)
            for item in self._checkpoint.get("active_skills", ())
            if isinstance(item, dict)
        )

    @property
    def search_count(self) -> int:
        return int(self._checkpoint.get("search_count", 0))

    @property
    def load_count(self) -> int:
        return int(self._checkpoint.get("load_count", 0))
