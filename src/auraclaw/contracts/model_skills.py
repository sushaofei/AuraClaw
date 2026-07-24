from __future__ import annotations

from typing import Any

from pydantic import Field

from auraclaw.contracts.internal import ContractModel


class ModelSkillSnapshot(ContractModel):
    """Normalized source material used to build one immutable Skill package."""

    tenant_id: str = Field(min_length=1, max_length=128)
    model: dict[str, Any]
    version: dict[str, Any]
    sections: dict[str, list[dict[str, Any]]]
    source_revision: str = Field(min_length=1, max_length=256)
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
