from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ApprovalMode(StrEnum):
    REQUEST_APPROVAL = "request_approval"
    AUTO_REVIEW = "auto_review"
    FULL_ACCESS = "full_access"


class InteractionMode(StrEnum):
    STREAMING = "streaming"
    NON_STREAMING = "non_streaming"


class ApprovalConfiguration(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    effective_approval_mode: ApprovalMode | None = None
    interaction_mode: InteractionMode | None = None
    approval_mode_source: Literal["explicit", "default", "inherited", "legacy"] = "legacy"
    approval_mode_revision: int = Field(default=0, ge=0)

    @classmethod
    def resolve(
        cls,
        interaction: InteractionMode,
        mode: ApprovalMode | None = None,
    ) -> ApprovalConfiguration:
        non_interactive = interaction == InteractionMode.NON_STREAMING
        return cls(
            # A caller without an interactive approval channel must never create a
            # Run that can become permanently stuck in waiting_for_human. Explicit
            # approval modes are therefore meaningful only for streaming UI calls.
            effective_approval_mode=(
                ApprovalMode.FULL_ACCESS
                if non_interactive
                else mode or ApprovalMode.REQUEST_APPROVAL
            ),
            interaction_mode=interaction,
            approval_mode_source=(
                "default" if non_interactive else "explicit" if mode is not None else "default"
            ),
            approval_mode_revision=1,
        )

    def public_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
