"""User-authorized, bounded read refresh; never grants tool permissions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ReadRefreshGrant(BaseModel):
    model_config = ConfigDict(extra="forbid")
    capability_id: str = Field(min_length=1, max_length=512)
    max_calls: int = Field(default=2, ge=2, le=10)
    min_interval_seconds: int = Field(default=5, ge=1, le=300)
    duration_seconds: int = Field(default=60, ge=1, le=1800)

    def snapshot(self) -> dict[str, Any]:
        return {
            "capability_id": self.capability_id,
            "max_calls": self.max_calls,
            "min_interval_seconds": self.min_interval_seconds,
            "expires_at": (
                datetime.now(UTC) + timedelta(seconds=self.duration_seconds)
            ).isoformat(),
        }
