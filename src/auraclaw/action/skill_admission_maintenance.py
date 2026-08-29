from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from auraclaw.action.skill_lifecycle import SkillLifecycleStore


@dataclass(frozen=True)
class SkillAdmissionMaintenanceResult:
    cutoff: datetime
    deleted: int


class SkillAdmissionMaintenanceWorker:
    def __init__(
        self,
        lifecycle: SkillLifecycleStore,
        *,
        retention: timedelta,
        batch_size: int = 1000,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if retention <= timedelta(0):
            raise ValueError("Skill admission retention must be positive")
        if batch_size < 1:
            raise ValueError("Skill admission cleanup batch size must be positive")
        self._lifecycle = lifecycle
        self._retention = retention
        self._batch_size = batch_size
        self._now = now or (lambda: datetime.now(UTC))

    async def run_once(self) -> SkillAdmissionMaintenanceResult:
        now = self._now()
        if now.tzinfo is None:
            raise ValueError("Skill admission maintenance clock must be timezone-aware")
        cutoff = now - self._retention
        deleted = await self._lifecycle.delete_admissions_before(
            cutoff, limit=self._batch_size
        )
        return SkillAdmissionMaintenanceResult(cutoff=cutoff, deleted=deleted)
