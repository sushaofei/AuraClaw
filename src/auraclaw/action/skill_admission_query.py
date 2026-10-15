from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.action.skill_lifecycle import SkillAdmissionMetricRecord, SkillAdmissionPage


class SkillAdmissionReader(Protocol):
    async def page_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        since: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SkillAdmissionPage: ...

    async def admission_metrics(
        self, tenant_id: str, *, since: datetime | None = None
    ) -> tuple[SkillAdmissionMetricRecord, ...]: ...


@dataclass(frozen=True)
class SkillAdmissionMetricsPolicy:
    window_hours: int = 24
    quarantine_alert_ratio: float = 0.25
    quarantine_alert_min_samples: int = 20

    def __post_init__(self) -> None:
        if not 1 <= self.window_hours <= 2160:
            raise ValueError("Skill admission metrics window must be between 1 and 2160 hours")
        if not 0 <= self.quarantine_alert_ratio <= 1:
            raise ValueError("Skill admission quarantine alert ratio must be between 0 and 1")
        if self.quarantine_alert_min_samples < 1:
            raise ValueError("Skill admission quarantine alert minimum samples must be positive")


def utc_now() -> datetime:
    return datetime.now(UTC)


class SkillAdmissionMetricsService:
    """Own bounded admission windows and quarantine alert evaluation."""

    def __init__(
        self,
        reader: SkillAdmissionReader,
        policy: SkillAdmissionMetricsPolicy,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._reader = reader
        self._policy = policy
        self._clock = clock

    async def metrics(self, tenant_id: str, *, window_hours: int | None = None) -> dict[str, Any]:
        if window_hours is not None and not 1 <= window_hours <= 2160:
            raise ValueError("Skill admission metrics window must be between 1 and 2160 hours")
        selected_window = window_hours or self._policy.window_hours
        observed_at = self._clock()
        since = observed_at - timedelta(hours=selected_window)
        groups = await self._reader.admission_metrics(tenant_id, since=since)
        metrics: list[dict[str, Any]] = []
        for group in groups:
            labels = {
                "outcome": group.outcome,
                "content_policy_version": group.content_policy_version,
                "window_hours": str(selected_window),
            }
            metrics.extend(
                (
                    {
                        "name": "skill.admission.count",
                        "value": group.count,
                        "labels": labels,
                    },
                    {
                        "name": "skill.admission.duration_ms.average",
                        "value": group.average_duration_ms,
                        "labels": labels,
                    },
                )
            )
        sample_count = sum(group.count for group in groups)
        quarantined_count = sum(group.count for group in groups if group.outcome == "quarantined")
        quarantine_ratio = quarantined_count / sample_count if sample_count else 0.0
        ratio_labels = {"window_hours": str(selected_window)}
        metrics.append(
            {
                "name": "skill.admission.quarantine_ratio",
                "value": quarantine_ratio,
                "labels": ratio_labels,
            }
        )
        alert_status = (
            "insufficient_data"
            if sample_count < self._policy.quarantine_alert_min_samples
            else "firing"
            if quarantine_ratio > self._policy.quarantine_alert_ratio
            else "ok"
        )
        return {
            "window": {
                "hours": selected_window,
                "since": since.isoformat(),
                "observed_at": observed_at.isoformat(),
            },
            "metrics": metrics,
            "alerts": [
                {
                    "rule": "skill.admission.quarantine_ratio",
                    "status": alert_status,
                    "value": quarantine_ratio,
                    "threshold": self._policy.quarantine_alert_ratio,
                    "sample_count": sample_count,
                    "minimum_samples": self._policy.quarantine_alert_min_samples,
                }
            ],
        }
