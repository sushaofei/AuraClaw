from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.action.skill_admission_maintenance import (
    SkillAdmissionMaintenanceWorker,
)
from auraclaw.action.skill_lifecycle import (
    InMemorySkillLifecycleStore,
    SkillAdmissionAuditRecord,
)
from auraclaw.contracts.errors import SchemaValidationError


def _admission(admission_id: str, occurred_at: datetime) -> SkillAdmissionAuditRecord:
    return SkillAdmissionAuditRecord(
        admission_id=admission_id,
        tenant_id="tenant-a",
        command_id=f"command-{admission_id}",
        operation="publish",
        actor_id="admin-a",
        correlation_id=f"correlation-{admission_id}",
        causation_id=f"causation-{admission_id}",
        publisher="platform",
        name="release.prepare",
        version="1.0.0",
        package_digest=f"sha256:{'a' * 64}",
        artifact_id=None,
        outcome="accepted",
        stage="completed",
        safe_error_code=None,
        duration_ms=5,
        occurred_at=occurred_at,
        content_policy_version="skill-content-v1",
    )


def test_admission_keyset_pagination_window_and_invalid_cursor() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleStore()
        now = datetime.now(UTC)
        for offset in range(3):
            await store.record_admission(
                _admission(f"skad_{offset}", now - timedelta(minutes=offset))
            )

        first = await store.page_admissions("tenant-a", limit=2)
        assert [record.admission_id for record in first.admissions] == [
            "skad_0",
            "skad_1",
        ]
        assert first.next_cursor is not None
        second = await store.page_admissions(
            "tenant-a", cursor=first.next_cursor, limit=2
        )
        assert [record.admission_id for record in second.admissions] == ["skad_2"]
        assert second.next_cursor is None
        windowed = await store.page_admissions(
            "tenant-a", since=now - timedelta(seconds=30)
        )
        assert [record.admission_id for record in windowed.admissions] == ["skad_0"]
        with pytest.raises(SchemaValidationError, match="cursor is invalid"):
            await store.page_admissions("tenant-a", cursor="not-a-cursor")

    asyncio.run(scenario())


def test_admission_retention_cleanup_is_strict_and_bounded() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleStore()
        now = datetime.now(UTC)
        await store.record_admission(_admission("skad_old_1", now - timedelta(days=91)))
        await store.record_admission(_admission("skad_old_2", now - timedelta(days=90, seconds=1)))
        await store.record_admission(_admission("skad_boundary", now - timedelta(days=90)))
        worker = SkillAdmissionMaintenanceWorker(
            store,
            retention=timedelta(days=90),
            batch_size=1,
            now=lambda: now,
        )

        first = await worker.run_once()
        assert first.deleted == 1
        second = await worker.run_once()
        assert second.deleted == 1
        third = await worker.run_once()
        assert third.deleted == 0
        remaining = await store.page_admissions("tenant-a")
        assert [record.admission_id for record in remaining.admissions] == [
            "skad_boundary"
        ]

    asyncio.run(scenario())
