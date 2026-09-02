from __future__ import annotations

from typing import Any

from auraclaw.action.skill_lifecycle import SkillSourceLease
from auraclaw.contracts.skills import (
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillSourceSyncState,
)
from auraclaw.infrastructure.persistence.postgres_common import json_dumps, json_loads


def source_lease_from_row(row: dict[str, Any]) -> SkillSourceLease:
    return SkillSourceLease(
        tenant_id=str(row["tenant_id"]),
        source_id=str(row["source_id"]),
        owner=str(row["owner"]),
        fencing_token=int(row["fencing_token"]),
        expires_at=row["expires_at"],
    )


def source_values(record: SkillSourceRecord) -> tuple[object, ...]:
    return (
        record.source_id,
        record.tenant_id,
        record.kind.value,
        record.desired_state.value,
        json_dumps(record.publisher_allowlist),
        record.credential_ref,
        json_dumps(record.config_metadata),
        record.revision,
        record.created_by,
        record.updated_by,
        record.created_at,
        record.updated_at,
        record.priority,
    )


def source_from_row(row: dict[str, Any]) -> SkillSourceRecord:
    return SkillSourceRecord(
        source_id=str(row["source_id"]),
        tenant_id=str(row["tenant_id"]),
        kind=SkillSourceKind(str(row["kind"])),
        desired_state=SkillSourceDesiredState(str(row["desired_state"])),
        publisher_allowlist=tuple(json_loads(row["publisher_allowlist"])),
        credential_ref=row["credential_ref"],
        config_metadata=dict(json_loads(row["config_metadata"])),
        priority=int(row.get("priority", 0)),
        revision=int(row["revision"]),
        created_by=str(row["created_by"]),
        updated_by=str(row["updated_by"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def sync_state_from_row(row: dict[str, Any]) -> SkillSourceSyncState:
    return SkillSourceSyncState(
        source_id=str(row["source_id"]),
        tenant_id=str(row["tenant_id"]),
        generation=int(row["generation"]),
        cursor=row["cursor"],
        complete_snapshot=bool(row["complete_snapshot"]),
        last_success_at=row["last_success_at"],
        last_attempt_at=row["last_attempt_at"],
        consecutive_failures=int(row["consecutive_failures"]),
        safe_error_code=row["safe_error_code"],
    )
