from __future__ import annotations

from typing import Any

from auraclaw.contracts.skills import SkillPublicationRecord, SkillPublicationStatus


def publication_values(record: SkillPublicationRecord) -> tuple[object, ...]:
    return (
        record.publication_id,
        record.tenant_id,
        record.publisher,
        record.name,
        record.version,
        record.package_digest,
        record.status.value,
        record.source_id,
        record.revision,
        record.created_by,
        record.updated_by,
        record.created_at,
        record.updated_at,
        record.reason_code,
        (record.revocation_action.value if record.revocation_action is not None else None),
        record.revocation_policy_version,
        record.revocation_policy_decision_id,
    )


def publication_from_row(row: dict[str, Any]) -> SkillPublicationRecord:
    return SkillPublicationRecord(
        publication_id=str(row["publication_id"]),
        tenant_id=str(row["tenant_id"]),
        publisher=str(row["publisher"]),
        name=str(row["name"]),
        version=str(row["version"]),
        package_digest=str(row["package_digest"]),
        status=SkillPublicationStatus(str(row["status"])),
        source_id=row["source_id"],
        revision=int(row["revision"]),
        created_by=str(row["created_by"]),
        updated_by=str(row["updated_by"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        reason_code=row["reason_code"],
        revocation_action=row.get("revocation_action"),
        revocation_policy_version=row.get("revocation_policy_version"),
        revocation_policy_decision_id=row.get("revocation_policy_decision_id"),
    )
