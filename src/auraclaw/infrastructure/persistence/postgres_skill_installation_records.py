from __future__ import annotations

from typing import Any

from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillRevocationAction,
)


def installation_values(record: SkillInstallationRecord) -> tuple[object, ...]:
    return (
        record.installation_id,
        record.tenant_id,
        record.publisher,
        record.name,
        record.version_constraint,
        record.pinned_package_digest,
        record.status.value,
        record.auto_upgrade,
        record.revision,
        record.created_by,
        record.updated_by,
        record.created_at,
        record.updated_at,
        record.reason_code,
        (
            record.uninstall_action.value
            if record.uninstall_action is not None
            else None
        ),
        record.uninstall_policy_version,
        record.uninstall_policy_decision_id,
    )


def installation_from_row(row: dict[str, Any]) -> SkillInstallationRecord:
    return SkillInstallationRecord(
        installation_id=str(row["installation_id"]),
        tenant_id=str(row["tenant_id"]),
        publisher=str(row["publisher"]),
        name=str(row["name"]),
        version_constraint=str(row["version_constraint"]),
        pinned_package_digest=row["pinned_package_digest"],
        status=SkillInstallationStatus(str(row["status"])),
        auto_upgrade=bool(row["auto_upgrade"]),
        revision=int(row["revision"]),
        created_by=str(row["created_by"]),
        updated_by=str(row["updated_by"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        reason_code=row["reason_code"],
        uninstall_action=(
            SkillRevocationAction(str(row["uninstall_action"]))
            if row.get("uninstall_action") is not None
            else None
        ),
        uninstall_policy_version=row.get("uninstall_policy_version"),
        uninstall_policy_decision_id=row.get("uninstall_policy_decision_id"),
    )
