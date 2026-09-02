from __future__ import annotations

from typing import Any

from auraclaw.contracts.skills import (
    SkillManifest,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.persistence.postgres_common import json_loads


def artifact_payload(ref: ArtifactRef) -> dict[str, object]:
    return {
        "artifact_id": ref.artifact_id,
        "version": ref.version,
        "content_hash": ref.content_hash,
        "media_type": ref.media_type,
        "size": ref.size,
    }


def artifact_from_value(value: Any) -> ArtifactRef:
    payload = dict(json_loads(value))
    return ArtifactRef(
        artifact_id=str(payload["artifact_id"]),
        version=int(payload["version"]),
        content_hash=str(payload["content_hash"]),
        media_type=str(payload["media_type"]),
        size=int(payload["size"]),
    )


def package_from_row(row: dict[str, Any]) -> SkillPackageRecord:
    return SkillPackageRecord(
        tenant_id=str(row["tenant_id"]),
        manifest=SkillManifest.model_validate(json_loads(row["manifest_json"])),
        package_digest=str(row["package_digest"]),
        artifact_ref=artifact_from_value(row["artifact_ref"]),
        signature_key_id=row["signature_key_id"],
        retention_status=SkillPackageRetentionStatus(str(row["retention_status"])),
        retention_until=row["retention_until"],
        legal_hold=bool(row["legal_hold"]),
        retention_revision=int(row["retention_revision"]),
        retention_updated_by=str(row["retention_updated_by"]),
        retention_updated_at=row["retention_updated_at"],
        created_at=row["created_at"],
        purged_at=row["purged_at"],
    )
