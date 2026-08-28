from __future__ import annotations

from typing import Any

import asyncpg  # type: ignore[import-untyped]

from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.contracts.errors import NotFoundError, VersionConflictError
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillSourceSyncState,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
)


class PostgresSkillLifecycleStore(LazyPool, SkillLifecycleStore):
    async def put_package(self, record: SkillPackageRecord) -> SkillPackageRecord:
        pool = await self.pool()
        manifest = record.manifest
        try:
            row = await pool.fetchrow(
                """INSERT INTO hands.skill_package
                (tenant_id,publisher,name,version,package_digest,manifest_json,
                 artifact_ref,signature_key_id,retention_status,retention_until,
                 legal_hold,retention_revision,retention_updated_by,
                 retention_updated_at,created_at,purged_at)
                VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10,$11,$12,
                        $13,$14,$15,$16)
                ON CONFLICT (tenant_id,publisher,name,version) DO NOTHING
                RETURNING *""",
                record.tenant_id,
                manifest.publisher,
                manifest.name,
                manifest.version,
                record.package_digest,
                json_dumps(manifest.model_dump(mode="json")),
                json_dumps(_artifact_payload(record.artifact_ref)),
                record.signature_key_id,
                record.retention_status.value,
                record.retention_until,
                record.legal_hold,
                record.retention_revision,
                record.retention_updated_by,
                record.retention_updated_at,
                record.created_at,
                record.purged_at,
            )
        except asyncpg.UniqueViolationError as exc:
            raise VersionConflictError(
                "Skill package digest belongs to another version"
            ) from exc
        if row is not None:
            return _package(dict(row))
        existing = await self.get_package(
            record.tenant_id,
            manifest.publisher,
            manifest.name,
            manifest.version,
        )
        if existing is None or existing.package_digest != record.package_digest:
            raise VersionConflictError("Skill version is immutable")
        return existing

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_package
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
            tenant_id,
            publisher,
            name,
            version,
        )
        return None if row is None else _package(dict(row))

    async def update_package_retention(
        self, record: SkillPackageRecord, *, expected_revision: int
    ) -> SkillPackageRecord:
        if record.retention_revision != expected_revision + 1:
            raise VersionConflictError(
                "Skill package retention next revision is invalid"
            )
        pool = await self.pool()
        row = await pool.fetchrow(
            """UPDATE hands.skill_package SET
            retention_status=$1,retention_until=$2,legal_hold=$3,
            retention_revision=$4,retention_updated_by=$5,
            retention_updated_at=$6,purged_at=$7
            WHERE tenant_id=$8 AND publisher=$9 AND name=$10 AND version=$11
              AND package_digest=$12 AND retention_revision=$13
            RETURNING *""",
            record.retention_status.value,
            record.retention_until,
            record.legal_hold,
            record.retention_revision,
            record.retention_updated_by,
            record.retention_updated_at,
            record.purged_at,
            record.tenant_id,
            record.manifest.publisher,
            record.manifest.name,
            record.manifest.version,
            record.package_digest,
            expected_revision,
        )
        if row is None:
            raise VersionConflictError("Skill package retention revision conflict")
        return _package(dict(row))

    async def put_publication(
        self, record: SkillPublicationRecord, *, expected_revision: int
    ) -> SkillPublicationRecord:
        _require_next_revision(record.revision, expected_revision, "publication")
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            package_exists = await connection.fetchval(
                """SELECT true FROM hands.skill_package
                WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4
                  AND package_digest=$5""",
                record.tenant_id,
                record.publisher,
                record.name,
                record.version,
                record.package_digest,
            )
            if package_exists is None:
                raise NotFoundError("Skill package was not found")
            if record.source_id is not None:
                source_exists = await connection.fetchval(
                    """SELECT true FROM hands.skill_source
                    WHERE tenant_id=$1 AND source_id=$2""",
                    record.tenant_id,
                    record.source_id,
                )
                if source_exists is None:
                    raise NotFoundError("Skill Source was not found")
            if expected_revision == 0:
                row = await connection.fetchrow(
                    """INSERT INTO hands.skill_publication
                    (publication_id,tenant_id,publisher,name,version,package_digest,
                     status,source_id,revision,created_by,updated_by,created_at,
                     updated_at,reason_code)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                    ON CONFLICT (tenant_id,publisher,name,version) DO NOTHING
                    RETURNING *""",
                    *_publication_values(record),
                )
            else:
                row = await connection.fetchrow(
                    """UPDATE hands.skill_publication SET
                    status=$1,source_id=$2,revision=$3,updated_by=$4,updated_at=$5,
                    reason_code=$6
                    WHERE tenant_id=$7 AND publisher=$8 AND name=$9 AND version=$10
                      AND publication_id=$11 AND package_digest=$12 AND revision=$13
                    RETURNING *""",
                    record.status.value,
                    record.source_id,
                    record.revision,
                    record.updated_by,
                    record.updated_at,
                    record.reason_code,
                    record.tenant_id,
                    record.publisher,
                    record.name,
                    record.version,
                    record.publication_id,
                    record.package_digest,
                    expected_revision,
                )
            if row is None:
                raise VersionConflictError("Skill publication revision conflict")
        return _publication(dict(row))

    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_publication
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3 AND version=$4""",
            tenant_id,
            publisher,
            name,
            version,
        )
        return None if row is None else _publication(dict(row))

    async def list_publications(
        self, tenant_id: str
    ) -> tuple[SkillPublicationRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_publication WHERE tenant_id=$1
            ORDER BY publisher,name,version""",
            tenant_id,
        )
        return tuple(_publication(dict(row)) for row in rows)

    async def list_tenants(self) -> tuple[str, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT tenant_id FROM hands.skill_publication
            UNION SELECT tenant_id FROM hands.skill_installation
            ORDER BY tenant_id"""
        )
        return tuple(str(row["tenant_id"]) for row in rows)

    async def put_installation(
        self, record: SkillInstallationRecord, *, expected_revision: int
    ) -> SkillInstallationRecord:
        _require_next_revision(record.revision, expected_revision, "installation")
        pool = await self.pool()
        if record.source_id is not None:
            source_exists = await pool.fetchval(
                """SELECT true FROM hands.skill_source
                WHERE tenant_id=$1 AND source_id=$2""",
                record.tenant_id,
                record.source_id,
            )
            if source_exists is None:
                raise NotFoundError("Skill Source was not found")
        if expected_revision == 0:
            row = await pool.fetchrow(
                """INSERT INTO hands.skill_installation
                (installation_id,tenant_id,publisher,name,version_constraint,
                 pinned_package_digest,status,source_id,auto_upgrade,revision,
                 created_by,updated_by,created_at,updated_at,reason_code)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                ON CONFLICT (tenant_id,publisher,name) DO NOTHING RETURNING *""",
                *_installation_values(record),
            )
        else:
            row = await pool.fetchrow(
                """UPDATE hands.skill_installation SET
                version_constraint=$1,pinned_package_digest=$2,status=$3,source_id=$4,
                auto_upgrade=$5,revision=$6,updated_by=$7,updated_at=$8,reason_code=$9
                WHERE tenant_id=$10 AND publisher=$11 AND name=$12
                  AND installation_id=$13 AND revision=$14 RETURNING *""",
                record.version_constraint,
                record.pinned_package_digest,
                record.status.value,
                record.source_id,
                record.auto_upgrade,
                record.revision,
                record.updated_by,
                record.updated_at,
                record.reason_code,
                record.tenant_id,
                record.publisher,
                record.name,
                record.installation_id,
                expected_revision,
            )
        if row is None:
            raise VersionConflictError("Skill installation revision conflict")
        return _installation(dict(row))

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_installation
            WHERE tenant_id=$1 AND publisher=$2 AND name=$3""",
            tenant_id,
            publisher,
            name,
        )
        return None if row is None else _installation(dict(row))

    async def list_installations(
        self, tenant_id: str
    ) -> tuple[SkillInstallationRecord, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.skill_installation
            WHERE tenant_id=$1 ORDER BY publisher,name""",
            tenant_id,
        )
        return tuple(_installation(dict(row)) for row in rows)

    async def put_source(
        self, record: SkillSourceRecord, *, expected_revision: int
    ) -> SkillSourceRecord:
        _require_next_revision(record.revision, expected_revision, "source")
        pool = await self.pool()
        if expected_revision == 0:
            row = await pool.fetchrow(
                """INSERT INTO hands.skill_source
                (source_id,tenant_id,kind,desired_state,publisher_allowlist,
                 credential_ref,config_metadata,revision,created_by,updated_by,
                 created_at,updated_at)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7::jsonb,$8,$9,$10,$11,$12)
                ON CONFLICT (tenant_id,source_id) DO NOTHING RETURNING *""",
                *_source_values(record),
            )
        else:
            row = await pool.fetchrow(
                """UPDATE hands.skill_source SET
                desired_state=$1,publisher_allowlist=$2::jsonb,
                credential_ref=$3,config_metadata=$4::jsonb,revision=$5,
                updated_by=$6,updated_at=$7
                WHERE tenant_id=$8 AND source_id=$9 AND revision=$10 AND kind=$11
                RETURNING *""",
                record.desired_state.value,
                json_dumps(record.publisher_allowlist),
                record.credential_ref,
                json_dumps(record.config_metadata),
                record.revision,
                record.updated_by,
                record.updated_at,
                record.tenant_id,
                record.source_id,
                expected_revision,
                record.kind.value,
            )
        if row is None:
            raise VersionConflictError("Skill Source revision conflict")
        return _source(dict(row))

    async def get_source(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceRecord | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_source
            WHERE tenant_id=$1 AND source_id=$2""",
            tenant_id,
            source_id,
        )
        return None if row is None else _source(dict(row))

    async def put_sync_state(self, state: SkillSourceSyncState) -> None:
        pool = await self.pool()
        source_exists = await pool.fetchval(
            "SELECT true FROM hands.skill_source WHERE tenant_id=$1 AND source_id=$2",
            state.tenant_id,
            state.source_id,
        )
        if source_exists is None:
            raise NotFoundError("Skill Source was not found")
        row = await pool.fetchrow(
            """INSERT INTO hands.skill_source_sync_state
            (source_id,tenant_id,generation,cursor,complete_snapshot,last_success_at,
             last_attempt_at,consecutive_failures,safe_error_code,updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
            ON CONFLICT (tenant_id,source_id) DO UPDATE SET
              generation=EXCLUDED.generation,cursor=EXCLUDED.cursor,
              complete_snapshot=EXCLUDED.complete_snapshot,
              last_success_at=EXCLUDED.last_success_at,
              last_attempt_at=EXCLUDED.last_attempt_at,
              consecutive_failures=EXCLUDED.consecutive_failures,
              safe_error_code=EXCLUDED.safe_error_code,updated_at=now()
            WHERE EXCLUDED.generation >= hands.skill_source_sync_state.generation
            RETURNING *""",
            state.source_id,
            state.tenant_id,
            state.generation,
            state.cursor,
            state.complete_snapshot,
            state.last_success_at,
            state.last_attempt_at,
            state.consecutive_failures,
            state.safe_error_code,
        )
        if row is None:
            raise VersionConflictError("Skill Source generation cannot move backwards")

    async def get_sync_state(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceSyncState | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM hands.skill_source_sync_state
            WHERE tenant_id=$1 AND source_id=$2""",
            tenant_id,
            source_id,
        )
        return None if row is None else _sync_state(dict(row))


def _require_next_revision(revision: int, expected_revision: int, label: str) -> None:
    if revision != expected_revision + 1:
        raise VersionConflictError(f"Skill {label} next revision is invalid")


def _artifact_payload(ref: ArtifactRef) -> dict[str, object]:
    return {
        "artifact_id": ref.artifact_id,
        "version": ref.version,
        "content_hash": ref.content_hash,
        "media_type": ref.media_type,
        "size": ref.size,
    }


def _artifact(value: Any) -> ArtifactRef:
    payload = dict(json_loads(value))
    return ArtifactRef(
        artifact_id=str(payload["artifact_id"]),
        version=int(payload["version"]),
        content_hash=str(payload["content_hash"]),
        media_type=str(payload["media_type"]),
        size=int(payload["size"]),
    )


def _package(row: dict[str, Any]) -> SkillPackageRecord:
    return SkillPackageRecord(
        tenant_id=str(row["tenant_id"]),
        manifest=SkillManifest.model_validate(json_loads(row["manifest_json"])),
        package_digest=str(row["package_digest"]),
        artifact_ref=_artifact(row["artifact_ref"]),
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


def _publication_values(record: SkillPublicationRecord) -> tuple[object, ...]:
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
    )


def _publication(row: dict[str, Any]) -> SkillPublicationRecord:
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
    )


def _installation_values(record: SkillInstallationRecord) -> tuple[object, ...]:
    return (
        record.installation_id,
        record.tenant_id,
        record.publisher,
        record.name,
        record.version_constraint,
        record.pinned_package_digest,
        record.status.value,
        record.source_id,
        record.auto_upgrade,
        record.revision,
        record.created_by,
        record.updated_by,
        record.created_at,
        record.updated_at,
        record.reason_code,
    )


def _installation(row: dict[str, Any]) -> SkillInstallationRecord:
    return SkillInstallationRecord(
        installation_id=str(row["installation_id"]),
        tenant_id=str(row["tenant_id"]),
        publisher=str(row["publisher"]),
        name=str(row["name"]),
        version_constraint=str(row["version_constraint"]),
        pinned_package_digest=row["pinned_package_digest"],
        status=SkillInstallationStatus(str(row["status"])),
        source_id=row["source_id"],
        auto_upgrade=bool(row["auto_upgrade"]),
        revision=int(row["revision"]),
        created_by=str(row["created_by"]),
        updated_by=str(row["updated_by"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        reason_code=row["reason_code"],
    )


def _source_values(record: SkillSourceRecord) -> tuple[object, ...]:
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
    )


def _source(row: dict[str, Any]) -> SkillSourceRecord:
    return SkillSourceRecord(
        source_id=str(row["source_id"]),
        tenant_id=str(row["tenant_id"]),
        kind=SkillSourceKind(str(row["kind"])),
        desired_state=SkillSourceDesiredState(str(row["desired_state"])),
        publisher_allowlist=tuple(json_loads(row["publisher_allowlist"])),
        credential_ref=row["credential_ref"],
        config_metadata=dict(json_loads(row["config_metadata"])),
        revision=int(row["revision"]),
        created_by=str(row["created_by"]),
        updated_by=str(row["updated_by"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _sync_state(row: dict[str, Any]) -> SkillSourceSyncState:
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
