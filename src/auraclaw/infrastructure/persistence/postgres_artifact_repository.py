from __future__ import annotations

import hashlib
from datetime import timedelta
from uuid import uuid4

import asyncpg  # type: ignore[import-untyped]

from auraclaw.artifact.internal_service import PendingUpload
from auraclaw.infrastructure.persistence.postgres_common import LazyPool


class PostgresArtifactRepository(LazyPool):
    async def save_pending(self, pending: PendingUpload) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO artifact.metadata
            (tenant_id,artifact_id,root_session_id,session_id,artifact_type,media_type,
             name,version,content_hash,size,storage_ref,producer,lineage_refs,
             classification,acl,created_at,status,upload_id,upload_expires_at,
             expected_checksum,scan_status,upload_mode,multipart_upload_id,
             multipart_part_size,retention_until)
            VALUES ($1,$2,$3,$4,'upload',$5,$6,1,$7,$8,$9,$10,'[]'::jsonb,$11,
                    '[]'::jsonb,now(),'pending',$12,$13,$7,'pending',$14,$15,$16,$17)""",
            pending.tenant_id,
            pending.artifact_id,
            pending.root_session_id,
            pending.session_id,
            pending.media_type,
            pending.name,
            pending.expected_checksum,
            pending.expected_size,
            pending.object_key,
            "artifact-service",
            pending.classification,
            pending.upload_id,
            pending.expires_at,
            pending.upload_mode,
            pending.multipart_upload_id,
            pending.multipart_part_size,
            pending.retention_until,
        )

    async def mark_multipart_completed(self, pending: PendingUpload) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET multipart_completed_at=now(),
            object_side_effect_started_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning') AND finalize_claim_token=$4
              AND finalize_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            pending.finalize_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def claim_finalize(
        self, pending: PendingUpload, *, claim_ttl: timedelta = timedelta(seconds=30)
    ) -> PendingUpload | None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET status='reconciling',
            object_state='unknown',reconciliation_reason='finalize_owner_lost_after_side_effect',
            reconciliation_updated_at=now()
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status='scanning' AND finalize_claim_expires_at<=now()
              AND object_side_effect_started_at IS NOT NULL""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
        )
        token = f"finalize_{uuid4().hex}"
        row = await pool.fetchrow(
            """UPDATE artifact.metadata SET status='scanning',scan_status='scanning',
            scan_started_at=now(),finalize_heartbeat_at=now(),finalize_claim_token=$4,
            finalize_claim_expires_at=now()+$5::interval
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning')
              AND object_side_effect_started_at IS NULL
              AND (finalize_claim_expires_at IS NULL OR finalize_claim_expires_at<=now())
            RETURNING *""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            token,
            claim_ttl,
        )
        return self._pending(row) if row is not None else None

    async def get_upload(
        self, tenant_id: str, artifact_id: str, upload_id: str
    ) -> PendingUpload | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM artifact.metadata WHERE tenant_id=$1 AND artifact_id=$2
            AND upload_id=$3 AND status IN ('pending','scanning')""",
            tenant_id,
            artifact_id,
            upload_id,
        )
        return self._pending(row) if row is not None else None

    async def mark_ready(self, pending: PendingUpload, version: int) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='ready',scan_status='clean',version=$4,
            finalize_claim_token=NULL,finalize_claim_expires_at=NULL,
            finalize_heartbeat_at=NULL,object_state='known',reconciliation_reason=NULL,
            object_side_effect_started_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND ((status='pending' AND $5::text IS NULL)
                   OR (status='scanning' AND finalize_claim_token=$5
                       AND finalize_claim_expires_at > now()))""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            version,
            pending.finalize_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def mark_quarantined(self, pending: PendingUpload, reason: str) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='quarantined',
            scan_status='quarantined',scan_error=$4,
            finalize_claim_token=NULL,finalize_claim_expires_at=NULL,
            finalize_heartbeat_at=NULL,object_state='known',
            object_side_effect_started_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning') AND finalize_claim_token=$5
              AND finalize_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            reason,
            pending.finalize_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def renew_finalize(self, pending: PendingUpload, *, claim_ttl: timedelta) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET finalize_heartbeat_at=now(),
            finalize_claim_expires_at=now()+$5::interval
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status='scanning' AND finalize_claim_token=$4
              AND finalize_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            pending.finalize_claim_token,
            claim_ttl,
        )
        return str(result) == "UPDATE 1"

    async def validate_finalize(self, pending: PendingUpload) -> bool:
        pool = await self.pool()
        return bool(
            await pool.fetchval(
                """SELECT EXISTS(SELECT 1 FROM artifact.metadata
                WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
                  AND status='scanning' AND finalize_claim_token=$4
                  AND finalize_claim_expires_at > now())""",
                pending.tenant_id,
                pending.artifact_id,
                pending.upload_id,
                pending.finalize_claim_token,
            )
        )

    async def get_ready(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> PendingUpload | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM artifact.metadata WHERE tenant_id=$1 AND artifact_id=$2
            AND version=$3 AND status='ready' AND object_state='known'
            AND deleted_at IS NULL""",
            tenant_id,
            artifact_id,
            version,
        )
        return self._pending(row) if row is not None else None

    async def claim_ready_delete(
        self,
        tenant_id: str,
        artifact_id: str,
        version: int,
        *,
        claim_ttl: timedelta = timedelta(seconds=30),
        ignore_retention: bool = False,
        include_deleted: bool = False,
    ) -> PendingUpload | None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET status='reconciling',object_state='unknown',
            reconciliation_reason='delete_owner_lost_after_side_effect',
            reconciliation_updated_at=now()
            WHERE tenant_id=$1 AND artifact_id=$2 AND version=$3
              AND status='deleting' AND gc_claim_expires_at<=now()
              AND object_side_effect_started_at IS NOT NULL""",
            tenant_id,
            artifact_id,
            version,
        )
        token = f"delete_{uuid4().hex}"
        row = await pool.fetchrow(
            """UPDATE artifact.metadata SET status='deleting',gc_claim_token=$4,
            gc_heartbeat_at=now(), deleted_at=NULL, object_state='known',
            physical_removal_pending=physical_removal_pending OR $7,
            object_side_effect_started_at=NULL,
            gc_claim_expires_at=now()+$5::interval,gc_attempt_count=gc_attempt_count+1
            WHERE tenant_id=$1 AND artifact_id=$2 AND version=$3
              AND (status='ready' OR ($7 AND status IN ('deleted','reconciling')) OR (
                    status='deleting' AND gc_claim_expires_at <= now()))
              AND ($7 OR (deleted_at IS NULL AND object_state='known'
                          AND object_side_effect_started_at IS NULL)) AND NOT legal_hold
              AND ($7 OR NOT physical_removal_pending)
              AND (NOT $7 OR media_type='application/vnd.auraclaw.skill-package+json')
              AND (($6 AND media_type='application/vnd.auraclaw.skill-package+json'
                        AND skill_bound_at IS NOT NULL)
                   OR (retention_until IS NOT NULL AND retention_until <= now()))
              AND (gc_claim_expires_at IS NULL OR gc_claim_expires_at <= now())
            RETURNING *""",
            tenant_id,
            artifact_id,
            version,
            token,
            claim_ttl,
            ignore_retention,
            include_deleted,
        )
        return self._pending(row) if row is not None else None

    async def is_removed(self, tenant_id: str, artifact_id: str, version: int) -> bool:
        pool = await self.pool()
        return bool(
            await pool.fetchval(
                "SELECT EXISTS(SELECT 1 FROM artifact.skill_removal_receipt "
                "WHERE tenant_id=$1 AND removal_digest=$2)",
                tenant_id,
                _removal_digest(tenant_id, artifact_id, version),
            )
        )

    async def has_shared_storage(self, pending: PendingUpload) -> bool:
        pool = await self.pool()
        return bool(
            await pool.fetchval(
                "SELECT EXISTS(SELECT 1 FROM artifact.metadata "
                "WHERE storage_ref=$1 AND NOT (tenant_id=$2 AND artifact_id=$3 AND version=$4) "
                "AND deleted_at IS NULL)",
                pending.object_key,
                pending.tenant_id,
                pending.artifact_id,
                pending.version,
            )
        )

    async def mark_ready_removed(self, pending: PendingUpload) -> bool:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """DELETE FROM artifact.metadata
                WHERE tenant_id=$1 AND artifact_id=$2 AND version=$3 AND status='deleting'
                  AND gc_claim_token=$4 AND gc_claim_expires_at > now()
                  AND media_type='application/vnd.auraclaw.skill-package+json' AND NOT legal_hold
                RETURNING artifact_id""",
                pending.tenant_id,
                pending.artifact_id,
                pending.version,
                pending.gc_claim_token,
            )
            if row is None:
                return False
            await connection.execute(
                "INSERT INTO artifact.skill_removal_receipt "
                "(tenant_id,removal_digest) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                pending.tenant_id,
                _removal_digest(pending.tenant_id, pending.artifact_id, pending.version),
            )
            return True

    async def mark_ready_deleted(self, pending: PendingUpload) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='deleted',deleted_at=now(),
            gc_last_error=NULL,gc_claim_token=NULL,gc_claim_expires_at=NULL,
            gc_heartbeat_at=NULL,object_state='known',reconciliation_reason=NULL,
            object_side_effect_started_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND status='deleting'
              AND gc_claim_token=$3 AND gc_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            pending.gc_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def renew_gc(self, pending: PendingUpload, *, claim_ttl: timedelta) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET gc_heartbeat_at=now(),
            gc_claim_expires_at=now()+$4::interval
            WHERE tenant_id=$1 AND artifact_id=$2 AND gc_claim_token=$3
              AND gc_claim_expires_at > now() AND deleted_at IS NULL""",
            pending.tenant_id,
            pending.artifact_id,
            pending.gc_claim_token,
            claim_ttl,
        )
        return str(result) == "UPDATE 1"

    async def validate_gc(self, pending: PendingUpload) -> bool:
        pool = await self.pool()
        return bool(
            await pool.fetchval(
                """SELECT EXISTS(SELECT 1 FROM artifact.metadata
                WHERE tenant_id=$1 AND artifact_id=$2 AND gc_claim_token=$3
                  AND gc_claim_expires_at > now() AND deleted_at IS NULL)""",
                pending.tenant_id,
                pending.artifact_id,
                pending.gc_claim_token,
            )
        )

    async def mark_reconciling(
        self, pending: PendingUpload, *, operation: str, reason: str
    ) -> bool:
        pool = await self.pool()
        token = pending.finalize_claim_token if operation == "finalize" else pending.gc_claim_token
        token_column = "finalize_claim_token" if operation == "finalize" else "gc_claim_token"
        result = await pool.execute(
            f"""UPDATE artifact.metadata SET status='reconciling',object_state='unknown',
            reconciliation_reason=$4,reconciliation_updated_at=now(),
            object_operation_ref=$5
            WHERE tenant_id=$1 AND artifact_id=$2 AND {token_column}=$3
              AND deleted_at IS NULL""",
            pending.tenant_id,
            pending.artifact_id,
            token,
            reason,
            f"{operation}:{pending.artifact_id}:{pending.version}",
        )
        return str(result) == "UPDATE 1"

    async def begin_object_side_effect(self, pending: PendingUpload, *, operation: str) -> bool:
        pool = await self.pool()
        if operation == "finalize":
            result = await pool.execute(
                """UPDATE artifact.metadata SET object_side_effect_started_at=now(),
                object_operation_ref=$5
                WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
                  AND status='scanning' AND finalize_claim_token=$4
                  AND finalize_claim_expires_at > now()""",
                pending.tenant_id,
                pending.artifact_id,
                pending.upload_id,
                pending.finalize_claim_token,
                f"finalize:{pending.artifact_id}:{pending.version}",
            )
        else:
            result = await pool.execute(
                """UPDATE artifact.metadata SET object_side_effect_started_at=now(),
                object_operation_ref=$4
                WHERE tenant_id=$1 AND artifact_id=$2 AND gc_claim_token=$3
                  AND gc_claim_expires_at > now() AND deleted_at IS NULL""",
                pending.tenant_id,
                pending.artifact_id,
                pending.gc_claim_token,
                f"gc:{pending.artifact_id}:{pending.version}",
            )
        return str(result) == "UPDATE 1"

    async def is_deleted(self, tenant_id: str, artifact_id: str, version: int) -> bool:
        pool = await self.pool()
        return bool(
            await pool.fetchval(
                """SELECT EXISTS(SELECT 1 FROM artifact.metadata
                WHERE tenant_id=$1 AND artifact_id=$2 AND version=$3
                  AND status='deleted' AND deleted_at IS NOT NULL)""",
                tenant_id,
                artifact_id,
                version,
            )
        )

    async def release_ready_delete(self, pending: PendingUpload, error: str) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='ready',gc_claim_token=NULL,
            gc_claim_expires_at=NULL,gc_heartbeat_at=NULL,gc_last_error=$3,
            object_side_effect_started_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND status='deleting'
              AND gc_claim_token=$4 AND gc_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            error,
            pending.gc_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def get_ready_delete_claim(
        self,
        tenant_id: str,
        artifact_id: str,
        version: int,
        claim_token: str,
    ) -> PendingUpload | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM artifact.metadata WHERE tenant_id=$1
            AND artifact_id=$2 AND version=$3 AND status='deleting'
            AND deleted_at IS NULL AND gc_claim_token=$4
            AND gc_claim_expires_at > now()""",
            tenant_id,
            artifact_id,
            version,
            claim_token,
        )
        return self._pending(row) if row is not None else None

    async def claim_skill_publication(
        self,
        tenant_id: str,
        artifact_id: str,
        version: int,
        command_id: str,
    ) -> PendingUpload | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """UPDATE artifact.metadata SET skill_publish_claim_token=$4,
            skill_publish_claim_expires_at=now()+interval '15 minutes'
            WHERE tenant_id=$1 AND artifact_id=$2 AND version=$3
              AND status='ready' AND object_state='known' AND deleted_at IS NULL
              AND media_type='application/vnd.auraclaw.skill-package+json'
              AND (root_session_id='skill-registry'
                   OR root_session_id LIKE 'skill-upload:%')
              AND (skill_bound_at IS NOT NULL OR (
                   retention_until IS NOT NULL AND retention_until > now()
                   AND (skill_publish_claim_expires_at IS NULL
                        OR skill_publish_claim_expires_at <= now()
                        OR skill_publish_claim_token=$4)))
            RETURNING *""",
            tenant_id,
            artifact_id,
            version,
            command_id,
        )
        return self._pending(row) if row is not None else None

    async def bind_skill_publication(self, pending: PendingUpload, package_digest: str) -> bool:
        content_hash = package_digest.removeprefix("sha256:")
        if f"sha256:{content_hash}" != package_digest:
            return False
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='ready',skill_bound_at=now(),
            skill_bound_digest=$5,skill_publish_claim_token=NULL,
            skill_publish_claim_expires_at=NULL,gc_claim_token=NULL,
            gc_claim_expires_at=NULL,gc_last_error=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND version=$3
              AND expected_checksum=$4
              AND deleted_at IS NULL AND (
                (status='ready' AND (
                  skill_publish_claim_token=$6 OR skill_bound_digest=$5))
                OR (status='deleting' AND gc_claim_token=$7
                    AND gc_claim_expires_at > now()))""",
            pending.tenant_id,
            pending.artifact_id,
            pending.version,
            content_hash,
            package_digest,
            pending.skill_publish_claim_token,
            pending.gc_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def claim_skill_orphans(
        self,
        *,
        owner: str,
        limit: int = 100,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> list[PendingUpload]:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET status='reconciling',object_state='unknown',
            reconciliation_reason='orphan_owner_lost_after_side_effect',
            reconciliation_updated_at=now()
            WHERE status='deleting' AND gc_claim_expires_at<=now()
              AND object_side_effect_started_at IS NOT NULL
              AND media_type='application/vnd.auraclaw.skill-package+json'"""
        )
        token = f"skill-orphan:{owner}:{uuid4().hex}"
        rows = await pool.fetch(
            """UPDATE artifact.metadata target SET status='deleting',
                   gc_claim_token=$2,gc_heartbeat_at=now(),
                   gc_claim_expires_at=now()+$3::interval,
                   gc_attempt_count=gc_attempt_count+1
               WHERE (tenant_id,artifact_id) IN (
                   SELECT tenant_id,artifact_id FROM artifact.metadata
                   WHERE status='ready' AND object_state='known'
                     AND deleted_at IS NULL AND NOT legal_hold
                     AND media_type='application/vnd.auraclaw.skill-package+json'
                     AND (root_session_id='skill-registry'
                          OR root_session_id LIKE 'skill-upload:%')
                     AND skill_bound_at IS NULL
                     AND retention_until IS NOT NULL AND retention_until <= now()
                     AND (skill_publish_claim_expires_at IS NULL
                          OR skill_publish_claim_expires_at <= now())
                     AND (gc_claim_expires_at IS NULL OR gc_claim_expires_at <= now())
                     AND object_side_effect_started_at IS NULL
                   ORDER BY retention_until,artifact_id
                   FOR UPDATE SKIP LOCKED LIMIT $1
               ) RETURNING target.*""",
            limit,
            token,
            claim_ttl,
        )
        return [self._pending(row) for row in rows]

    async def cleanup_expired(self) -> int:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='deleted',deleted_at=now()
            WHERE status='pending' AND upload_expires_at <= now() AND NOT legal_hold"""
        )
        return int(result.rsplit(" ", 1)[-1])

    async def claim_reconciling(
        self,
        *,
        owner: str,
        limit: int = 1,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> list[PendingUpload]:
        pool = await self.pool()
        token = f"reconcile:{owner}:{uuid4().hex}"
        rows = await pool.fetch(
            """UPDATE artifact.metadata target SET gc_claim_token=$2,
            gc_heartbeat_at=now(),gc_claim_expires_at=now()+$3::interval
            WHERE (tenant_id,artifact_id) IN (
                SELECT tenant_id,artifact_id FROM artifact.metadata
                WHERE status='reconciling' AND object_state='unknown'
                  AND NOT physical_removal_pending
                  AND deleted_at IS NULL
                  AND (gc_claim_expires_at IS NULL OR gc_claim_expires_at<=now())
                ORDER BY reconciliation_updated_at,artifact_id
                FOR UPDATE SKIP LOCKED LIMIT $1
            ) RETURNING target.*""",
            limit,
            token,
            claim_ttl,
        )
        return [self._pending(row) for row in rows]

    async def resolve_reconciling(
        self, pending: PendingUpload, *, status: str, reason: str
    ) -> bool:
        if status not in {"ready", "pending", "deleted", "quarantined"}:
            return False
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status=$4,object_state='known',
            deleted_at=CASE WHEN $4='deleted' THEN now() ELSE deleted_at END,
            scan_status=CASE WHEN $4='ready' THEN 'clean'
                             WHEN $4='quarantined' THEN 'quarantined'
                             ELSE scan_status END,
            reconciliation_reason=$5,reconciliation_updated_at=now(),
            object_side_effect_started_at=NULL,finalize_claim_token=NULL,
            finalize_claim_expires_at=NULL,finalize_heartbeat_at=NULL,
            gc_claim_token=NULL,gc_claim_expires_at=NULL,gc_heartbeat_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND gc_claim_token=$3
              AND status='reconciling' AND object_state='unknown'
              AND NOT physical_removal_pending
              AND gc_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            pending.gc_claim_token,
            status,
            reason,
        )
        return str(result) == "UPDATE 1"

    async def expired_uploads(
        self, *, limit: int = 100, claim_ttl: timedelta = timedelta(seconds=30)
    ) -> list[PendingUpload]:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET status='reconciling',object_state='unknown',
            reconciliation_reason='gc_owner_lost_after_side_effect',
            reconciliation_updated_at=now()
            WHERE status IN ('pending','scanning') AND gc_claim_expires_at<=now()
              AND object_side_effect_started_at IS NOT NULL AND deleted_at IS NULL"""
        )
        token = f"gc_{uuid4().hex}"
        rows = await pool.fetch(
            """UPDATE artifact.metadata target SET gc_claim_token=$2,
                   gc_heartbeat_at=now(),gc_claim_expires_at=now()+$3::interval,
                   gc_attempt_count=gc_attempt_count+1
               WHERE (tenant_id,artifact_id) IN (
                   SELECT tenant_id,artifact_id FROM artifact.metadata
                   WHERE status IN ('pending','scanning')
                     AND object_state='known'
                     AND upload_expires_at <= now() AND NOT legal_hold
                     AND deleted_at IS NULL
                     AND (gc_claim_expires_at IS NULL OR gc_claim_expires_at <= now())
                     AND object_side_effect_started_at IS NULL
                   ORDER BY upload_expires_at,artifact_id
                   FOR UPDATE SKIP LOCKED LIMIT $1
               )
               RETURNING target.*""",
            limit,
            token,
            claim_ttl,
        )
        return [self._pending(row) for row in rows]

    async def mark_deleted(self, pending: PendingUpload) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='deleted',deleted_at=now(),
            gc_last_error=NULL,gc_claim_token=NULL,gc_claim_expires_at=NULL,
            gc_heartbeat_at=NULL,object_state='known',reconciliation_reason=NULL,
            object_side_effect_started_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning') AND NOT legal_hold
              AND gc_claim_token=$4 AND gc_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            pending.gc_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def release_gc(self, pending: PendingUpload, error: str) -> bool:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET gc_claim_token=NULL,
            gc_claim_expires_at=NULL,gc_last_error=$4,
            gc_heartbeat_at=NULL,object_side_effect_started_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND gc_claim_token=$5 AND gc_claim_expires_at > now()""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            error,
            pending.gc_claim_token,
        )
        return str(result) == "UPDATE 1"

    @staticmethod
    def _pending(row: asyncpg.Record) -> PendingUpload:
        return PendingUpload(
            tenant_id=str(row["tenant_id"]),
            artifact_id=str(row["artifact_id"]),
            upload_id=str(row["upload_id"]),
            object_key=str(row["storage_ref"]),
            root_session_id=str(row["root_session_id"]),
            session_id=str(row["session_id"]),
            name=str(row["name"]),
            media_type=str(row["media_type"]),
            expected_size=int(row["size"]),
            expected_checksum=str(row["expected_checksum"]),
            classification=str(row["classification"]),
            expires_at=row["upload_expires_at"],
            version=int(row["version"]),
            upload_mode=str(row["upload_mode"]),
            multipart_upload_id=(
                str(row["multipart_upload_id"]) if row["multipart_upload_id"] is not None else None
            ),
            multipart_part_size=(
                int(row["multipart_part_size"]) if row["multipart_part_size"] is not None else None
            ),
            multipart_completed=row["multipart_completed_at"] is not None,
            gc_claim_token=(
                str(row["gc_claim_token"]) if row["gc_claim_token"] is not None else None
            ),
            finalize_claim_token=(
                str(row["finalize_claim_token"])
                if row["finalize_claim_token"] is not None
                else None
            ),
            skill_bound_digest=(
                str(row["skill_bound_digest"]) if row["skill_bound_digest"] is not None else None
            ),
            skill_publish_claim_token=(
                str(row["skill_publish_claim_token"])
                if row["skill_publish_claim_token"] is not None
                else None
            ),
            retention_until=row["retention_until"],
            legal_hold=bool(row["legal_hold"]),
            lifecycle_status=str(row["status"]),
            scan_status=str(row["scan_status"]),
            object_operation_ref=(
                str(row["object_operation_ref"])
                if row["object_operation_ref"] is not None
                else None
            ),
        )


def _removal_digest(tenant_id: str, artifact_id: str, version: int) -> str:
    return hashlib.sha256(f"{tenant_id}\0{artifact_id}\0{version}".encode()).hexdigest()
