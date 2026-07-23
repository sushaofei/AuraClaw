from __future__ import annotations

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
             multipart_part_size)
            VALUES ($1,$2,$3,$4,'upload',$5,$6,1,$7,$8,$9,$10,'[]'::jsonb,$11,
                    '[]'::jsonb,now(),'pending',$12,$13,$7,'pending',$14,$15,$16)""",
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
        )

    async def mark_multipart_completed(self, pending: PendingUpload) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET multipart_completed_at=now()
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning') AND finalize_claim_token=$4""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            pending.finalize_claim_token,
        )

    async def claim_finalize(self, pending: PendingUpload) -> PendingUpload | None:
        pool = await self.pool()
        token = f"finalize_{uuid4().hex}"
        row = await pool.fetchrow(
            """UPDATE artifact.metadata SET status='scanning',scan_status='scanning',
            scan_started_at=now(),finalize_claim_token=$4,
            finalize_claim_expires_at=now()+$5::interval
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning')
              AND (finalize_claim_expires_at IS NULL OR finalize_claim_expires_at<=now())
            RETURNING *""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            token,
            timedelta(seconds=30),
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
            finalize_claim_token=NULL,finalize_claim_expires_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND ((status='pending' AND $5::text IS NULL)
                   OR (status='scanning' AND finalize_claim_token=$5))""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            version,
            pending.finalize_claim_token,
        )
        return str(result) == "UPDATE 1"

    async def mark_quarantined(self, pending: PendingUpload, reason: str) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET status='quarantined',
            scan_status='quarantined',scan_error=$4,
            finalize_claim_token=NULL,finalize_claim_expires_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning') AND finalize_claim_token=$5""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            reason,
            pending.finalize_claim_token,
        )

    async def get_ready(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> PendingUpload | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM artifact.metadata WHERE tenant_id=$1 AND artifact_id=$2
            AND version=$3 AND status='ready' AND deleted_at IS NULL""",
            tenant_id,
            artifact_id,
            version,
        )
        return self._pending(row) if row is not None else None

    async def cleanup_expired(self) -> int:
        pool = await self.pool()
        result = await pool.execute(
            """UPDATE artifact.metadata SET status='deleted',deleted_at=now()
            WHERE status='pending' AND upload_expires_at <= now() AND NOT legal_hold"""
        )
        return int(result.rsplit(" ", 1)[-1])

    async def expired_uploads(self, *, limit: int = 100) -> list[PendingUpload]:
        pool = await self.pool()
        token = f"gc_{uuid4().hex}"
        rows = await pool.fetch(
            """UPDATE artifact.metadata target SET gc_claim_token=$2,
                   gc_claim_expires_at=now()+$3::interval,
                   gc_attempt_count=gc_attempt_count+1
               WHERE (tenant_id,artifact_id) IN (
                   SELECT tenant_id,artifact_id FROM artifact.metadata
                   WHERE status IN ('pending','scanning')
                     AND upload_expires_at <= now() AND NOT legal_hold
                     AND deleted_at IS NULL
                     AND (gc_claim_expires_at IS NULL OR gc_claim_expires_at <= now())
                   ORDER BY upload_expires_at,artifact_id
                   FOR UPDATE SKIP LOCKED LIMIT $1
               )
               RETURNING target.*""",
            limit,
            token,
            timedelta(seconds=30),
        )
        return [self._pending(row) for row in rows]

    async def mark_deleted(self, pending: PendingUpload) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET status='deleted',deleted_at=now(),
            gc_last_error=NULL,gc_claim_token=NULL,gc_claim_expires_at=NULL
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND status IN ('pending','scanning') AND NOT legal_hold
              AND gc_claim_token=$4""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            pending.gc_claim_token,
        )

    async def release_gc(self, pending: PendingUpload, error: str) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET gc_claim_token=NULL,
            gc_claim_expires_at=NULL,gc_last_error=$4
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3
              AND gc_claim_token=$5""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            error,
            pending.gc_claim_token,
        )

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
            upload_mode=str(row["upload_mode"]),
            multipart_upload_id=(
                str(row["multipart_upload_id"])
                if row["multipart_upload_id"] is not None
                else None
            ),
            multipart_part_size=(
                int(row["multipart_part_size"])
                if row["multipart_part_size"] is not None
                else None
            ),
            multipart_completed=row["multipart_completed_at"] is not None,
            gc_claim_token=(
                str(row["gc_claim_token"])
                if row["gc_claim_token"] is not None
                else None
            ),
            finalize_claim_token=(
                str(row["finalize_claim_token"])
                if row["finalize_claim_token"] is not None
                else None
            ),
        )
