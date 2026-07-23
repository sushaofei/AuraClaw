from __future__ import annotations

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
             expected_checksum,scan_status)
            VALUES ($1,$2,$3,$4,'upload',$5,$6,1,$7,$8,$9,$10,'[]'::jsonb,$11,
                    '[]'::jsonb,now(),'pending',$12,$13,$7,'pending')""",
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
        )

    async def get_upload(
        self, tenant_id: str, artifact_id: str, upload_id: str
    ) -> PendingUpload | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM artifact.metadata WHERE tenant_id=$1 AND artifact_id=$2
            AND upload_id=$3 AND status='pending'""",
            tenant_id,
            artifact_id,
            upload_id,
        )
        return self._pending(row) if row is not None else None

    async def mark_ready(self, pending: PendingUpload, version: int) -> None:
        pool = await self.pool()
        await pool.execute(
            """UPDATE artifact.metadata SET status='ready',scan_status='clean',version=$4
            WHERE tenant_id=$1 AND artifact_id=$2 AND upload_id=$3 AND status='pending'""",
            pending.tenant_id,
            pending.artifact_id,
            pending.upload_id,
            version,
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
        )
