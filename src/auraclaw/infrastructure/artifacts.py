from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import uuid4

from auraclaw.contracts.errors import ArtifactAccessError, NotFoundError
from auraclaw.contracts.tools import ArtifactMetadata, ArtifactRef


class ObjectStorageAdapter(Protocol):
    async def put(self, storage_ref: str, content: bytes) -> None: ...

    async def get(self, storage_ref: str) -> bytes: ...


class InMemoryObjectStorage:
    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    async def put(self, storage_ref: str, content: bytes) -> None:
        self._objects.setdefault(storage_ref, bytes(content))

    async def get(self, storage_ref: str) -> bytes:
        try:
            return self._objects[storage_ref]
        except KeyError as exc:
            raise NotFoundError("artifact object not found") from exc


class ArtifactStore:
    """Immutable, tenant-scoped Artifact metadata and controlled object access."""

    def __init__(self, objects: ObjectStorageAdapter, *, signing_key: bytes) -> None:
        if len(signing_key) < 16:
            raise ValueError("artifact signing key must contain at least 16 bytes")
        self._objects = objects
        self._signing_key = bytes(signing_key)
        self._metadata: dict[tuple[str, str], ArtifactMetadata] = {}
        self._content_storage: dict[tuple[str, str], str] = {}

    async def put(
        self,
        *,
        tenant_id: str,
        root_session_id: str,
        session_id: str,
        content: bytes,
        artifact_type: str,
        media_type: str,
        name: str,
        producer: str,
        lineage_refs: tuple[str, ...] = (),
        classification: str = "internal",
        acl: tuple[str, ...] = (),
        retention_until: datetime | None = None,
    ) -> ArtifactRef:
        for lineage_id in lineage_refs:
            if (tenant_id, lineage_id) not in self._metadata:
                raise ArtifactAccessError("artifact lineage crosses tenant or is missing")
        content_hash = hashlib.sha256(content).hexdigest()
        storage_ref = self._content_storage.get((tenant_id, content_hash))
        if storage_ref is None:
            storage_ref = f"objects/{tenant_id}/{content_hash}"
            await self._objects.put(storage_ref, content)
            self._content_storage[(tenant_id, content_hash)] = storage_ref
        artifact_id = f"art_{uuid4().hex}"
        metadata = ArtifactMetadata(
            artifact_id=artifact_id,
            tenant_id=tenant_id,
            root_session_id=root_session_id,
            session_id=session_id,
            artifact_type=artifact_type,
            media_type=media_type,
            name=name,
            version=1,
            content_hash=content_hash,
            size=len(content),
            storage_ref=storage_ref,
            producer=producer,
            lineage_refs=lineage_refs,
            classification=classification,
            acl=acl,
            created_at=datetime.now(UTC),
            retention_until=retention_until,
        )
        self._metadata[(tenant_id, artifact_id)] = metadata
        return metadata.public_ref()

    async def derive_version(
        self,
        *,
        tenant_id: str,
        source_artifact_id: str,
        content: bytes,
        producer: str,
    ) -> ArtifactRef:
        source = await self.metadata(tenant_id, source_artifact_id)
        ref = await self.put(
            tenant_id=tenant_id,
            root_session_id=source.root_session_id,
            session_id=source.session_id,
            content=content,
            artifact_type=source.artifact_type,
            media_type=source.media_type,
            name=source.name,
            producer=producer,
            lineage_refs=(source.artifact_id,),
            classification=source.classification,
            acl=source.acl,
            retention_until=source.retention_until,
        )
        created = await self.metadata(tenant_id, ref.artifact_id)
        versioned = replace(created, version=source.version + 1)
        self._metadata[(tenant_id, ref.artifact_id)] = versioned
        return versioned.public_ref()

    async def metadata(self, tenant_id: str, artifact_id: str) -> ArtifactMetadata:
        record = self._metadata.get((tenant_id, artifact_id))
        if record is None:
            raise NotFoundError(f"Artifact not found: {artifact_id}")
        return record

    async def issue_download_token(
        self,
        *,
        tenant_id: str,
        artifact_id: str,
        actor_id: str,
        ttl: timedelta = timedelta(minutes=5),
    ) -> str:
        record = await self.metadata(tenant_id, artifact_id)
        if record.acl and actor_id not in record.acl:
            raise ArtifactAccessError("actor is not allowed to download Artifact")
        payload = {
            "actor_id": actor_id,
            "artifact_id": artifact_id,
            "expires_at": int((datetime.now(UTC) + ttl).timestamp()),
            "tenant_id": tenant_id,
        }
        body = base64.urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).rstrip(b"=")
        signature = hmac.new(self._signing_key, body, hashlib.sha256).hexdigest().encode()
        return f"{body.decode()}.{signature.decode()}"

    async def download(self, *, token: str, tenant_id: str, actor_id: str) -> bytes:
        try:
            encoded, supplied_signature = token.split(".", 1)
            body = encoded.encode()
            expected_signature = hmac.new(self._signing_key, body, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(supplied_signature, expected_signature):
                raise ArtifactAccessError("invalid Artifact download signature")
            padded = encoded + "=" * (-len(encoded) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        except ArtifactAccessError:
            raise
        except (ValueError, json.JSONDecodeError) as exc:
            raise ArtifactAccessError("invalid Artifact download token") from exc
        if payload.get("tenant_id") != tenant_id or payload.get("actor_id") != actor_id:
            raise ArtifactAccessError("Artifact download token scope mismatch")
        if int(payload["expires_at"]) <= int(datetime.now(UTC).timestamp()):
            raise ArtifactAccessError("Artifact download token expired")
        record = await self.metadata(tenant_id, str(payload["artifact_id"]))
        return await self._objects.get(record.storage_ref)
