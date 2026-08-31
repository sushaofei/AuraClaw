import asyncio
import hashlib
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest

from auraclaw.artifact.internal_service import ArtifactInternalService, PendingUpload
from auraclaw.composition.object_storage import build_object_storage
from auraclaw.config import get_settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.infrastructure.artifacts.s3 import (
    S3CompatibleMultipartClient,
    S3CompatiblePresigner,
)
from auraclaw.infrastructure.clients.artifact import RemoteSkillPackageUploadClient
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import artifact_routes

SETTINGS = get_settings()
pytestmark = pytest.mark.skipif(
    not SETTINGS.obs_enabled,
    reason="OBS S3 endpoint is not configured",
)


def test_obs_presigned_put_head_get_and_delete() -> None:
    async def scenario() -> None:
        assert SETTINGS.obs_ak is not None
        assert SETTINGS.obs_sk is not None
        presigner = S3CompatiblePresigner(
            SETTINGS.obs_s3_endpoint,
            access_key=SETTINGS.obs_ak.get_secret_value(),
            secret_key=SETTINGS.obs_sk.get_secret_value(),
            bucket=SETTINGS.obs_bucket,
            region=SETTINGS.obs_region,
            path_style=SETTINGS.obs_path_style,
        )
        key = f"integration/obs/{uuid4().hex}"
        content = b"auraclaw-obs-s3-boundary"
        put_url, _ = presigner.presign("PUT", key)
        head_url, _ = presigner.presign("HEAD", key)
        get_url, _ = presigner.presign("GET", key)
        delete_url, _ = presigner.presign("DELETE", key)
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                uploaded = await client.put(put_url, content=content)
                assert uploaded.status_code in {200, 201, 204}
                head = await client.head(head_url)
                assert head.status_code == 200
                assert int(head.headers["Content-Length"]) == len(content)
                downloaded = await client.get(get_url)
                assert downloaded.status_code == 200
                assert downloaded.content == content
            finally:
                deleted = await client.delete(delete_url)
                assert deleted.status_code in {200, 202, 204, 404}

    asyncio.run(scenario())


def test_obs_multipart_complete_and_recover_object() -> None:
    async def scenario() -> None:
        assert SETTINGS.obs_ak is not None
        assert SETTINGS.obs_sk is not None
        presigner = S3CompatiblePresigner(
            SETTINGS.obs_s3_endpoint,
            access_key=SETTINGS.obs_ak.get_secret_value(),
            secret_key=SETTINGS.obs_sk.get_secret_value(),
            bucket=SETTINGS.obs_bucket,
            region=SETTINGS.obs_region,
            path_style=SETTINGS.obs_path_style,
        )
        key = f"integration/obs-multipart/{uuid4().hex}"
        part_size = 5 * 1024 * 1024
        content = b"a" * part_size + b"recovered-tail"
        async with httpx.AsyncClient(timeout=60.0) as client:
            multipart = S3CompatibleMultipartClient(presigner, client=client)
            upload_id, part_urls = await multipart.create(
                key, expected_size=len(content), part_size=part_size
            )
            parts = []
            try:
                for index, url in enumerate(part_urls, start=1):
                    offset = (index - 1) * part_size
                    response = await client.put(
                        url, content=content[offset : offset + part_size]
                    )
                    assert response.status_code in {200, 201, 204}
                    assert response.headers.get("ETag")
                    parts.append(
                        {"part_number": index, "etag": response.headers["ETag"]}
                    )
                await multipart.complete(key, upload_id, tuple(parts))
                get_url, _ = presigner.presign("GET", key)
                downloaded = await client.get(get_url)
                assert downloaded.status_code == 200
                assert downloaded.content == content
            finally:
                delete_url, _ = presigner.presign("DELETE", key)
                deleted = await client.delete(delete_url)
                assert deleted.status_code in {200, 202, 204, 404}

    asyncio.run(scenario())


class _ProxyRepository:
    def __init__(self) -> None:
        self.uploads: dict[tuple[str, str, str], PendingUpload] = {}
        self.ready: dict[tuple[str, str, int], PendingUpload] = {}

    async def save_pending(self, pending: PendingUpload) -> None:
        self.uploads[(pending.tenant_id, pending.artifact_id, pending.upload_id)] = pending

    async def get_upload(
        self, tenant_id: str, artifact_id: str, upload_id: str
    ) -> PendingUpload | None:
        return self.uploads.get((tenant_id, artifact_id, upload_id))

    async def claim_finalize(self, pending: PendingUpload) -> PendingUpload:
        return replace(pending, finalize_claim_token="proxy-finalize")

    async def get_ready(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> PendingUpload | None:
        return self.ready.get((tenant_id, artifact_id, version))

    async def mark_multipart_completed(self, pending: PendingUpload) -> None:
        key = (pending.tenant_id, pending.artifact_id, pending.upload_id)
        self.uploads[key] = replace(pending, multipart_completed=True)

    async def mark_quarantined(self, pending: PendingUpload, reason: str) -> None:
        del pending, reason

    async def mark_ready(self, pending: PendingUpload, version: int) -> bool:
        self.ready[(pending.tenant_id, pending.artifact_id, version)] = pending
        return True


def test_obs_skill_proxy_upload_hides_object_store_for_single_and_multipart() -> None:
    async def scenario() -> None:
        storage = build_object_storage(SETTINGS)
        assert storage.verifier is not None
        assert storage.multipart is not None
        repository = _ProxyRepository()
        service = ArtifactInternalService(
            storage.presigner,
            repository=repository,
            object_verifier=storage.verifier,
            multipart=storage.multipart,
            multipart_threshold=5 * 1024 * 1024,
            multipart_part_size=5 * 1024 * 1024,
        )
        app = create_contract_app(
            "artifact-service",
            artifact_routes(service),
            workload_identities={"task-token": ServiceIdentity.TASK_API},
        )
        client = RemoteSkillPackageUploadClient(
            "http://artifact.test",
            bearer_token="task-token",
            transport=httpx.ASGITransport(app=app),
        )
        contents = (
            b'{"files":{"SKILL.md":"single"}}',
            b"m" * (5 * 1024 * 1024) + b"multipart-tail",
        )
        try:
            for index, content in enumerate(contents, start=1):
                checksum = hashlib.sha256(content).hexdigest()
                finalized = await client.stage(
                    tenant_id="tenant-obs-proxy",
                    name=f"proxy-{index}.skill.json",
                    content=content,
                    checksum=checksum,
                    correlation_id=f"proxy-{index}",
                    command_id=f"proxy-{index}",
                )
                assert finalized.status == "ready"
                assert finalized.artifact_ref["content_hash"] == checksum
            assert {record.upload_mode for record in repository.ready.values()} == {
                "single",
                "multipart",
            }
        finally:
            async with httpx.AsyncClient(timeout=30.0) as object_client:
                for pending in repository.uploads.values():
                    delete_url, _ = storage.presigner.presign(
                        "DELETE", pending.object_key
                    )
                    await object_client.delete(delete_url)
            await client.aclose()
            await storage.verifier.aclose()
            await storage.multipart.aclose()

    asyncio.run(scenario())
