import asyncio
from uuid import uuid4

import httpx
import pytest

from auraclaw.config import get_settings
from auraclaw.infrastructure.artifacts.s3 import (
    S3CompatibleMultipartClient,
    S3CompatiblePresigner,
)

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
