import asyncio
from uuid import uuid4

import httpx
import pytest

from auraclaw.config import get_settings
from auraclaw.infrastructure.artifacts.seaweedfs import SeaweedFSS3Presigner

SETTINGS = get_settings()
pytestmark = pytest.mark.skipif(
    not SETTINGS.seaweedfs_enabled,
    reason="SeaweedFS S3 endpoint is not configured",
)


def test_seaweedfs_presigned_put_head_get_and_delete() -> None:
    async def scenario() -> None:
        assert SETTINGS.seaweedfs_access_key is not None
        assert SETTINGS.seaweedfs_secret_key is not None
        presigner = SeaweedFSS3Presigner(
            SETTINGS.seaweedfs_s3_endpoint,
            access_key=SETTINGS.seaweedfs_access_key.get_secret_value(),
            secret_key=SETTINGS.seaweedfs_secret_key.get_secret_value(),
            bucket=SETTINGS.seaweedfs_bucket,
            region=SETTINGS.seaweedfs_region,
            path_style=SETTINGS.seaweedfs_path_style,
        )
        key = f"integration/s3/{uuid4().hex}"
        content = b"auraclaw-seaweedfs-s3-boundary"
        put_url, _ = presigner.presign("PUT", key)
        head_url, _ = presigner.presign("HEAD", key)
        get_url, _ = presigner.presign("GET", key)
        delete_url, _ = presigner.presign("DELETE", key)
        async with httpx.AsyncClient(timeout=15.0) as client:
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
