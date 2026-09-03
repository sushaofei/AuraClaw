from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import pytest

from auraclaw.artifact.internal_service import PendingUpload
from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.infrastructure.artifacts.s3 import S3CompatibleObjectVerifier, S3CompatiblePresigner


def _pending() -> PendingUpload:
    return PendingUpload(
        tenant_id="t",
        artifact_id="a",
        upload_id="u",
        object_key="t/old-skill",
        root_session_id="r",
        session_id="s",
        name="old",
        media_type="application/vnd.auraclaw.skill-package+json",
        expected_size=1,
        expected_checksum="0" * 64,
        classification="internal",
        expires_at=datetime.now(UTC),
    )


@pytest.mark.parametrize("status", ["Enabled", "Suspended", "disabled"])
def test_purge_deletes_exact_object_versions_and_markers(status: str) -> None:
    async def scenario() -> None:
        remaining = {"v1": "Version", "v2": "Version", "marker": "DeleteMarker"}
        deleted: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            query = request.url.params
            if request.method == "DELETE":
                assert request.url.path == "/bucket/t/old-skill"
                version = query.get("versionId")
                deleted.append(version)
                if version is not None:
                    remaining.pop(version)
                return httpx.Response(204)
            if "versioning" in query:
                content = f"<Status>{status}</Status>" if status != "disabled" else ""
                return httpx.Response(
                    200, text=f"<VersioningConfiguration>{content}</VersioningConfiguration>"
                )
            assert query["prefix"] == "t/old-skill"
            # One exact version per page plus a similarly named object that must never be removed.
            items = list(remaining.items())[:1]
            content = "".join(
                f"<{kind}><Key>t/old-skill</Key><VersionId>{version}</VersionId></{kind}>"
                for version, kind in items
            )
            return httpx.Response(
                200,
                text=f"<ListVersionsResult>{content}"
                "<Version><Key>t/old-skill-other</Key><VersionId>keep</VersionId></Version>"
                "</ListVersionsResult>",
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            verifier = S3CompatibleObjectVerifier(
                S3CompatiblePresigner(
                    "https://fixture.invalid",
                    access_key="fixture",
                    secret_key="fixture",
                    bucket="bucket",
                    region="us-east-1",
                ),
                client=client,
            )
            assert await verifier.purge(_pending())
        assert deleted == ([None] if status == "disabled" else ["v1", "v2", "marker"])
        if status != "disabled":
            assert not remaining

    asyncio.run(scenario())


def test_purge_does_not_claim_success_when_version_access_is_denied() -> None:
    async def scenario() -> None:
        methods = []

        def handler(request):
            methods.append(request.method)
            return httpx.Response(403)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            verifier = S3CompatibleObjectVerifier(
                S3CompatiblePresigner(
                    "https://fixture.invalid",
                    access_key="fixture",
                    secret_key="fixture",
                    bucket="bucket",
                    region="us-east-1",
                ),
                client=client,
            )
            with pytest.raises(ArtifactAccessError, match="verify complete"):
                await verifier.purge(_pending())
        assert methods == ["GET"]

    asyncio.run(scenario())
