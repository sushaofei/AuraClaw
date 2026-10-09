from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from auraclaw.artifact.internal_service import PendingUpload
from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.infrastructure.artifacts.scanner import RemoteArtifactContentScanner


def _pending() -> PendingUpload:
    return PendingUpload(
        tenant_id="tenant-a",
        artifact_id="artifact-a",
        upload_id="upload-a",
        object_key="objects/a",
        root_session_id="root-a",
        session_id="session-a",
        name="report.pdf",
        media_type="application/pdf",
        expected_size=42,
        expected_checksum="a" * 64,
        classification="confidential",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )


@pytest.mark.asyncio
async def test_remote_artifact_scanner_sends_bounded_metadata_and_accepts_clean() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer scanner-workload-token"
        assert request.url.path == "/v1/artifacts:scan"
        payload = request.read()
        assert b"scanner-workload-token" not in payload
        assert b"https://objects.example/signed" in payload
        return httpx.Response(
            200,
            json={"verdict": "clean", "policy_version": "artifact-content-v1"},
        )

    scanner = RemoteArtifactContentScanner(
        "https://scanner.example",
        bearer_token="scanner-workload-token",
        policy_version="artifact-content-v1",
        transport=httpx.MockTransport(handler),
    )
    try:
        result = await scanner.scan(
            _pending(), download_url="https://objects.example/signed"
        )
        assert result.verdict == "clean"
        assert result.finding_code is None
    finally:
        await scanner.aclose()


@pytest.mark.asyncio
async def test_remote_artifact_scanner_rejects_untrusted_response_values() -> None:
    scanner = RemoteArtifactContentScanner(
        "https://scanner.example",
        bearer_token="scanner-workload-token",
        policy_version="artifact-content-v1",
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "verdict": "quarantined",
                    "policy_version": "artifact-content-v1",
                    "finding_code": "../../unsafe",
                },
            )
        ),
    )
    try:
        with pytest.raises(ArtifactAccessError, match="finding is invalid"):
            await scanner.scan(_pending(), download_url="https://objects.example/signed")
    finally:
        await scanner.aclose()
