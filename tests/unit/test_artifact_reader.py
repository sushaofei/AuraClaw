from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.contracts.internal import INTERNAL_API_VERSION
from auraclaw.contracts.tools import ArtifactRef, PolicyDecision
from auraclaw.infrastructure.clients.artifact_reader import RemoteArtifactReader


class _Policy:
    async def evaluate_action(self, **kwargs: object) -> PolicyEvaluation:
        assert kwargs["action"] == "artifact.download"
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="decision-1",
            policy_version="test",
        )


def test_remote_artifact_reader_verifies_downloaded_content() -> None:
    async def scenario() -> None:
        content = b"persisted-skill-package"
        artifact = ArtifactRef(
            artifact_id="art_skill",
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(content),
        )

        def contract_handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["X-AuraClaw-Contract-Version"] == (
                INTERNAL_API_VERSION
            )
            assert request.headers["Authorization"] == "Bearer hands-token"
            return httpx.Response(
                200,
                json={
                    "api_version": INTERNAL_API_VERSION,
                    "download_url": "https://objects.test/skill",
                    "expires_at": (
                        datetime.now(UTC) + timedelta(minutes=5)
                    ).isoformat(),
                },
            )

        def object_handler(request: httpx.Request) -> httpx.Response:
            assert str(request.url) == "https://objects.test/skill"
            return httpx.Response(200, content=content)

        reader = RemoteArtifactReader(
            "http://artifact-service",
            bearer_token="hands-token",
            policy=_Policy(),
            transport=httpx.MockTransport(contract_handler),
            object_transport=httpx.MockTransport(object_handler),
        )
        try:
            assert await reader.read(
                tenant_id="tenant-a",
                artifact_ref=artifact,
                actor_id="action-hands-skill-rebuilder",
                correlation_id="skill-rebuild:tenant-a",
            ) == content
            with pytest.raises(ArtifactAccessError, match="digest"):
                await reader.read(
                    tenant_id="tenant-a",
                    artifact_ref=replace(artifact, content_hash="0" * 64),
                    actor_id="action-hands-skill-rebuilder",
                    correlation_id="skill-rebuild:tenant-a",
                )
            limited_reader = RemoteArtifactReader(
                "http://artifact-service",
                bearer_token="hands-token",
                policy=_Policy(),
                max_content_bytes=8,
                transport=httpx.MockTransport(contract_handler),
                object_transport=httpx.MockTransport(object_handler),
            )
            try:
                with pytest.raises(ArtifactAccessError, match="too large"):
                    await limited_reader.read(
                        tenant_id="tenant-a",
                        artifact_ref=replace(artifact, size=8),
                        actor_id="action-hands-skill-rebuilder",
                        correlation_id="skill-rebuild:tenant-a",
                    )
            finally:
                await limited_reader.aclose()
        finally:
            await reader.aclose()

    asyncio.run(scenario())
