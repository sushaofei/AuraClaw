from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import pytest
from fastapi import FastAPI

from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_internal_service import SkillPublicationInternalService
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.contracts.errors import SchemaValidationError
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.contracts.skills import (
    PublishSkillCommand,
    SkillManifest,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.clients.skill_publication import (
    RemoteSkillPublicationClient,
)
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import skill_publication_routes

_KEY = b"internal-skill-publication-key"


class _ArtifactWriter:
    def __init__(self) -> None:
        self.calls = 0

    async def put(self, **kwargs: object) -> ArtifactRef:
        self.calls += 1
        content = kwargs["content"]
        assert isinstance(content, bytes)
        return ArtifactRef(
            artifact_id="art_persisted_skill",
            version=1,
            content_hash="a" * 64,
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(content),
        )


def _package() -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"platform": _KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version="2.0.0",
        description="Prepare a release",
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {"SKILL.md": b"# Release\n"}
    manifest = unsigned.model_copy(
        update={"signature": verifier.sign(unsigned, files)}
    )
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def test_task_api_client_publishes_through_action_hands_service() -> None:
    async def scenario() -> None:
        now = datetime.now(UTC)
        artifacts = _ArtifactWriter()
        lifecycle = InMemorySkillLifecycleStore()
        registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier({"platform": _KEY}),
            resources=HandsResourceRegistry(),
        )
        publication = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
            bootstrap_sources=(
                SkillSourceRecord(
                    source_id="sks_admin_upload",
                    tenant_id="*",
                    kind=SkillSourceKind.ADMIN_UPLOAD,
                    desired_state=SkillSourceDesiredState.ENABLED,
                    publisher_allowlist=("platform",),
                    created_by="system",
                    updated_by="system",
                    created_at=now,
                    updated_at=now,
                ),
            ),
        )
        contract = create_contract_app(
            "skill-publication-test",
            skill_publication_routes(
                SkillPublicationInternalService(publication)
            ),
            workload_identities={"task-token": ServiceIdentity.TASK_API},
        )
        app = FastAPI()
        app.mount("/internal/v1/skill-publications", contract)
        compatibility_registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier({"platform": _KEY}),
            resources=HandsResourceRegistry(),
        )
        client = RemoteSkillPublicationClient(
            "http://hands",
            bearer_token="task-token",
            transport=httpx.ASGITransport(app=app),
            compatibility_cache=compatibility_registry,
        )
        try:
            result = await client.publish(
                PublishSkillCommand(
                    tenant_id="tenant-a",
                    actor_id="admin-a",
                    source_id="sks_admin_upload",
                    command_id="publish-internal-1",
                    expected_revision=0,
                    correlation_id="corr-1",
                    causation_id="publish-internal-1",
                ),
                _package(),
            )
            valid = _package()
            unsafe = SkillPackage(
                manifest=valid.manifest,
                files={**valid.files, "../escape.md": b"unsafe"},
            )
            with pytest.raises(SchemaValidationError):
                await client.publish(
                    PublishSkillCommand(
                        tenant_id="tenant-a",
                        actor_id="admin-a",
                        source_id="sks_admin_upload",
                        command_id="publish-internal-unsafe",
                        expected_revision=0,
                        correlation_id="corr-unsafe",
                        causation_id="publish-internal-unsafe",
                    ),
                    unsafe,
                )
        finally:
            await client.aclose()

        assert result.artifact_ref.artifact_id == "art_persisted_skill"
        assert artifacts.calls == 1
        assert (
            compatibility_registry.get_publication(
                "tenant-a", "platform", "release.prepare", "2.0.0"
            )
            == result
        )
        stored = await lifecycle.get_publication(
            "tenant-a", "platform", "release.prepare", "2.0.0"
        )
        assert stored is not None
        assert stored.created_by == "admin-a"

    asyncio.run(scenario())
