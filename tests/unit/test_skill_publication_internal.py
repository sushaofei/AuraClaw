from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime

import httpx
import pytest
from fastapi import FastAPI

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_internal_service import SkillPublicationInternalService
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.capabilities import CapabilityKind
from auraclaw.contracts.errors import SchemaValidationError
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PublishSkillCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationOperation,
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
        self.contents: dict[str, bytes] = {}

    async def put(self, **kwargs: object) -> ArtifactRef:
        self.calls += 1
        content = kwargs["content"]
        assert isinstance(content, bytes)
        self.contents["art_persisted_skill"] = content
        return ArtifactRef(
            artifact_id="art_persisted_skill",
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(content),
        )

    async def read(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        correlation_id: str,
    ) -> bytes:
        del tenant_id, actor_id, correlation_id
        return self.contents[artifact_ref.artifact_id]


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
        catalog_store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(catalog_store)
        rebuilder = SkillStateRebuilder(
            lifecycle=lifecycle,
            artifacts=artifacts,
            registry=registry,
            catalog=catalog,
        )
        management = SkillManagementService(
            lifecycle=lifecycle,
            projector=rebuilder,
        )
        contract = create_contract_app(
            "skill-publication-test",
            skill_publication_routes(
                SkillPublicationInternalService(
                    publication,
                    management=management,
                    rebuilder=rebuilder,
                )
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
            installation_state = await client.get_installation(
                "tenant-a", "platform", "release.prepare"
            )
            assert installation_state.revision == 1
            disabled = await client.change_installation(
                ChangeSkillInstallationCommand(
                    tenant_id="tenant-a",
                    actor_id="admin-b",
                    publisher="platform",
                    name="release.prepare",
                    operation=SkillInstallationOperation.DISABLE,
                    reason_code="tenant_disabled",
                    command_id="disable-internal-1",
                    expected_revision=1,
                    correlation_id="corr-disable",
                    causation_id="disable-internal-1",
                )
            )
            assert disabled.status.value == "disabled"
            assert await catalog.search(
                tenant_id="tenant-a", kinds=(CapabilityKind.SKILL,)
            ) == ()
            revoked = await client.revoke_publication(
                RevokeSkillPublicationCommand(
                    tenant_id="tenant-a",
                    actor_id="security-a",
                    publisher="platform",
                    name="release.prepare",
                    version="2.0.0",
                    reason_code="publisher_key_compromised",
                    command_id="revoke-internal-1",
                    expected_revision=1,
                    correlation_id="corr-revoke",
                    causation_id="revoke-internal-1",
                )
            )
            assert revoked.status.value == "revoked"
            publication_state = await client.get_publication(
                "tenant-a", "platform", "release.prepare", "2.0.0"
            )
            assert publication_state.revision == 2
            assert publication_state.updated_by == "security-a"
        finally:
            await client.aclose()

        assert result.artifact_ref.artifact_id == "art_persisted_skill"
        assert artifacts.calls == 1
        cached = compatibility_registry.get_publication(
            "tenant-a", "platform", "release.prepare", "2.0.0"
        )
        assert cached.status.value == "revoked"
        assert cached.package_digest == result.package_digest
        stored = await lifecycle.get_publication(
            "tenant-a", "platform", "release.prepare", "2.0.0"
        )
        assert stored is not None
        assert stored.created_by == "admin-a"
        assert stored.updated_by == "security-a"
        projected = await catalog.search(
            tenant_id="tenant-a", kinds=(CapabilityKind.SKILL,)
        )
        assert projected == ()

    asyncio.run(scenario())
