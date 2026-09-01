from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime

import pytest

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
    SkillResolver,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.capabilities import CapabilityKind
from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.skills import (
    PublishSkillCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationStatus,
    SkillManifest,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef

_KEY = b"skill-rebuild-test-key"


class _Artifacts:
    def __init__(self) -> None:
        self.contents: dict[str, bytes] = {}
        self.read_calls = 0

    async def put(self, **kwargs: object) -> ArtifactRef:
        content = kwargs["content"]
        media_type = kwargs["media_type"]
        assert isinstance(content, bytes)
        assert isinstance(media_type, str)
        artifact_id = f"art_{len(self.contents) + 1}"
        self.contents[artifact_id] = content
        return ArtifactRef(
            artifact_id=artifact_id,
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type=media_type,
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
        self.read_calls += 1
        return self.contents[artifact_ref.artifact_id]


def _package() -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"platform": _KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version="3.0.0",
        description="Prepare an auditable release",
        applies_when=("release requested",),
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {"SKILL.md": b"# Release\n\nPrepare the release."}
    manifest = unsigned.model_copy(
        update={"signature": verifier.sign(unsigned, files)}
    )
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def test_rebuild_restores_registry_catalog_and_installation_visibility() -> None:
    async def scenario() -> None:
        now = datetime.now(UTC)
        artifacts = _Artifacts()
        lifecycle = InMemorySkillLifecycleStore()
        publishing_registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier({"platform": _KEY}),
        )
        publisher = SkillPublicationService(
            registry=publishing_registry,
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
        published = await publisher.publish(
            PublishSkillCommand(
                tenant_id="tenant-a",
                actor_id="admin-a",
                source_id="sks_admin_upload",
                command_id="publish-rebuild-1",
                correlation_id="corr-1",
                causation_id="publish-rebuild-1",
            ),
            _package(),
        )

        restored_registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier({"platform": _KEY}),
            resources=HandsResourceRegistry(),
        )
        catalog_store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(catalog_store)
        rebuilder = SkillStateRebuilder(
            lifecycle=lifecycle,
            artifacts=artifacts,
            registry=restored_registry,
            catalog=catalog,
        )
        cold_binding = await SkillResolver(
            restored_registry,
            catalog_store,
            reload_tenant=rebuilder.rebuild_tenant,
        ).resolve(
            tenant_id="tenant-a",
            name="release.prepare",
            role="worker",
            policy_version="test",
        )
        assert cold_binding.package_digest == published.package_digest
        result = await rebuilder.rebuild_all()

        assert result.publication_count == 1
        assert result.failure_count == 0
        assert artifacts.read_calls == 1
        for _ in range(10):
            assert await rebuilder.rebuild_tenant("tenant-a") == (1, ())
        assert artifacts.read_calls == 1
        matches = await catalog.search(
            tenant_id="tenant-a",
            query="release",
            kinds=(CapabilityKind.SKILL,),
        )
        assert len(matches) == 1
        assert matches[0].content_digest == published.package_digest
        binding = await SkillResolver(restored_registry, catalog_store).resolve(
            tenant_id="tenant-a",
            name="release.prepare",
            role="worker",
            policy_version="test",
        )
        assert binding.package_digest == published.package_digest

        installation = await lifecycle.get_installation(
            "tenant-a", "platform", "release.prepare"
        )
        assert installation is not None
        await lifecycle.put_installation(
            installation.model_copy(
                update={
                    "status": SkillInstallationStatus.DISABLED,
                    "revision": 2,
                    "updated_by": "admin-a",
                    "updated_at": datetime.now(UTC),
                    "reason_code": "tenant_disabled",
                }
            ),
            expected_revision=1,
        )
        count, failures = await rebuilder.rebuild_tenant("tenant-a")
        assert count == 1
        assert failures == ()
        assert await catalog.search(
            tenant_id="tenant-a", kinds=(CapabilityKind.SKILL,)
        ) == ()
        assert restored_registry.candidates("tenant-a", "release.prepare") == ()
        assert restored_registry.load_part(
            "tenant-a",
            publisher=published.manifest.publisher,
            name=published.manifest.name,
            version=published.manifest.version,
            package_digest=published.package_digest,
            path="SKILL.md",
        ) == b"# Release\n\nPrepare the release."

        revoked = await SkillManagementService(
            lifecycle=lifecycle,
            projector=rebuilder,
        ).revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id="tenant-a",
                actor_id="security-a",
                publisher=published.manifest.publisher,
                name=published.manifest.name,
                version=published.manifest.version,
                reason_code="publisher_key_compromised",
                command_id="revoke-rebuild-1",
                expected_revision=1,
                correlation_id="corr-revoke",
                causation_id="revoke-rebuild-1",
            )
        )
        assert revoked.status.value == "revoked"
        with pytest.raises(NotFoundError):
            restored_registry.load_part(
                "tenant-a",
                publisher=published.manifest.publisher,
                name=published.manifest.name,
                version=published.manifest.version,
                package_digest=published.package_digest,
                path="SKILL.md",
            )

    asyncio.run(scenario())


def test_rebuild_tenant_replays_when_a_new_generation_arrives() -> None:
    async def scenario() -> None:
        artifacts = _Artifacts()
        rebuilder = SkillStateRebuilder(
            lifecycle=InMemorySkillLifecycleStore(),
            artifacts=artifacts,
            registry=SkillPackageRegistry(
                artifacts=artifacts,
                signature_verifier=HmacSkillSignatureVerifier(
                    {"platform": _KEY}
                ),
            ),
            catalog=CapabilityCatalog(InMemoryCapabilityCatalogStore()),
        )
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        calls = 0

        async def rebuild_once(
            tenant_id: str,
        ) -> tuple[int, tuple[str, ...]]:
            nonlocal calls
            assert tenant_id == "tenant-a"
            calls += 1
            if calls == 1:
                first_started.set()
                await release_first.wait()
            return 0, ()

        rebuilder._rebuild_tenant_locked = rebuild_once  # type: ignore[method-assign]
        first = asyncio.create_task(rebuilder.rebuild_tenant("tenant-a"))
        await first_started.wait()
        second = asyncio.create_task(rebuilder.rebuild_tenant("tenant-a"))
        await asyncio.sleep(0)
        release_first.set()

        assert await first == (0, ())
        assert await second == (0, ())
        assert calls == 2

    asyncio.run(scenario())
