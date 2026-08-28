from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime

import pytest

from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
    skill_package_archive,
    skill_package_digest,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.contracts.errors import (
    InvalidTransitionError,
    PolicyDeniedError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    PublishSkillCommand,
    SkillInstallationStatus,
    SkillManifest,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage

_KEY = b"skill-publication-test-key"


def _package(*, markdown: bytes = b"# Release\n") -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"acme": _KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version="1.0.0",
        description="Prepare a release",
        publisher="acme",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {"SKILL.md": markdown}
    manifest = unsigned.model_copy(update={"signature": verifier.sign(unsigned, files)})
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def _source(*, publishers: tuple[str, ...] = ("acme",)) -> SkillSourceRecord:
    now = datetime.now(UTC)
    return SkillSourceRecord(
        source_id="sks_admin_upload",
        tenant_id="tenant-a",
        kind=SkillSourceKind.ADMIN_UPLOAD,
        desired_state=SkillSourceDesiredState.ENABLED,
        publisher_allowlist=publishers,
        created_by="system",
        updated_by="system",
        created_at=now,
        updated_at=now,
    )


def _command(*, activate: bool = True, expected_revision: int = 0) -> PublishSkillCommand:
    return PublishSkillCommand(
        tenant_id="tenant-a",
        actor_id="admin-a",
        source_id="sks_admin_upload",
        activate=activate,
        command_id="publish-1",
        expected_revision=expected_revision,
        correlation_id="corr-1",
        causation_id="publish-1",
    )


def _service(
    source: SkillSourceRecord,
) -> tuple[SkillPublicationService, InMemorySkillLifecycleStore]:
    lifecycle = InMemorySkillLifecycleStore()
    registry = SkillPackageRegistry(
        artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=_KEY),
        signature_verifier=HmacSkillSignatureVerifier({"acme": _KEY}),
        resources=HandsResourceRegistry(),
    )
    return (
        SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
            bootstrap_sources=(source,),
        ),
        lifecycle,
    )


def test_publish_service_persists_package_publication_and_installation() -> None:
    async def scenario() -> None:
        service, lifecycle = _service(_source())
        first = await service.publish(_command(), _package())
        repeated = await service.publish(_command(), _package())

        assert repeated == first
        publication = await lifecycle.get_publication(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert publication is not None
        assert publication.status is SkillPublicationStatus.ACTIVE
        assert publication.revision == 1
        installation = await lifecycle.get_installation("tenant-a", "acme", "release.prepare")
        assert installation is not None
        assert installation.status is SkillInstallationStatus.ACTIVE
        assert installation.pinned_package_digest == first.package_digest
        assert installation.auto_upgrade is False

        with pytest.raises(VersionConflictError, match="immutable"):
            await service.publish(_command(), _package(markdown=b"# Changed\n"))

    asyncio.run(scenario())


def test_staged_publication_requires_revision_to_activate() -> None:
    async def scenario() -> None:
        service, lifecycle = _service(_source())
        staged = await service.publish(_command(activate=False), _package())
        assert staged.status is SkillPublicationStatus.STAGED
        assert await lifecycle.get_installation("tenant-a", "acme", "release.prepare") is None

        with pytest.raises(VersionConflictError, match="revision conflict"):
            await service.publish(_command(activate=True), _package())
        active = await service.publish(_command(activate=True, expected_revision=1), _package())
        assert active.status is SkillPublicationStatus.ACTIVE
        publication = await lifecycle.get_publication(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert publication is not None and publication.revision == 2

    asyncio.run(scenario())


def test_publish_service_enforces_source_publisher_allowlist() -> None:
    async def scenario() -> None:
        service, _ = _service(_source(publishers=("other",)))
        with pytest.raises(PolicyDeniedError, match="publisher"):
            await service.publish(_command(), _package())

    asyncio.run(scenario())


def test_publish_service_cannot_reactivate_revoked_publication() -> None:
    async def scenario() -> None:
        service, lifecycle = _service(_source())
        await service.publish(_command(), _package())
        publication = await lifecycle.get_publication(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert publication is not None
        await lifecycle.put_publication(
            publication.model_copy(
                update={
                    "status": SkillPublicationStatus.REVOKED,
                    "revision": 2,
                    "reason_code": "security_revoke",
                    "updated_at": datetime.now(UTC),
                }
            ),
            expected_revision=1,
        )

        with pytest.raises(InvalidTransitionError, match="staged to active"):
            await service.publish(_command(activate=True, expected_revision=2), _package())

    asyncio.run(scenario())


def test_staged_artifact_publish_reuses_validated_immutable_artifact() -> None:
    class StagedArtifacts:
        def __init__(self, content: bytes) -> None:
            self.content = content
            self.reads = 0

        async def put(self, **kwargs: object) -> ArtifactRef:
            del kwargs
            raise AssertionError("staged publication must not duplicate the Artifact")

        async def read(self, **kwargs: object) -> bytes:
            del kwargs
            self.reads += 1
            return self.content

    async def scenario() -> None:
        package = _package()
        archive = skill_package_archive(package)
        artifacts = StagedArtifacts(archive)
        lifecycle = InMemorySkillLifecycleStore()
        registry = SkillPackageRegistry(
            artifacts=artifacts,
            signature_verifier=HmacSkillSignatureVerifier({"acme": _KEY}),
        )
        service = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
            artifacts=artifacts,
            bootstrap_sources=(_source(),),
        )
        artifact_ref = ArtifactRef(
            artifact_id="art_staged",
            version=1,
            content_hash=hashlib.sha256(archive).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(archive),
        )
        published = await service.publish_artifact(
            _command(), artifact_ref, skill_package_digest(package)
        )
        assert published.artifact_ref == artifact_ref
        assert artifacts.reads == 1
        stored = await lifecycle.get_package(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert stored is not None and stored.artifact_ref == artifact_ref

        with pytest.raises(VersionConflictError, match="Artifact digest"):
            await service.publish_artifact(
                _command(),
                artifact_ref,
                f"sha256:{'0' * 64}",
            )

    asyncio.run(scenario())
