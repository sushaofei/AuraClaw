from __future__ import annotations

import asyncio
from datetime import timedelta

from auraclaw.action.ports import SkillArtifactOrphan
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_reliability import SkillPublicationReliabilityWorker
from auraclaw.contracts.skills import (
    PublishSkillCommand,
    SkillManifest,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage

_KEY = b"skill-reliability-test-key"


class _Artifacts:
    def __init__(
        self, published: ArtifactRef, orphan: ArtifactRef, *, delay: float = 0.0
    ) -> None:
        self.published = published
        self.orphan = orphan
        self.claims: list[str] = []
        self.binds: list[str] = []
        self.resolutions: list[tuple[str, bool]] = []
        self._returned_orphans = False
        self.delay = delay

    async def claim_publication(self, **kwargs: object) -> None:
        await asyncio.sleep(self.delay)
        self.claims.append(str(kwargs["command_id"]))

    async def bind_publication(self, **kwargs: object) -> None:
        self.binds.append(str(kwargs["package_digest"]))

    async def claim_orphans(
        self, *, owner: str, limit: int = 100
    ) -> tuple[SkillArtifactOrphan, ...]:
        del owner, limit
        if self._returned_orphans:
            return ()
        self._returned_orphans = True
        return (
            SkillArtifactOrphan("tenant-a", self.published, "claim-referenced"),
            SkillArtifactOrphan("tenant-a", self.orphan, "claim-orphan"),
        )

    async def resolve_orphan(self, **kwargs: object) -> str:
        orphan = kwargs["orphan"]
        assert isinstance(orphan, SkillArtifactOrphan)
        referenced = bool(kwargs["referenced"])
        self.resolutions.append((orphan.artifact_ref.artifact_id, referenced))
        return "retained" if referenced else "deleted"


class _Rebuilder:
    def __init__(self) -> None:
        self.tenants: list[str] = []

    async def rebuild_tenant(self, tenant_id: str) -> tuple[int, tuple[str, ...]]:
        self.tenants.append(tenant_id)
        return 1, ()


def _package(version: str = "1.0.0") -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"acme": _KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version=version,
        description="Prepare release",
        publisher="acme",
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


def test_reliability_worker_delivers_outbox_and_repairs_or_deletes_orphans() -> None:
    async def scenario() -> None:
        lifecycle = InMemorySkillLifecycleStore()
        registry = SkillPackageRegistry(
            artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=_KEY),
            signature_verifier=HmacSkillSignatureVerifier({"acme": _KEY}),
        )
        service = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
        )
        published = await service.publish(
            PublishSkillCommand(
                tenant_id="tenant-a",
                actor_id="admin-a",
                command_id="publish-a",
                correlation_id="corr-a",
                causation_id="publish-a",
            ),
            _package(),
        )
        unrelated = ArtifactRef(
            artifact_id="art-unpublished",
            version=1,
            content_hash="f" * 64,
            media_type="application/vnd.auraclaw.skill-package+json",
            size=10,
        )
        artifacts = _Artifacts(published.artifact_ref, unrelated)
        rebuilder = _Rebuilder()
        worker = SkillPublicationReliabilityWorker(
            lifecycle=lifecycle,
            artifacts=artifacts,
            rebuilder=rebuilder,  # type: ignore[arg-type]
            owner="hands-a",
        )
        result = await worker.run_once()
        assert result.outbox_completed == 1
        assert result.references_repaired == 1
        assert result.orphans_deleted == 1
        assert result.outbox_failed == result.orphan_failed == 0
        assert rebuilder.tenants == ["tenant-a"]
        assert artifacts.claims == ["publish-a"]
        assert artifacts.binds == [published.package_digest]
        assert artifacts.resolutions == [
            (published.artifact_ref.artifact_id, True),
            ("art-unpublished", False),
        ]
        assert await lifecycle.claim_outbox(owner="hands-b") == ()

    asyncio.run(scenario())


def test_reliability_worker_renews_and_coalesces_same_tenant_rebuild() -> None:
    async def scenario() -> None:
        lifecycle = InMemorySkillLifecycleStore()
        registry = SkillPackageRegistry(
            artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=_KEY),
            signature_verifier=HmacSkillSignatureVerifier({"acme": _KEY}),
        )
        service = SkillPublicationService(
            registry=registry,
            lifecycle=lifecycle,
        )
        publications = []
        for index, version in enumerate(("1.0.0", "1.0.1"), start=1):
            publications.append(
                await service.publish(
                    PublishSkillCommand(
                        tenant_id="tenant-a",
                        actor_id="admin-a",
                        command_id=f"publish-{index}",
                        correlation_id=f"corr-{index}",
                        causation_id=f"publish-{index}",
                    ),
                    _package(version),
                )
            )
        artifacts = _Artifacts(
            publications[0].artifact_ref,
            ArtifactRef("orphan", 1, "f" * 64, "application/octet-stream", 1),
            delay=0.04,
        )
        rebuilder = _Rebuilder()
        worker = SkillPublicationReliabilityWorker(
            lifecycle=lifecycle,
            artifacts=artifacts,
            rebuilder=rebuilder,  # type: ignore[arg-type]
            owner="hands-a",
            claim_ttl=timedelta(seconds=0.03),
        )
        result = await worker.run_once()
        assert result.outbox_completed == 2
        assert result.outbox_failed == 0
        assert rebuilder.tenants == ["tenant-a"]
        assert await lifecycle.claim_outbox(owner="hands-b") == ()

    asyncio.run(scenario())
