from __future__ import annotations

import asyncio
import hashlib

from auraclaw.action.skill_content_cache import SkillPackageContentCache
from auraclaw.action.skill_packages import (
    SkillPackage,
    skill_package_archive,
    skill_package_digest,
)
from auraclaw.contracts.skills import SkillManifest
from auraclaw.contracts.tools import ArtifactRef


class _Artifacts:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.read_calls = 0

    async def read(self, **kwargs: object) -> bytes:
        del kwargs
        self.read_calls += 1
        await asyncio.sleep(0.01)
        return self.content


def _package(name: str = "release.prepare") -> SkillPackage:
    manifest = SkillManifest(
        name=name,
        version="1.0.0",
        description="Prepare a release",
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    return SkillPackage(
        manifest=manifest,
        files={
            "manifest.json": manifest.model_dump_json().encode(),
            "SKILL.md": b"# Release\n",
        },
    )


def test_cache_coalesces_one_hundred_concurrent_cold_loads() -> None:
    async def scenario() -> None:
        package = _package()
        content = skill_package_archive(package)
        artifacts = _Artifacts(content)
        cache = SkillPackageContentCache(artifacts)
        digest = skill_package_digest(package)
        artifact_ref = ArtifactRef(
            artifact_id="art_skill",
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(content),
        )

        loaded = await asyncio.gather(
            *(
                cache.load(
                    tenant_id="tenant-a",
                    package_digest=digest,
                    artifact_ref=artifact_ref,
                    actor_id="action-hands-skill-rebuilder",
                    correlation_id=f"load-{index}",
                )
                for index in range(100)
            )
        )

        assert artifacts.read_calls == 1
        assert all(item.manifest.name == "release.prepare" for item in loaded)
        await cache.load(
            tenant_id="tenant-a",
            package_digest=digest,
            artifact_ref=artifact_ref,
            actor_id="task-api-skill-admin",
            correlation_id="warm-hit",
        )
        assert artifacts.read_calls == 1

    asyncio.run(scenario())


def test_cache_prunes_digests_removed_from_tenant_lifecycle() -> None:
    async def scenario() -> None:
        package = _package()
        content = skill_package_archive(package)
        artifacts = _Artifacts(content)
        cache = SkillPackageContentCache(artifacts)
        digest = skill_package_digest(package)
        artifact_ref = ArtifactRef(
            artifact_id="art_skill",
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(content),
        )
        arguments = {
            "tenant_id": "tenant-a",
            "package_digest": digest,
            "artifact_ref": artifact_ref,
            "actor_id": "action-hands-skill-rebuilder",
            "correlation_id": "load",
        }

        await cache.load(**arguments)
        assert await cache.prune_tenant(
            "tenant-a", retained_digests=frozenset()
        ) == 1
        await cache.load(**arguments)
        assert artifacts.read_calls == 2

    asyncio.run(scenario())
