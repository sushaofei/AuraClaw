from __future__ import annotations

from dataclasses import dataclass

from auraclaw.action.ports import SkillArtifactLifecycle
from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.tools import ArtifactRef


@dataclass(frozen=True)
class SkillReliabilityResult:
    outbox_completed: int = 0
    outbox_failed: int = 0
    orphans_deleted: int = 0
    references_repaired: int = 0
    orphan_failed: int = 0


class SkillPublicationReliabilityWorker:
    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        artifacts: SkillArtifactLifecycle,
        rebuilder: SkillStateRebuilder,
        owner: str,
    ) -> None:
        self._lifecycle = lifecycle
        self._artifacts = artifacts
        self._rebuilder = rebuilder
        self._owner = owner

    async def run_once(self, *, limit: int = 100) -> SkillReliabilityResult:
        completed = 0
        outbox_failed = 0
        deleted = 0
        repaired = 0
        orphan_failed = 0
        try:
            outbox = await self._lifecycle.claim_outbox(
                owner=self._owner, limit=limit
            )
        except Exception:
            return SkillReliabilityResult(outbox_failed=1)
        for record in outbox:
            try:
                artifact_payload = record.payload.get("artifact_ref")
                if not isinstance(artifact_payload, dict):
                    raise ValueError("Skill outbox Artifact Ref is invalid")
                artifact_ref = ArtifactRef(
                    artifact_id=str(artifact_payload["artifact_id"]),
                    version=int(artifact_payload["version"]),
                    content_hash=str(artifact_payload["content_hash"]),
                    media_type=str(artifact_payload["media_type"]),
                    size=int(artifact_payload["size"]),
                )
                package_digest = str(record.payload["package_digest"])
                correlation_id = f"skill-outbox:{record.outbox_id}"
                await self._artifacts.claim_publication(
                    tenant_id=record.tenant_id,
                    artifact_ref=artifact_ref,
                    command_id=record.command_id,
                    correlation_id=correlation_id,
                )
                await self._artifacts.bind_publication(
                    tenant_id=record.tenant_id,
                    artifact_ref=artifact_ref,
                    command_id=record.command_id,
                    package_digest=package_digest,
                    correlation_id=correlation_id,
                )
                await self._rebuilder.rebuild_tenant(record.tenant_id)
                await self._lifecycle.complete_outbox(
                    outbox_id=record.outbox_id, owner=self._owner
                )
                completed += 1
            except Exception as exc:
                await self._lifecycle.fail_outbox(
                    outbox_id=record.outbox_id,
                    owner=self._owner,
                    safe_error_code=type(exc).__name__,
                )
                outbox_failed += 1

        try:
            orphans = await self._artifacts.claim_orphans(
                owner=self._owner, limit=limit
            )
        except Exception:
            return SkillReliabilityResult(
                outbox_completed=completed,
                outbox_failed=outbox_failed,
                orphans_deleted=deleted,
                references_repaired=repaired,
                orphan_failed=orphan_failed + 1,
            )
        for orphan in orphans:
            try:
                referenced = await self._lifecycle.has_artifact_reference(
                    orphan.tenant_id,
                    orphan.artifact_ref.artifact_id,
                    orphan.artifact_ref.version,
                )
                status = await self._artifacts.resolve_orphan(
                    tenant_id=orphan.tenant_id,
                    orphan=orphan,
                    referenced=referenced,
                    package_digest=(
                        f"sha256:{orphan.artifact_ref.content_hash}"
                        if referenced
                        else None
                    ),
                    correlation_id=f"skill-orphan:{orphan.artifact_ref.artifact_id}",
                )
                if status == "deleted":
                    deleted += 1
                else:
                    repaired += 1
            except Exception:
                orphan_failed += 1
        return SkillReliabilityResult(
            outbox_completed=completed,
            outbox_failed=outbox_failed,
            orphans_deleted=deleted,
            references_repaired=repaired,
            orphan_failed=orphan_failed,
        )
