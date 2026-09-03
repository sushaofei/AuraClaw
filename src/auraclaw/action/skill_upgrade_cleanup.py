from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Protocol

from auraclaw.action.ports import ArtifactDeleter, SkillBindingReferenceReader
from auraclaw.action.skill_lifecycle import SkillLifecycleStore, is_replaced_package
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.skills import (
    SkillPackageRecord,
    SkillPublicationStatus,
    SkillRevocationAction,
    SkillUpgradeState,
)


class UpgradeProjector(Protocol):
    async def rebuild_tenant(self, tenant_id: str) -> object: ...


class SkillUpgradeCleanupWorker:
    """Reconcile the current upgrade; old packages are erased, never archived."""

    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        artifacts: ArtifactDeleter,
        references: SkillBindingReferenceReader,
        projector: UpgradeProjector,
        claim_ttl: timedelta = timedelta(seconds=30),
        concurrency: int = 4,
    ) -> None:
        if claim_ttl <= timedelta(0) or concurrency < 1:
            raise ValueError("Skill cleanup claim TTL and capacity must be positive")
        self._store, self._artifacts = lifecycle, artifacts
        self._references, self._projector = references, projector
        self._ttl, self._concurrency = claim_ttl, concurrency

    async def run_once(self, *, limit: int = 100) -> int:
        semaphore = asyncio.Semaphore(self._concurrency)

        async def process(state: SkillUpgradeState) -> bool:
            async with semaphore:
                return await self._process(state)

        return sum(
            await asyncio.gather(
                *(process(state) for state in await self._store.list_pending_upgrades(limit=limit))
            )
        )

    async def _old_packages(self, state: SkillUpgradeState) -> tuple[SkillPackageRecord, ...]:
        packages = (
            *await self._store.list_packages(state.tenant_id),
            *await self._store.list_package_tombstones(
                state.tenant_id, state.publisher, state.name
            ),
        )
        return tuple(
            {
                (p.manifest.version, p.package_digest, p.artifact_ref.artifact_id): p
                for p in packages
                if is_replaced_package(state, p)
            }.values()
        )

    async def _referenced(self, state: SkillUpgradeState, package: SkillPackageRecord) -> bool:
        return await self._references.has_active_skill_reference(
            tenant_id=state.tenant_id,
            publisher=state.publisher,
            name=state.name,
            package_digest=package.package_digest,
            correlation_id=state.correlation_id,
        )

    async def _process(self, state: SkillUpgradeState) -> bool:
        token = await self._store.claim_upgrade(state, ttl=self._ttl)
        if token is None:
            return False
        lost = asyncio.Event()

        async def fence() -> None:
            if lost.is_set() or not await self._store.renew_upgrade(state, token, ttl=self._ttl):
                raise VersionConflictError("Skill cleanup claim was lost")

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(self._ttl.total_seconds() / 3)
                try:
                    await fence()
                except Exception:
                    lost.set()
                    return

        task = asyncio.create_task(heartbeat())
        try:
            packages = await self._old_packages(state)
            for package in packages:
                if package.legal_hold:
                    await self._store.set_upgrade_phase(
                        state, token, phase="blocked", reason="skill_package_legal_hold"
                    )
                    return False
                if await self._referenced(state, package):
                    await self._store.set_upgrade_phase(
                        state, token, phase="draining", reason="skill_binding_active"
                    )
                    return False
            await fence()
            if not await self._store.set_upgrade_phase(state, token, phase="deleting"):
                return False
            for package in packages:
                await fence()
                publication = await self._store.get_publication(
                    state.tenant_id, state.publisher, state.name, package.manifest.version
                )
                # Close late admissions before erasing any bytes. Already-running bindings were
                # drained above; a late stale binding must fail its next authoritative check.
                if publication is not None and publication.package_digest == package.package_digest:
                    if (
                        publication.status is not SkillPublicationStatus.REVOKED
                        or publication.revocation_action is not SkillRevocationAction.CANCEL
                    ):
                        await self._store.put_publication(
                            publication.model_copy(
                                update={
                                    "status": SkillPublicationStatus.REVOKED,
                                    "revocation_action": SkillRevocationAction.CANCEL,
                                    "revision": publication.revision + 1,
                                    "updated_by": state.actor_id,
                                    "updated_at": datetime.now(UTC),
                                    "reason_code": "skill_version_replaced",
                                }
                            ),
                            expected_revision=publication.revision,
                        )
                await self._projector.rebuild_tenant(state.tenant_id)
                if await self._referenced(state, package):
                    await self._store.set_upgrade_phase(
                        state, token, phase="draining", reason="skill_binding_active"
                    )
                    return False
                await fence()
                await self._artifacts.purge(
                    tenant_id=state.tenant_id,
                    artifact_ref=package.artifact_ref,
                    actor_id=state.actor_id,
                    reason_code="skill_version_replaced",
                    correlation_id=state.correlation_id,
                )
                await fence()
                if not await self._store.remove_replaced_package(state, token, package):
                    raise VersionConflictError("Skill cleanup metadata changed")
            await fence()
            if await self._old_packages(state):
                raise VersionConflictError("Skill cleanup inventory changed")
            await self._projector.rebuild_tenant(state.tenant_id)
            return await self._store.set_upgrade_phase(state, token, phase="completed")
        except Exception as exc:
            await self._store.set_upgrade_phase(
                state, token, phase="blocked", reason=type(exc).__name__
            )
            return False
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
