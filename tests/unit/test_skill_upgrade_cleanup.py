from __future__ import annotations

import asyncio
from datetime import timedelta

from tests.unit.test_skill_publication import _command, _package, _service

from auraclaw.action.skill_upgrade_cleanup import SkillUpgradeCleanupWorker
from auraclaw.contracts.skills import SkillPublicationStatus


class _Artifacts:
    failed = False

    def __init__(self):
        self.calls = []

    async def purge(self, **kwargs):
        self.calls.append(kwargs["artifact_ref"])
        if self.failed:
            raise RuntimeError("temporary object storage failure")


class _References:
    active = True

    async def has_active_skill_reference(self, **kwargs):
        return self.active


class _Projector:
    async def rebuild_tenant(self, tenant_id):
        return None


def test_upgrade_drains_retries_and_erases_old_package_metadata_and_replay_material() -> None:
    async def scenario():
        service, store = _service()
        old = await service.publish(_command(), _package(version="1.0.0"))
        current = await service.publish(_command(command_id="upgrade"), _package(version="2.0.0"))
        assert current.upgrade is not None
        artifacts, refs = _Artifacts(), _References()
        worker = SkillUpgradeCleanupWorker(
            lifecycle=store, artifacts=artifacts, references=refs, projector=_Projector()
        )
        assert await worker.run_once() == 0
        assert artifacts.calls == []
        assert (await store.get_upgrade("tenant-a", "acme", "release.prepare")).phase == "draining"
        refs.active = False
        artifacts.failed = True
        assert await worker.run_once() == 0
        assert (await store.get_upgrade("tenant-a", "acme", "release.prepare")).phase == "blocked"
        assert await store.get_package("tenant-a", "acme", "release.prepare", "1.0.0")
        artifacts.failed = False
        assert await worker.run_once() == 1
        assert await store.get_package("tenant-a", "acme", "release.prepare", "1.0.0") is None
        assert await store.get_publication("tenant-a", "acme", "release.prepare", "1.0.0") is None
        assert await store.list_package_tombstones("tenant-a", "acme", "release.prepare") == ()
        assert store._commands[("tenant-a", "publish-1")][1] is None
        assert all(
            record.payload.get("package_digest") != old.package_digest
            for record in store._outbox.values()
        )
        assert (
            await store.get_publication("tenant-a", "acme", "release.prepare", "2.0.0")
        ).status is SkillPublicationStatus.ACTIVE
        assert await worker.run_once() == 0
        assert len(artifacts.calls) == 2
        assert (await store.get_upgrade("tenant-a", "acme", "release.prepare")).phase == "completed"

    asyncio.run(scenario())


def test_cleanup_claim_excludes_other_workers_and_fences_a_replaced_generation() -> None:
    async def scenario():
        service, store = _service()
        await service.publish(_command(), _package(version="1.0.0"))
        second = await service.publish(_command(command_id="upgrade"), _package(version="2.0.0"))
        token = await store.claim_upgrade(second.upgrade, ttl=timedelta(seconds=30))
        assert token
        assert await store.claim_upgrade(second.upgrade, ttl=timedelta(seconds=30)) is None
        await service.publish(_command(command_id="newer"), _package(version="3.0.0"))
        assert not await store.renew_upgrade(second.upgrade, token, ttl=timedelta(seconds=30))
        assert not await store.set_upgrade_phase(second.upgrade, token, phase="completed")

    asyncio.run(scenario())


def test_late_activation_after_old_entry_is_closed_delays_physical_removal() -> None:
    async def scenario():
        service, store = _service()
        await service.publish(_command(), _package(version="1.0.0"))
        await service.publish(_command(command_id="upgrade"), _package(version="2.0.0"))
        artifacts = _Artifacts()

        class References(_References):
            count = 0

            async def has_active_skill_reference(self, **kwargs):
                self.count += 1
                return self.count == 2

        refs = References()
        worker = SkillUpgradeCleanupWorker(
            lifecycle=store, artifacts=artifacts, references=refs, projector=_Projector()
        )
        assert await worker.run_once() == 0 and not artifacts.calls
        assert (await store.get_upgrade("tenant-a", "acme", "release.prepare")).phase == "draining"
        old = await store.get_publication("tenant-a", "acme", "release.prepare", "1.0.0")
        assert old.revocation_action.value == "cancel"
        assert await worker.run_once() == 1

    asyncio.run(scenario())
