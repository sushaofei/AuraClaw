import asyncio
from dataclasses import replace

import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.session.ports import StreamAppend


def test_same_command_is_idempotent() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
        context = CommandContext(
            command_id="cmd_1",
            tenant_id="tenant_1",
            actor=Actor(type="user", id="user_1"),
            correlation_id="corr_1",
            expected_version=0,
        )
        first = await store.append(
            root_session_id="ses_1",
            session_id="ses_1",
            run_id="run_1",
            context=context,
            events=[NewEvent(type="session.created", payload={})],
            command_result={"session_id": "ses_1"},
        )
        second = await store.append(
            root_session_id="ses_other",
            session_id="ses_other",
            run_id="run_other",
            context=context,
            events=[NewEvent(type="session.created", payload={})],
            command_result={"session_id": "ses_other"},
        )

        assert len(first.events) == 1
        assert second.deduplicated is True
        assert second.command_result == {"session_id": "ses_1"}
        assert await store.load("tenant_1", "ses_other") == []

    asyncio.run(scenario())


def test_batch_append_version_failure_rolls_back_every_stream() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
        context = CommandContext(
            command_id="seed",
            tenant_id="tenant_1",
            actor=Actor(type="coordinator", id="coordinator_1"),
            correlation_id="corr_1",
            expected_version=0,
            operation="seed",
        )
        await store.append(
            root_session_id="ses_root",
            session_id="ses_conflict",
            run_id=None,
            context=context,
            events=[NewEvent(type="seeded")],
            command_result={},
        )
        with pytest.raises(VersionConflictError):
            await store.append_batch(
                root_session_id="ses_root",
                context=replace(
                    context,
                    command_id="batch",
                    operation="collaboration.submit_plan",
                ),
                appends=(
                    StreamAppend(
                        session_id="ses_new",
                        run_id=None,
                        expected_version=0,
                        events=(NewEvent(type="child.created"),),
                    ),
                    StreamAppend(
                        session_id="ses_conflict",
                        run_id=None,
                        expected_version=0,
                        events=(NewEvent(type="child.created"),),
                    ),
                ),
                command_result={"status": "submitted"},
            )
        assert await store.load("tenant_1", "ses_new") == []
        assert [event.type for event in await store.load("tenant_1", "ses_conflict")] == [
            "seeded"
        ]

    asyncio.run(scenario())


def test_active_skill_reference_ends_only_with_run_terminal_event() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
        primary_digest = f"sha256:{'a' * 64}"
        dependency_digest = f"sha256:{'b' * 64}"
        base = CommandContext(
            command_id="activate-skill",
            tenant_id="tenant_1",
            actor=Actor(type="runtime", id="runtime_1"),
            correlation_id="run_1",
            expected_version=0,
        )
        await store.append(
            root_session_id="ses_1",
            session_id="ses_1",
            run_id="run_1",
            context=base,
            events=[
                NewEvent(
                    type="skill.activated",
                    payload={
                        "activation": {
                            "binding": {
                                "publisher": "acme",
                                "skill_name": "release.prepare",
                                "package_digest": primary_digest,
                                "resolved_skills": [
                                    {
                                        "publisher": "acme",
                                        "name": "audit.verify",
                                        "package_digest": dependency_digest,
                                    }
                                ],
                            }
                        }
                    },
                )
            ],
            command_result={},
        )
        assert await store.has_active_skill_reference(
            "tenant_1", "acme", "release.prepare"
        )
        assert await store.has_active_skill_reference(
            "tenant_1", "acme", "audit.verify"
        )
        assert not await store.has_active_skill_reference(
            "tenant_1", "other", "release.prepare"
        )
        assert await store.has_active_skill_reference(
            "tenant_1", "acme", "release.prepare", primary_digest
        )
        assert await store.has_active_skill_reference(
            "tenant_1", "acme", "audit.verify", dependency_digest
        )
        assert not await store.has_active_skill_reference(
            "tenant_1", "acme", "release.prepare", f"sha256:{'c' * 64}"
        )

        await store.append(
            root_session_id="ses_1",
            session_id="ses_1",
            run_id="run_1",
            context=replace(
                base,
                command_id="complete-run",
                expected_version=1,
            ),
            events=[NewEvent(type="run.completed", payload={})],
            command_result={},
        )
        assert not await store.has_active_skill_reference(
            "tenant_1", "acme", "release.prepare"
        )
        assert not await store.has_active_skill_reference(
            "tenant_1", "acme", "audit.verify"
        )
        assert not await store.has_active_skill_reference(
            "tenant_1", "acme", "release.prepare", primary_digest
        )

    asyncio.run(scenario())
