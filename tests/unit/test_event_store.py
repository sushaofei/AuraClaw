import asyncio
from dataclasses import replace

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore


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


def test_active_skill_reference_ends_only_with_run_terminal_event() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
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
                                "resolved_skills": [
                                    {"publisher": "acme", "name": "audit.verify"}
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

    asyncio.run(scenario())
