import asyncio

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.infrastructure.memory import InMemoryEventStore


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
