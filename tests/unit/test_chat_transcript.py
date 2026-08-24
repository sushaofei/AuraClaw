import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from auraclaw.composition.providers import (
    get_approval_projection,
    get_event_store,
    get_task_projection,
    get_task_service,
)
from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.contracts.internal import (
    InternalRequestContext,
    ServiceIdentity,
    SessionFeedRequest,
)
from auraclaw.contracts.state import Visibility
from auraclaw.gateways.query.transcript import build_transcript
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.main import create_app
from auraclaw.session.internal_service import SessionInternalService


def setup_function() -> None:
    get_settings().storage_backend = "memory"
    get_settings().runtime_event_backend = "memory"
    get_task_service.cache_clear()
    get_event_store.cache_clear()
    get_task_projection.cache_clear()
    get_approval_projection.cache_clear()


def _event(
    *,
    event_id: str,
    session_id: str,
    version: int,
    event_type: str,
    payload: dict,
    run_id: str | None = None,
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=event_id,
        tenant_id="tenant-1",
        root_session_id=session_id,
        session_id=session_id,
        run_id=run_id,
        aggregate_version=version,
        type=event_type,
        occurred_at=datetime(2026, 7, 21, 10, 0, version, tzinfo=UTC),
        actor=Actor(type="user", id="u1"),
        correlation_id="corr",
        causation_id="cause",
        visibility=Visibility.INTERNAL,
        schema_version=1,
        payload=payload,
    )


def test_build_transcript_filters_messages_and_pending_approval() -> None:
    session_id = "ses_1"
    events = [
        _event(
            event_id="e1",
            session_id=session_id,
            version=1,
            event_type="session.created",
            payload={"goal": "第一问"},
        ),
        _event(
            event_id="e2",
            session_id=session_id,
            version=2,
            event_type="tool.call.started",
            payload={"tool_name": "ignored"},
        ),
        _event(
            event_id="e3",
            session_id=session_id,
            version=3,
            event_type="model.output.completed",
            payload={"output": "第一答"},
            run_id="run-1",
        ),
        _event(
            event_id="e4",
            session_id=session_id,
            version=4,
            event_type="approval.requested",
            payload={
                "approval_id": "apr_1",
                "tool_name": "shell",
                "reason": "needs review",
                "risk": "high",
                "redacted_arguments": {"cmd": "ls"},
                "expected_effect": "list files",
                "status": "waiting",
            },
        ),
    ]
    transcript = build_transcript(events)
    assert [item["content"] for item in transcript["messages"]] == ["第一问", "第一答"]
    assert transcript["messages"][1]["run_id"] == "run-1"
    assert transcript["pending_approval"]["approval_id"] == "apr_1"
    assert transcript["pending_approval"]["tool_name"] == "shell"


def test_event_store_load_supports_type_filter_and_limit() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
        context = CommandContext(
            command_id="cmd-1",
            tenant_id="tenant-1",
            actor=Actor(type="user", id="u1"),
            correlation_id="corr",
            expected_version=0,
        )
        await store.append(
            root_session_id="ses_1",
            session_id="ses_1",
            run_id=None,
            context=context,
            events=[
                NewEvent(type="session.created", payload={"goal": "g"}),
                NewEvent(type="tool.call.started", payload={}),
                NewEvent(type="model.output.completed", payload={"output": "a"}),
            ],
            command_result={"session_id": "ses_1"},
        )
        filtered = await store.load(
            "tenant-1",
            "ses_1",
            event_types=("session.created", "model.output.completed"),
        )
        assert [event.type for event in filtered] == [
            "session.created",
            "model.output.completed",
        ]
        limited = await store.load("tenant-1", "ses_1", limit=1)
        assert len(limited) == 1
        assert limited[0].type == "session.created"

    asyncio.run(scenario())


def test_postgres_event_store_filters_on_event_type_column() -> None:
    async def scenario() -> None:
        from auraclaw.infrastructure.persistence.postgres_event_store import (
            PostgresEventStore,
        )

        store = PostgresEventStore("postgresql://unused")
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=[])
        store.pool = AsyncMock(return_value=pool)  # type: ignore[method-assign]
        await store.load(
            "tenant-1",
            "ses_1",
            event_types=("session.created", "model.output.completed"),
            limit=10,
        )
        query, *_params = pool.fetch.await_args.args
        assert "event_type = ANY" in query
        assert "type = ANY" not in query.replace("event_type = ANY", "")

    asyncio.run(scenario())


def test_session_feed_pushes_limit_to_store() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
        service = SessionInternalService(store, lease_verifier=AsyncMock())
        context = CommandContext(
            command_id="cmd-feed",
            tenant_id="tenant-1",
            actor=Actor(type="user", id="u1"),
            correlation_id="corr",
            expected_version=0,
        )
        await store.append(
            root_session_id="ses_feed",
            session_id="ses_feed",
            run_id=None,
            context=context,
            events=[
                NewEvent(type="session.created", payload={"goal": "g"}),
                NewEvent(type="run.requested", payload={}),
                NewEvent(type="model.output.completed", payload={"output": "a"}),
            ],
            command_result={"session_id": "ses_feed"},
        )
        request = SessionFeedRequest(
            context=InternalRequestContext(
                tenant_id="tenant-1",
                service_identity=ServiceIdentity.TASK_API,
                request_id="req-1",
                correlation_id="corr-1",
                causation_id="cause-1",
            ),
            session_id="ses_feed",
            from_version=1,
            limit=2,
        )
        page = await service.feed(request)
        assert len(page.events) == 2
        assert page.next_version == 3
        filtered = await service.feed(
            SessionFeedRequest(
                context=request.context,
                session_id="ses_feed",
                from_version=1,
                limit=10,
                event_types=("session.created", "model.output.completed"),
            )
        )
        assert [event["type"] for event in filtered.events] == [
            "session.created",
            "model.output.completed",
        ]
        assert filtered.next_version is None

    asyncio.run(scenario())


def test_transcript_api_returns_messages_without_timeline_noise() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "cmd-transcript-1", "X-Tenant-ID": "tenant-1"},
            json={"goal": "介绍 AuraClaw"},
        )
        assert created.status_code == 202
        session_id = created.json()["session_id"]

        store = get_event_store()
        context = CommandContext(
            command_id="cmd-append-model",
            tenant_id="tenant-1",
            actor=Actor(type="runtime", id="runtime-1"),
            correlation_id="corr-model",
            expected_version=2,
        )

        async def append_noise() -> None:
            await store.append(
                root_session_id=session_id,
                session_id=session_id,
                run_id="run-1",
                context=context,
                events=[
                    NewEvent(type="tool.call.started", payload={"tool_name": "noise"}),
                    NewEvent(
                        type="model.output.completed",
                        payload={"output": "这是回答"},
                    ),
                ],
                command_result={"ok": True},
            )

        asyncio.run(append_noise())
        response = client.get(
            f"/v1/tasks/{session_id}/transcript",
            headers={"X-Tenant-ID": "tenant-1"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["session_id"] == session_id
        contents = [item["content"] for item in body["messages"]]
        assert contents[0] == "介绍 AuraClaw"
        assert "这是回答" in contents
        missing = client.get(
            "/v1/tasks/ses_missing/transcript",
            headers={"X-Tenant-ID": "tenant-1"},
        )
        assert missing.status_code == 404
