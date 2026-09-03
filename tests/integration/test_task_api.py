import asyncio
from datetime import UTC, datetime, timedelta

import httpx
from fastapi.testclient import TestClient

from auraclaw.composition.providers import (
    get_approval_projection,
    get_collaboration_projection,
    get_event_store,
    get_sync_invocation_gateway,
    get_task_projection,
    get_task_result_waiter,
    get_task_service,
)
from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.main import create_app


def setup_function() -> None:
    get_settings().storage_backend = "memory"
    get_settings().runtime_event_backend = "memory"
    get_settings().allow_insecure_identity_headers = True
    get_settings().sync_invoke_poll_interval_seconds = 0.05
    get_task_service.cache_clear()
    get_event_store.cache_clear()
    get_task_projection.cache_clear()
    get_approval_projection.cache_clear()
    get_collaboration_projection.cache_clear()
    get_task_result_waiter.cache_clear()
    get_sync_invocation_gateway.cache_clear()


def test_cancelled_run_leaves_root_session_ready_and_session_can_be_closed() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "cmd-create-1", "X-Tenant-ID": "tenant-1"},
            json={"goal": "prepare a release plan"},
        )
        assert created.status_code == 202
        session_id = created.json()["session_id"]

        task = client.get(f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-1"})
        assert task.status_code == 200
        assert task.json()["status"] == "pending"
        assert task.json()["projection_version"] == 2

        unchanged = client.get(
            f"/v1/tasks/{session_id}",
            headers={"X-Tenant-ID": "tenant-1", "If-None-Match": task.headers["etag"]},
        )
        assert unchanged.status_code == 304

        cancelled = client.post(
            f"/v1/sessions/{session_id}/cancel",
            headers={
                "Idempotency-Key": "cmd-cancel-1",
                "X-Tenant-ID": "tenant-1",
                "X-Expected-Version": "2",
            },
            json={"reason": "no longer needed"},
        )
        assert cancelled.status_code == 202

        final = client.get(f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-1"})
        assert final.json()["status"] == "ready"
        assert final.json()["run_status"] == "cancelled"
        assert final.json()["projection_version"] == 3

        closed = client.post(
            f"/v1/sessions/{session_id}/close",
            headers={
                "Idempotency-Key": "cmd-close-1",
                "X-Tenant-ID": "tenant-1",
                "X-Expected-Version": "3",
            },
            json={"reason": "conversation finished"},
        )
        assert closed.status_code == 202
        assert closed.json()["status"] == "closed"

        rejected = client.post(
            f"/v1/sessions/{session_id}/messages",
            headers={
                "Idempotency-Key": "append-after-close",
                "X-Tenant-ID": "tenant-1",
                "X-Expected-Version": "4",
            },
            json={"message": "too late"},
        )
        assert rejected.status_code == 409
        assert rejected.json()["code"] == "invalid_transition"


def test_create_is_idempotent() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        headers = {"Idempotency-Key": "stable-key", "X-Tenant-ID": "tenant-1"}
        first = client.post("/v1/tasks", headers=headers, json={"goal": "one"})
        second = client.post("/v1/tasks", headers=headers, json={"goal": "one"})

        assert first.status_code == 202
        assert second.status_code == 202
        assert first.json() == second.json()


def test_tenant_cannot_read_another_tenants_task() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "tenant-bound", "X-Tenant-ID": "tenant-1"},
            json={"goal": "private task"},
        )
        session_id = created.json()["session_id"]

        denied = client.get(f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-2"})
        assert denied.status_code == 404


def test_append_message_is_idempotent_and_honors_min_version() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "create-message-task", "X-Tenant-ID": "tenant-1"},
            json={"goal": "collect feedback"},
        )
        session_id = created.json()["session_id"]
        headers = {
            "Idempotency-Key": "append-message-1",
            "X-Tenant-ID": "tenant-1",
            "X-Expected-Version": "2",
        }
        first = client.post(
            f"/v1/sessions/{session_id}/messages",
            headers=headers,
            json={"message": "include customer notes"},
        )
        second = client.post(
            f"/v1/sessions/{session_id}/messages",
            headers=headers,
            json={"message": "this duplicate payload is ignored"},
        )
        assert first.status_code == 202
        assert second.json() == first.json()

        task = client.get(
            f"/v1/tasks/{session_id}?min_version=4", headers={"X-Tenant-ID": "tenant-1"}
        )
        assert task.status_code == 202
        assert task.headers["retry-after"] == "1"
        assert task.json()["projection_version"] == 3


def test_human_approval_response_enters_through_task_gateway() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "approval-task", "X-Tenant-ID": "tenant-approval"},
            json={"goal": "perform a controlled write"},
        )
        session_id = created.json()["session_id"]
        run_id = created.json()["run_id"]

        async def seed_approval() -> None:
            store = get_event_store()
            result = await store.append(
                root_session_id=session_id,
                session_id=session_id,
                run_id=run_id,
                context=CommandContext(
                    command_id="runtime-approval-request",
                    tenant_id="tenant-approval",
                    actor=Actor(type="runtime", id="runtime-1"),
                    correlation_id=run_id,
                    expected_version=2,
                    operation="runtime.approval.requested",
                ),
                events=[
                    NewEvent(
                        type="approval.requested",
                        payload={
                            "approval_id": "apr-api-test",
                            "run_id": run_id,
                            "action_digest": "digest-api-test",
                            "tool_name": "controlled-write",
                            "redacted_arguments": {"target": "release"},
                            "risk": "high",
                            "reason": "write requires approval",
                            "expected_effect": "write",
                            "allowed_decisions": ["approved", "rejected"],
                            "assigned_approvers": ["approver-1"],
                            "policy_version": "m3-v1",
                            "expires_at": (
                                datetime.now(UTC) + timedelta(hours=1)
                            ).isoformat(),
                            "status": "waiting",
                        },
                    )
                ],
                command_result={"approval_id": "apr-api-test"},
            )
            await get_task_projection().project(result.events)

        asyncio.run(seed_approval())
        waiting = client.get(
            f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-approval"}
        )
        assert waiting.json()["status"] == "waiting_for_human"

        response = client.post(
            f"/v1/sessions/{session_id}/approvals/apr-api-test/responses",
            headers={
                "Idempotency-Key": "approval-response-1",
                "X-Tenant-ID": "tenant-approval",
                "X-Actor-ID": "approver-1",
                "X-Expected-Version": "3",
            },
            json={"decision": "approved", "feedback": "proceed"},
        )
        assert response.status_code == 202
        assert response.json()["decision"] == "approved"
        assert response.json()["status"] == "runnable"

        task = client.get(
            f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-approval"}
        )
        assert task.json()["status"] == "runnable"
        assert task.json()["projection_version"] == 5


def test_create_records_source_and_lists_root_tasks() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        headers = {"X-Tenant-ID": "tenant-list"}
        chat = client.post(
            "/v1/tasks",
            headers={**headers, "Idempotency-Key": "list-chat"},
            json={"goal": "chat goal", "source": "chat"},
        )
        scheduled = client.post(
            "/v1/tasks",
            headers={**headers, "Idempotency-Key": "list-schedule"},
            json={
                "goal": "scheduled goal",
                "source": "schedule",
                "schedule_id": "sch_daily",
                "occurrence_id": "2026-08-24T01:00:00Z",
            },
        )
        rejected = client.post(
            "/v1/tasks",
            headers={**headers, "Idempotency-Key": "list-bad-schedule"},
            json={"goal": "missing occurrence", "source": "schedule", "schedule_id": "sch_x"},
        )
        assert chat.status_code == 202
        assert scheduled.status_code == 202
        assert rejected.status_code == 422

        chat_task = client.get(
            f"/v1/tasks/{chat.json()['session_id']}", headers=headers
        )
        assert chat_task.json()["source"] == "chat"
        scheduled_task = client.get(
            f"/v1/tasks/{scheduled.json()['session_id']}", headers=headers
        )
        assert scheduled_task.json()["source"] == "schedule"
        assert scheduled_task.json()["schedule_id"] == "sch_daily"

        listed = client.get("/v1/tasks", headers=headers)
        assert listed.status_code == 200
        session_ids = {item["session_id"] for item in listed.json()["tasks"]}
        assert chat.json()["session_id"] in session_ids
        assert scheduled.json()["session_id"] in session_ids

        chats = client.get("/v1/tasks", headers=headers, params={"kind": "chat"})
        assert {item["source"] for item in chats.json()["tasks"]} == {"chat"}

        other = client.get("/v1/tasks", headers={"X-Tenant-ID": "tenant-other"})
        assert other.json()["tasks"] == []


def _async_client(app: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


def test_sync_invoke_times_out_without_cancelling_the_session() -> None:
    async def scenario() -> None:
        app = create_app(profile="task-api")
        async with _async_client(app) as client:
            response = await client.post(
                "/v1/tasks/sync",
                headers={"Idempotency-Key": "sync-timeout", "X-Tenant-ID": "tenant-sync"},
                json={"goal": "a long running analysis", "timeout_seconds": 1},
            )
            assert response.status_code == 202
            body = response.json()
            assert body["wait_outcome"] == "timeout"
            assert body["status"] == "pending"
            assert "result_url" in body
            session_id = body["session_id"]
            task = await client.get(
                f"/v1/tasks/{session_id}", headers={"X-Tenant-ID": "tenant-sync"}
            )
            assert task.status_code == 200
            assert task.json()["run_status"] == "pending"

    asyncio.run(scenario())


def test_sync_invoke_reuses_idempotent_session_and_returns_cancelled() -> None:
    async def scenario() -> None:
        app = create_app(profile="task-api")
        headers = {"X-Tenant-ID": "tenant-sync-cancel"}
        async with _async_client(app) as client:
            created = await client.post(
                "/v1/tasks",
                headers={**headers, "Idempotency-Key": "sync-same-key"},
                json={"goal": "cancel while waiting", "interaction_mode": "non_streaming"},
            )
            assert created.status_code == 202
            session_id = created.json()["session_id"]
            wait = asyncio.create_task(
                client.post(
                    "/v1/tasks/sync",
                    headers={**headers, "Idempotency-Key": "sync-same-key"},
                    json={"goal": "cancel while waiting", "timeout_seconds": 5},
                )
            )
            await asyncio.sleep(0.05)
            async with _async_client(app) as other:
                cancelled = await other.post(
                    f"/v1/sessions/{session_id}/cancel",
                    headers={
                        **headers,
                        "Idempotency-Key": "sync-cancel-run",
                        "X-Expected-Version": "2",
                    },
                    json={"reason": "stop waiting"},
                )
            assert cancelled.status_code == 202
            response = await wait
            assert response.status_code == 200
            body = response.json()
            assert body["session_id"] == session_id
            assert body["wait_outcome"] == "cancelled"
            assert body["status"] == "cancelled"

    asyncio.run(scenario())


def test_result_wait_returns_after_cancel() -> None:
    async def scenario() -> None:
        app = create_app(profile="task-api")
        headers = {"X-Tenant-ID": "tenant-result-wait"}
        async with _async_client(app) as client:
            created = await client.post(
                "/v1/tasks",
                headers={**headers, "Idempotency-Key": "result-wait-create"},
                json={"goal": "wait on existing session"},
            )
            session_id = created.json()["session_id"]
            wait = asyncio.create_task(
                client.get(
                    f"/v1/tasks/{session_id}/result",
                    headers=headers,
                    params={"wait": "true", "timeout_seconds": 5},
                )
            )
            await asyncio.sleep(0.05)
            async with _async_client(app) as other:
                await other.post(
                    f"/v1/sessions/{session_id}/cancel",
                    headers={
                        **headers,
                        "Idempotency-Key": "result-wait-cancel",
                        "X-Expected-Version": "2",
                    },
                    json={"reason": "done"},
                )
            response = await wait
            assert response.status_code == 200
            assert response.json()["wait_outcome"] == "cancelled"

            snapshot = await client.get(
                f"/v1/tasks/{session_id}/result", headers=headers
            )
            assert snapshot.status_code == 200
            assert "wait_outcome" not in snapshot.json()

    asyncio.run(scenario())


def test_result_wait_returns_conflict_when_waiting_for_human() -> None:
    async def scenario() -> None:
        app = create_app(profile="task-api")
        headers = {"X-Tenant-ID": "tenant-sync-hitl"}
        async with _async_client(app) as client:
            created = await client.post(
                "/v1/tasks",
                headers={**headers, "Idempotency-Key": "sync-hitl-create"},
                json={"goal": "needs approval"},
            )
            session_id = created.json()["session_id"]
            run_id = created.json()["run_id"]
            store = get_event_store()
            result = await store.append(
                root_session_id=session_id,
                session_id=session_id,
                run_id=run_id,
                context=CommandContext(
                    command_id="runtime-approval-request-sync",
                    tenant_id="tenant-sync-hitl",
                    actor=Actor(type="runtime", id="runtime-1"),
                    correlation_id=run_id,
                    expected_version=2,
                    operation="runtime.approval.requested",
                ),
                events=[
                    NewEvent(
                        type="approval.requested",
                        payload={
                            "approval_id": "apr-sync",
                            "run_id": run_id,
                            "action_digest": "digest-sync",
                            "tool_name": "controlled-write",
                            "redacted_arguments": {"target": "release"},
                            "risk": "high",
                            "reason": "write requires approval",
                            "expected_effect": "write",
                            "allowed_decisions": ["approved", "rejected"],
                            "assigned_approvers": ["approver-1"],
                            "policy_version": "m3-v1",
                            "expires_at": (
                                datetime.now(UTC) + timedelta(hours=1)
                            ).isoformat(),
                            "status": "waiting",
                        },
                    )
                ],
                command_result={"approval_id": "apr-sync"},
            )
            await get_task_projection().project(result.events)

            response = await client.get(
                f"/v1/tasks/{session_id}/result",
                headers=headers,
                params={"wait": "true", "timeout_seconds": 5},
            )
            assert response.status_code == 409
            body = response.json()
            assert body["code"] == "needs_human"
            assert body["wait_outcome"] == "needs_human"

    asyncio.run(scenario())


def test_sync_invoke_rejects_schedule_fields() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        response = client.post(
            "/v1/tasks/sync",
            headers={"Idempotency-Key": "sync-schedule", "X-Tenant-ID": "tenant-1"},
            json={
                "goal": "timer must not use sync",
                "source": "schedule",
                "schedule_id": "sch_daily",
                "occurrence_id": "2026-08-25T01:00:00Z",
            },
        )
        assert response.status_code == 422


def test_create_task_still_returns_accepted() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        response = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "still-async", "X-Tenant-ID": "tenant-1"},
            json={"goal": "keep 202"},
        )
        assert response.status_code == 202
        assert "wait_outcome" not in response.json()
