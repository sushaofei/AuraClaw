import asyncio
import json
from datetime import UTC, datetime, timedelta

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
from auraclaw.contracts.state import Visibility
from auraclaw.gateways.query.activity import build_activity, page_activity
from auraclaw.main import create_app
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.ports import ModelPolicy, ModelRequest


def setup_function() -> None:
    get_settings().storage_backend = "memory"
    get_settings().runtime_event_backend = "memory"
    get_task_service.cache_clear()
    get_event_store.cache_clear()
    get_task_projection.cache_clear()
    get_approval_projection.cache_clear()


def _event(
    version: int,
    event_type: str,
    payload: dict,
    *,
    run_id: str | None = "run-1",
    tenant_id: str = "tenant-1",
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"evt-{version}",
        tenant_id=tenant_id,
        root_session_id="ses-1",
        session_id="ses-1",
        run_id=run_id,
        aggregate_version=version,
        type=event_type,
        occurred_at=datetime(2026, 8, 26, tzinfo=UTC) + timedelta(seconds=version),
        actor=Actor(type="runtime", id="runtime-1"),
        correlation_id="corr-1",
        causation_id=f"cause-{version}",
        visibility=Visibility.INTERNAL,
        schema_version=1,
        payload=payload,
    )


def test_activity_folds_lifecycles_redacts_and_isolates_runs() -> None:
    events = [
        _event(1, "session.created", {"goal": "分析价格"}, run_id=None),
        _event(2, "run.requested", {"run_id": "run-1"}),
        _event(
            3,
            "model.input.prepared",
            {
                "model_call_id": "mdl-1",
                "message_count": 2,
                "user_prompt_preview": "分析价格 Bearer hidden-token",
                "input_digest": "sha256:abc",
            },
        ),
        _event(
            4,
            "tool.call.requested",
            {
                "tool_invocation_id": "tool-1",
                "name": "price.profile",
                "arguments": {"region": "华东", "api_token": "must-not-leak"},
                "activity": {
                    "source": "mcp",
                    "server_id": "price-server",
                    "capability_id": "cap-1",
                },
            },
        ),
        _event(
            5,
            "tool.call.completed",
            {
                "tool_invocation_id": "tool-1",
                "name": "price.profile",
                "result": {"status": "success", "authorization": "Bearer abc"},
            },
        ),
        _event(
            6,
            "skill.activated",
            {
                "skill_activation_id": "skill-1",
                "skill_name": "price-insight",
                "skill_version": "1.0.0",
            },
        ),
        _event(
            7,
            "skill.completed",
            {
                "skill_activation_id": "skill-1",
                "skill_name": "price-insight",
                "output_summary": "分析完成",
            },
        ),
        _event(
            8,
            "model.output.completed",
            {"model_call_id": "mdl-1", "output": "最终结论", "usage": {"output_tokens": 8}},
        ),
        _event(9, "run.completed", {"run_id": "run-1", "result_summary": "完成"}),
        _event(10, "run.requested", {"run_id": "run-2"}, run_id="run-2"),
        _event(
            11,
            "tool.call.requested",
            {"tool_invocation_id": "tool-1", "name": "price.profile", "arguments": {}},
            run_id="run-2",
        ),
    ]

    nodes = build_activity(events)
    tool_nodes = [node for node in nodes if node["type"] == "tool"]
    assert len(tool_nodes) == 2
    first_tool = next(node for node in tool_nodes if node["run_id"] == "run-1")
    assert first_tool["status"] == "completed"
    assert first_tool["sequence"] == 4
    assert first_tool["updated_version"] == 5
    assert first_tool["duration_ms"] == 1_000
    serialized = json.dumps(first_tool, ensure_ascii=False)
    assert "must-not-leak" not in serialized
    assert "Bearer abc" not in serialized
    model_input = next(node for node in nodes if node["type"] == "model_input")
    assert "hidden-token" not in json.dumps(model_input, ensure_ascii=False)
    assert "[REDACTED]" in serialized

    skill = next(node for node in nodes if node["type"] == "skill")
    assert skill["status"] == "completed"
    assert skill["sequence"] == 6
    assert skill["updated_version"] == 7


def test_activity_incremental_page_returns_updated_nodes() -> None:
    nodes = build_activity(
        [
            _event(1, "run.requested", {"run_id": "run-1"}),
            _event(
                2,
                "tool.call.requested",
                {"tool_invocation_id": "tool-1", "name": "lookup", "arguments": {}},
            ),
            _event(
                5,
                "tool.call.completed",
                {"tool_invocation_id": "tool-1", "name": "lookup", "result": {}},
            ),
            _event(6, "run.completed", {"run_id": "run-1"}),
        ]
    )
    first = page_activity(nodes, after_version=0, limit=1)
    assert first["has_more"] is True
    assert first["next_after_version"] == 5
    assert first["nodes"][0]["id"].startswith("tool:")
    second = page_activity(nodes, after_version=5, limit=10)
    assert [node["type"] for node in second["nodes"]] == ["run"]
    assert second["next_after_version"] == 6


def test_activity_handles_retry_cancel_failure_old_events_and_large_results() -> None:
    nodes = build_activity(
        [
            _event(1, "run.requested", {"run_id": "run-1"}),
            _event(2, "run.retry_scheduled", {"run_id": "run-1", "reason": "busy"}),
            _event(3, "run.failed", {"run_id": "run-1", "error": "provider failed"}),
            _event(
                4,
                "tool.call.requested",
                {"name": "legacy.tool", "arguments": {}},
                run_id="run-2",
            ),
            _event(
                5,
                "tool.call.completed",
                {"name": "legacy.tool", "result": {"content": "x" * 40_000}},
                run_id="run-2",
            ),
            _event(6, "run.cancelled", {"run_id": "run-2"}, run_id="run-2"),
            _event(
                7,
                "approval.expired",
                {"approval_id": "approval-old", "reason": "expired"},
                run_id="run-2",
            ),
        ]
    )
    runs = {node["run_id"]: node for node in nodes if node["type"] == "run"}
    assert runs["run-1"]["status"] == "failed"
    assert runs["run-2"]["status"] == "cancelled"
    legacy_nodes = [node for node in nodes if node["type"] == "tool"]
    assert [node["status"] for node in legacy_nodes] == ["running", "completed"]
    assert all(node["id"].startswith("tool:run-2:evt-") for node in legacy_nodes)
    serialized = json.dumps(legacy_nodes[1]["detail"])
    assert len(serialized) < 6_000
    assert '"truncated": true' in serialized
    approval = next(node for node in nodes if node["type"] == "approval")
    assert approval["status"] == "failed"


def test_model_input_evidence_never_persists_trusted_prompt_content() -> None:
    evidence = AgentHarness._model_input_evidence(
        ModelRequest(
            model_call_id="mdl-1",
            tenant_id="tenant-1",
            run_id="run-1",
            messages=(
                {"role": "system", "content": "private signed skill instructions"},
                {"role": "user", "content": "分析价格"},
            ),
            tools=(
                {
                    "type": "function",
                    "function": {"name": "lookup", "parameters": {"type": "object"}},
                },
            ),
            policy=ModelPolicy(preferred_model="model-a", allowed_providers=("provider-a",)),
        ),
        turn_index=2,
    )
    serialized = json.dumps(evidence, ensure_ascii=False)
    assert "private signed skill instructions" not in serialized
    assert evidence["user_prompt_preview"] == "分析价格"
    assert evidence["trusted_instruction_count"] == 1
    assert evidence["tool_names"] == ["lookup"]
    assert str(evidence["input_digest"]).startswith("sha256:")


def test_activity_api_is_tenant_scoped_and_returns_version_headers() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "cmd-activity-1", "X-Tenant-ID": "tenant-1"},
            json={"goal": "分析价格"},
        )
        assert created.status_code == 202
        session_id = created.json()["session_id"]
        store = get_event_store()

        async def append_activity() -> None:
            await store.append(
                root_session_id=session_id,
                session_id=session_id,
                run_id="run-1",
                context=CommandContext(
                    command_id="cmd-activity-events",
                    tenant_id="tenant-1",
                    actor=Actor(type="runtime", id="runtime-1"),
                    correlation_id="corr-activity",
                    expected_version=2,
                ),
                events=[
                    NewEvent(
                        type="model.input.prepared",
                        payload={
                            "model_call_id": "mdl-1",
                            "message_count": 1,
                            "user_prompt_preview": "分析价格",
                            "input_digest": "sha256:abc",
                        },
                    )
                ],
                command_result={"ok": True},
            )

        asyncio.run(append_activity())
        response = client.get(
            f"/v1/tasks/{session_id}/activity?after_version=0&limit=200",
            headers={"X-Tenant-ID": "tenant-1"},
        )
        assert response.status_code == 200
        assert response.headers["cache-control"] == "private, no-store"
        assert response.headers["x-activity-version"] == "3"
        assert response.json()["source_version"] == 3
        assert {node["type"] for node in response.json()["nodes"]} >= {
            "user_prompt",
            "run",
            "model_input",
        }

        foreign = client.get(
            f"/v1/tasks/{session_id}/activity",
            headers={"X-Tenant-ID": "tenant-2"},
        )
        assert foreign.status_code == 404
