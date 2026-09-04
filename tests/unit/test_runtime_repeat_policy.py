from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import ToolCall
from auraclaw.runtime.repeat_policy import (
    no_progress,
    obvious_shape_error,
    repeat_decision,
    repeat_target,
)


def fixture(permission="read-only"):
    assignment = RuntimeAssignment(tenant_id="t", root_session_id="s", session_id="s",
                                   run_id="r", runtime_id="rt", lease_id="l", fencing_token=1,
                                   role="root", resource_profile={}, user_id="1", dept_id="100")
    state = {"loaded": {"cap": {"kind": "tool", "permission": permission, "server_id": "mcp",
                              "config_revision": 1, "model_tool": {"function": {
                                  "name": "inventory", "parameters": {
                                      "type": "object", "required": ["input"], "properties": {
                                          "input": {"type": "object", "properties": {
                                              "limit": {"type": "integer"}}}}}}}}}}
    call = ToolCall("next", "inventory", {"input": {"limit": 3}})
    return assignment, state, call


def history(target, results):
    events = []
    for i, result in enumerate(results):
        base = {"run_id": "r", "occurred_at": datetime.now(UTC)}
        events.append(SimpleNamespace(**base, type="tool.call.requested", payload={
            "tool_invocation_id": str(i), "repeat_identity": {
                "binding": target.binding, "arguments": target.arguments}}))
        events.append(SimpleNamespace(**base, type="tool.call.completed", payload={
            "tool_invocation_id": str(i), "result": result}))
    return events


def test_read_suppression_preserves_binding_identity_and_pagination():
    a, state, call = fixture()
    target = repeat_target(a, call, state)
    events = history(target, [{"status": "success"}])
    result = repeat_decision(a, call, target, events)
    assert result["metadata"]["reason"] == "existing_read_result"
    assert result["status"] == "denied" and "output" not in result
    page = replace(call, arguments={"input": {"limit": 4}})
    assert repeat_decision(a, page, repeat_target(a, page, state), events) is None
    other = repeat_target(replace(a, user_id="2"), call, state)
    assert repeat_decision(a, call, other, events) is None
    state["loaded"]["cap"]["config_revision"] = 2
    assert repeat_decision(a, call, repeat_target(a, call, state), events) is None


def test_writes_never_reuse_success_and_unknown_never_replays():
    a, state, call = fixture("write")
    target = repeat_target(a, call, state)
    assert repeat_decision(a, call, target, history(target, [{"status": "success"}])) is None
    result = repeat_decision(a, call, target, history(target, [
        {"status": "unknown", "side_effect_status": "unknown", "retryable": True}]))
    assert result["metadata"]["reason"] == "prior_execution_unknown"


def test_schema_correction_is_bounded_but_valid_correction_is_allowed():
    a, state, call = fixture()
    invalid = replace(call, arguments={"limit": "3"})
    bad = repeat_target(a, invalid, state)
    events = history(bad, [{"status": "error", "error_code": "tool_schema_invalid",
                            "side_effect_status": "not_started"}] * 3)
    assert repeat_decision(a, invalid, bad, events)["metadata"]["reason"] == (
        "schema_correction_exhausted")
    changed = replace(invalid, arguments={"limit": "5"})
    assert repeat_decision(a, changed, repeat_target(a, changed, state), events) is not None
    assert repeat_decision(a, call, repeat_target(a, call, state), events) is None
    assert not obvious_shape_error({"$ref": "https://example.invalid/schema"}, {})


def test_nonretryable_error_and_alternating_suppressions_stop():
    a, state, call = fixture()
    target = repeat_target(a, call, state)
    result = repeat_decision(a, call, target, history(target, [
        {"status": "error", "retryable": False, "error_code": "remote_schema_error"}]))
    assert result["metadata"]["reason"] == "non_retryable_failure"
    blocked = history(target, [{"status": "denied", "error_code": "tool_repeat_suppressed"}] * 4)
    assert no_progress(blocked, "r")
    assert not no_progress(blocked, "other-run")
