import assert from "node:assert/strict";
import test from "node:test";
import {
  appendUniqueEvent,
  buildRestoredTranscript,
  createCommandId,
  createSseParser,
  extractApprovalRequest,
  filterTimeline,
  findPendingApproval,
  loadChatSessionIndex,
  metricSeries,
  normalizeBaseUrl,
  redact,
  removeChatSessionIndex,
  resultText,
  retryAfterMs,
  runtimeDelta,
  safeCurl,
  truncateTitle,
  upsertChatSessionIndex,
} from "../app/lib/protocol.mjs";

test("normalizes API base URLs and creates operation-scoped command ids", () => {
  assert.equal(normalizeBaseUrl(" https://api.example.test/// "), "https://api.example.test");
  const first = createCommandId("create-task");
  const second = createCommandId("create-task");
  assert.match(first, /^create-task-/);
  assert.notEqual(first, second);
});

test("parses fragmented SSE frames and preserves multiline data", () => {
  const events = [];
  const parser = createSseParser((event) => events.push(event));
  parser.push("id: session:1\nevent: tool.pro");
  parser.push("gress\ndata: {\"step\":1}\n\nid: session:2\ndata: line one\n");
  parser.push("data: line two\n\n");
  assert.deepEqual(events, [
    { id: "session:1", event: "tool.progress", data: '{"step":1}' },
    { id: "session:2", event: "message", data: "line one\nline two" },
  ]);
  assert.equal(parser.remaining(), "");
});

test("deduplicates replayed events and extracts streaming deltas", () => {
  const first = { id: "session:1", event: "model.output.delta", data: { payload: { delta: "你好" } } };
  const entries = appendUniqueEvent([], first);
  assert.equal(appendUniqueEvent(entries, first), entries);
  assert.deepEqual(appendUniqueEvent(entries, { ...first, id: "session:2" }).map((entry) => entry.id), ["session:1", "session:2"]);
  assert.equal(runtimeDelta(first.event, first.data), "你好");
  assert.equal(runtimeDelta("tool.progress", first.data), "");
});

test("normalizes result text and Retry-After delays", () => {
  assert.equal(resultText({ result_summary: "最终答案" }), "最终答案");
  assert.equal(resultText({ output: "fallback" }), "fallback");
  assert.equal(retryAfterMs("3"), 3000);
  assert.equal(retryAfterMs(null), 2000);
  assert.equal(retryAfterMs("0"), 250);
});

test("redacts sensitive values from copied curl commands", () => {
  const body = { goal: "inspect", password: "never-copy", nested: { api_key: "also-secret", safe: 1 } };
  assert.deepEqual(redact(body), { goal: "inspect", password: "[REDACTED]", nested: { api_key: "[REDACTED]", safe: 1 } });
  const curl = safeCurl({ method: "POST", url: "https://api.test/v1/tasks", headers: { Authorization: "Bearer secret", "X-Tenant-ID": "demo" }, body });
  assert.doesNotMatch(curl, /Bearer secret|never-copy|also-secret/);
  assert.match(curl, /X-Tenant-ID: demo/);
  assert.match(curl, /\[REDACTED\]/);
});

test("filters timeline records and builds chronologically ordered metric series", () => {
  const timeline = [
    { kind: "trace_span", component: "runtime", status: "error" },
    { kind: "canonical_event", type: "run.completed", run_id: "run-1" },
  ];
  assert.deepEqual(filterTimeline(timeline, "runtime", "all"), [timeline[0]]);
  assert.deepEqual(filterTimeline(timeline, "run-1", "canonical_event"), [timeline[1]]);
  const points = [
    { name: "projection.lag.seconds", value: 3, observed_at: "2026-07-20T02:00:00Z" },
    { name: "http.request.duration_ms", value: 12, observed_at: "2026-07-20T01:00:00Z" },
    { name: "projection.lag.seconds", value: 1, observed_at: "2026-07-20T01:00:00Z" },
  ];
  const series = metricSeries(points);
  assert.equal(series[1].name, "projection.lag.seconds");
  assert.deepEqual(series[1].values.map((point) => point.value), [1, 3]);
  assert.equal(series[1].latest.value, 3);
});

test("extracts approval requests from runtime and nested payloads", () => {
  assert.equal(extractApprovalRequest("model.output.delta", { payload: { delta: "hi" } }), null);
  assert.deepEqual(
    extractApprovalRequest("approval.requested", {
      payload: {
        approval_id: "apr-1",
        tool_name: "controlled-write",
        reason: "needs review",
        risk: "high",
        redacted_arguments: { target: "release" },
        expected_effect: "write",
        status: "waiting",
      },
    }),
    {
      approvalId: "apr-1",
      toolName: "controlled-write",
      reason: "needs review",
      risk: "high",
      redactedArguments: { target: "release" },
      expectedEffect: "write",
      status: "waiting",
    },
  );
  assert.equal(
    extractApprovalRequest("tool.call.denied", {
      metadata: { approval_request: { approval_id: "apr-2", tool_name: "delete", reason: "destructive" } },
      error_code: "approval_required",
    })?.approvalId,
    "apr-2",
  );
});

test("finds the latest unresolved approval from timeline entries", () => {
  const pending = findPendingApproval([
    { type: "approval.requested", timestamp: "2026-07-21T01:00:00Z", detail: { approval_id: "apr-old", tool_name: "a" } },
    { type: "approval.approved", timestamp: "2026-07-21T01:01:00Z", detail: { approval_id: "apr-old" } },
    { type: "approval.requested", timestamp: "2026-07-21T01:02:00Z", detail: { approval_id: "apr-new", tool_name: "b", reason: "confirm" } },
  ]);
  assert.equal(pending?.approvalId, "apr-new");
  assert.equal(pending?.toolName, "b");
  assert.equal(
    findPendingApproval([
      { type: "approval.requested", timestamp: "2026-07-21T01:00:00Z", detail: { approval_id: "apr-done" } },
      { type: "approval.rejected", timestamp: "2026-07-21T01:01:00Z", detail: { approval_id: "apr-done" } },
    ]),
    null,
  );
});

test("maintains a tenant-scoped chat session index without message bodies", () => {
  const memory = new Map();
  const storage = {
    getItem: (key) => memory.get(key) ?? null,
    setItem: (key, value) => memory.set(key, value),
  };
  assert.equal(truncateTitle("  hello   world  ".repeat(10)).endsWith("…"), true);
  const first = upsertChatSessionIndex(storage, "tenant-a", {
    sessionId: "ses_1",
    title: "first question",
    status: "ready",
    runStatus: "completed",
  });
  assert.equal(first[0].sessionId, "ses_1");
  const second = upsertChatSessionIndex(storage, "tenant-a", {
    sessionId: "ses_2",
    goal: "second question",
    status: "running",
    runStatus: "running",
  });
  assert.deepEqual(second.map((item) => item.sessionId), ["ses_2", "ses_1"]);
  assert.equal(loadChatSessionIndex(storage, "tenant-b").length, 0);
  const removed = removeChatSessionIndex(storage, "tenant-a", "ses_1");
  assert.deepEqual(removed.map((item) => item.sessionId), ["ses_2"]);
  const restored = buildRestoredTranscript({
    goal: "介绍架构",
    resultSummary: "边界清晰",
    sessionId: "ses_2",
  });
  assert.deepEqual(restored.map((item) => item.role), ["user", "assistant", "system"]);
  assert.match(JSON.stringify(memory.get("auraclaw-chat-sessions-v1")), /ses_2/);
  assert.doesNotMatch(JSON.stringify(memory.get("auraclaw-chat-sessions-v1")), /介绍架构|边界清晰/);
});
