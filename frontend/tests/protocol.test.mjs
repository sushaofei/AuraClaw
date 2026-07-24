import assert from "node:assert/strict";
import test from "node:test";
import {
  appendUniqueEvent,
  approvalFromTranscript,
  buildRestoredTranscript,
  createCommandId,
  createSseParser,
  extractApprovalRequest,
  filterTimeline,
  findPendingApproval,
  applyChatDelta,
  finalizeChatRuns,
  loadChatSessionIndex,
  metricSeries,
  normalizeBaseUrl,
  redact,
  removeChatSessionIndex,
  resultText,
  retryAfterMs,
  runtimeDelta,
  runtimeEventRunId,
  reconcileAssistantWithResult,
  safeCurl,
  transcriptFromApiMessages,
  transcriptFromTimeline,
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

test("merges streaming deltas by run_id and drops late tails after finalize", () => {
  assert.equal(runtimeEventRunId({ run_id: "run-a", payload: { delta: "x" } }), "run-a");
  assert.equal(runtimeEventRunId({ correlation: { run_id: "run-b" } }), "run-b");

  let messages = applyChatDelta([], {
    delta: "你好",
    runId: "run-1",
    createId: () => "a1",
  });
  messages = applyChatDelta(messages, { delta: "，世界", runId: "run-1" });
  assert.deepEqual(messages.map((item) => [item.role, item.content, item.runId, item.streaming]), [
    ["assistant", "你好，世界", "run-1", true],
  ]);

  messages = [
    ...finalizeChatRuns(messages, ["run-1"]),
    { id: "u1", role: "user", content: "好的" },
  ];
  const finalized = new Set(["run-1"]);
  messages = applyChatDelta(messages, {
    delta: "迟到尾巴",
    runId: "run-1",
    createId: () => "a-late",
    finalizedRunIds: finalized,
  });
  assert.deepEqual(
    messages.map((item) => [item.role, item.content]),
    [["assistant", "你好，世界"], ["user", "好的"]],
  );

  messages = applyChatDelta(messages, {
    delta: "第二轮",
    runId: "run-2",
    createId: () => "a2",
    finalizedRunIds: finalized,
  });
  assert.deepEqual(
    messages.map((item) => [item.role, item.content, item.runId]),
    [
      ["assistant", "你好，世界", "run-1"],
      ["user", "好的", undefined],
      ["assistant", "第二轮", "run-2"],
    ],
  );
});

test("reconciles truncated streaming bubbles with authoritative Result text", () => {
  const truncated = [
    { id: "u1", role: "user", content: "你好呀" },
    {
      id: "a1",
      role: "assistant",
      content: "你好呀！很高兴见到你。无论是",
      streaming: true,
      runId: "run-1",
    },
  ];
  const reconciled = reconcileAssistantWithResult(truncated, {
    runId: "run-1",
    resultSummary: "你好呀！很高兴见到你。无论是需要信息还是日常交流，我都很乐意与你对话。",
  });
  assert.deepEqual(
    reconciled.map((item) => [item.role, item.content, item.streaming, item.runId]),
    [
      ["user", "你好呀", undefined, undefined],
      [
        "assistant",
        "你好呀！很高兴见到你。无论是需要信息还是日常交流，我都很乐意与你对话。",
        false,
        "run-1",
      ],
    ],
  );

  const created = reconcileAssistantWithResult([{ id: "u1", role: "user", content: "hi" }], {
    runId: "run-2",
    resultSummary: "完整答案",
    createId: () => "a-new",
  });
  assert.deepEqual(
    created.map((item) => [item.id, item.role, item.content, item.streaming]),
    [
      ["u1", "user", "hi", undefined],
      ["a-new", "assistant", "完整答案", false],
    ],
  );
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

test("restores multi-turn chat transcript from timeline canonical events", () => {
  const timeline = [
    {
      kind: "trace_span",
      timestamp: "2026-07-21T10:00:00Z",
      type: "runtime.step",
      detail: { note: "ignored" },
    },
    {
      kind: "canonical_event",
      timestamp: "2026-07-21T10:00:01Z",
      type: "session.created",
      detail: { goal: "第一问" },
    },
    {
      kind: "canonical_event",
      timestamp: "2026-07-21T10:00:02Z",
      type: "model.output.completed",
      correlation: { run_id: "run-1" },
      detail: { output: "第一答" },
    },
    {
      kind: "canonical_event",
      timestamp: "2026-07-21T10:00:03Z",
      type: "user.message.appended",
      detail: { message: "第二问" },
    },
    {
      kind: "canonical_event",
      timestamp: "2026-07-21T10:00:04Z",
      type: "model.output.completed",
      correlation: { run_id: "run-2" },
      detail: { output: "第二答" },
    },
  ];
  assert.deepEqual(transcriptFromTimeline(timeline), [
    { role: "user", content: "第一问" },
    { role: "assistant", content: "第一答", runId: "run-1" },
    { role: "user", content: "第二问" },
    { role: "assistant", content: "第二答", runId: "run-2" },
  ]);
  const restored = buildRestoredTranscript({
    goal: "不应使用",
    resultSummary: "也不应使用",
    sessionId: "ses_multi",
    timelineEntries: timeline,
  });
  assert.deepEqual(
    restored.filter((item) => item.role !== "system").map((item) => item.content),
    ["第一问", "第一答", "第二问", "第二答"],
  );
  assert.match(restored.at(-1).content, /Canonical Event/);
  const fallback = buildRestoredTranscript({
    goal: "回退问题",
    resultSummary: "回退答案",
    sessionId: "ses_fallback",
    timelineEntries: [],
  });
  assert.deepEqual(
    fallback.filter((item) => item.role !== "system").map((item) => [item.role, item.content]),
    [["user", "回退问题"], ["assistant", "回退答案"]],
  );
});

test("prefers transcript API messages over timeline and maps pending approval", () => {
  assert.deepEqual(
    transcriptFromApiMessages([
      { role: "user", content: "问" },
      { role: "assistant", content: "答", run_id: "run-9" },
      { role: "system", content: "忽略" },
    ]),
    [
      { role: "user", content: "问" },
      { role: "assistant", content: "答", runId: "run-9" },
    ],
  );
  const restored = buildRestoredTranscript({
    goal: "不应使用",
    resultSummary: "也不应使用",
    sessionId: "ses_api",
    transcriptMessages: [
      { role: "user", content: "API 问" },
      { role: "assistant", content: "API 答", run_id: "run-api" },
    ],
    timelineEntries: [
      {
        kind: "canonical_event",
        timestamp: "2026-07-21T10:00:01Z",
        type: "session.created",
        detail: { goal: "Timeline 问" },
      },
    ],
  });
  assert.deepEqual(
    restored.filter((item) => item.role !== "system").map((item) => item.content),
    ["API 问", "API 答"],
  );
  assert.deepEqual(
    approvalFromTranscript({
      approval_id: "apr_1",
      tool_name: "shell",
      reason: "needs review",
      status: "waiting",
    }),
    {
      approvalId: "apr_1",
      toolName: "shell",
      reason: "needs review",
      risk: "",
      redactedArguments: null,
      expectedEffect: "",
      status: "waiting",
    },
  );
});

test("falls back to timeline when transcript API returns no messages", () => {
  const restored = buildRestoredTranscript({
    goal: "回退 Goal",
    resultSummary: "回退 Result",
    sessionId: "ses_fallback_timeline",
    transcriptMessages: [],
    timelineEntries: [
      {
        kind: "canonical_event",
        timestamp: "2026-07-21T10:00:01Z",
        type: "session.created",
        detail: { goal: "第一问" },
      },
      {
        kind: "canonical_event",
        timestamp: "2026-07-21T10:00:02Z",
        type: "model.output.completed",
        correlation: { run_id: "run-1" },
        detail: { output: "第一答" },
      },
      {
        kind: "canonical_event",
        timestamp: "2026-07-21T10:00:03Z",
        type: "user.message.appended",
        detail: { message: "第二问" },
      },
      {
        kind: "canonical_event",
        timestamp: "2026-07-21T10:00:04Z",
        type: "model.output.completed",
        correlation: { run_id: "run-2" },
        detail: { output: "第二答" },
      },
    ],
  });
  assert.deepEqual(
    restored.filter((item) => item.role !== "system").map((item) => item.content),
    ["第一问", "第一答", "第二问", "第二答"],
  );
});
