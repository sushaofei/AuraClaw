import assert from "node:assert/strict";
import test from "node:test";
import {
  appendUniqueEvent,
  createCommandId,
  createSseParser,
  filterTimeline,
  metricSeries,
  normalizeBaseUrl,
  redact,
  resultText,
  retryAfterMs,
  runtimeDelta,
  safeCurl,
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
