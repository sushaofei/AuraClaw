import assert from "node:assert/strict";
import test from "node:test";

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders the AuraClaw console shell", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await response.text();
  assert.match(html, /<title>AuraClaw Protocol Test Console<\/title>/i);
  assert.match(html, /AuraClaw/);
  assert.match(html, /智能问答/);
  assert.match(html, /创建任务/);
  assert.match(html, /历史会话/);
  assert.match(html, /Human-in-the-loop|STREAM \/ RESULT \/ HITL/);
  assert.match(html, /STREAM \/ RESULT/);
  assert.match(html, /Session 详情/);
  assert.match(html, /实时事件/);
  assert.match(html, />Timeline</);
  assert.match(html, /Metrics/);
  assert.doesNotMatch(html, /codex-preview|react-loading-skeleton|Your site is taking shape/i);
});
