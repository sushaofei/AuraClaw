"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  createCommandId,
  createSseParser,
  filterTimeline,
  metricSeries,
  normalizeBaseUrl,
  safeCurl,
} from "./lib/protocol.mjs";

type Json = Record<string, unknown>;
type RequestEntry = {
  id: string;
  method: string;
  path: string;
  status: number;
  duration: number;
  traceparent: string;
  at: string;
  curl: string;
};
type StreamEntry = { id: string; event: string; data: Json; receivedAt: string; replay: boolean };

const STORAGE_KEY = "auraclaw-console-v1";
const CRITICAL_METRICS = new Set([
  "projection.lag.seconds",
  "runtime.lease_lost.count",
  "tool.side_effect_unknown.count",
  "delivery.dlq.count",
]);

function asJson(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Json) : {};
}

function displayTime(value?: unknown) {
  if (!value) return "—";
  const date = new Date(String(value));
  return Number.isNaN(date.valueOf()) ? String(value) : date.toLocaleTimeString("zh-CN", { hour12: false });
}

function statusTone(status?: unknown) {
  const value = String(status || "unknown").toLowerCase();
  if (["completed", "ready", "ok", "succeeded"].includes(value)) return "good";
  if (["failed", "error", "cancelled", "critical"].includes(value)) return "bad";
  return "warn";
}

function JsonBlock({ value }: { value: unknown }) {
  return <pre className="json-block">{JSON.stringify(value, null, 2)}</pre>;
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="empty-state">
      <span className="empty-mark">···</span>
      <strong>{title}</strong>
      <p>{detail}</p>
    </div>
  );
}

export function AuraClawConsole() {
  const [baseUrl, setBaseUrl] = useState("http://127.0.0.1:8000");
  const [tenant, setTenant] = useState("local");
  const [actor, setActor] = useState("local-user");
  const [correlation, setCorrelation] = useState("");
  const [sessionId, setSessionId] = useState("");
  const [goal, setGoal] = useState("验证 AuraClaw managed agent 任务流程");
  const [task, setTask] = useState<Json | null>(null);
  const [children, setChildren] = useState<Json[]>([]);
  const [result, setResult] = useState<Json | null>(null);
  const [health, setHealth] = useState<{ live: string; ready: string; checkedAt: string }>({ live: "unknown", ready: "unknown", checkedAt: "" });
  const [history, setHistory] = useState<RequestEntry[]>([]);
  const [events, setEvents] = useState<StreamEntry[]>([]);
  const [streamState, setStreamState] = useState("idle");
  const [streamCursor, setStreamCursor] = useState("");
  const [eventFilter, setEventFilter] = useState("");
  const [timeline, setTimeline] = useState<Json[]>([]);
  const [timelineKind, setTimelineKind] = useState("all");
  const [timelineQuery, setTimelineQuery] = useState("");
  const [metrics, setMetrics] = useState<Json[]>([]);
  const [metricQuery, setMetricQuery] = useState("");
  const [activePanel, setActivePanel] = useState("task");
  const [busy, setBusy] = useState("");
  const [notice, setNotice] = useState("等待连接 AuraClaw API");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [message, setMessage] = useState("");
  const [approvalId, setApprovalId] = useState("");
  const [approvalFeedback, setApprovalFeedback] = useState("");
  const [expandedTimeline, setExpandedTimeline] = useState<number | null>(null);
  const taskEtag = useRef("");
  const streamAbort = useRef<AbortController | null>(null);
  const lastEventId = useRef("");

  useEffect(() => {
    let active = true;
    try {
      const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
      queueMicrotask(() => {
        if (!active) return;
        if (saved.baseUrl) setBaseUrl(saved.baseUrl);
        if (saved.tenant) setTenant(saved.tenant);
        if (saved.actor) setActor(saved.actor);
        if (saved.sessionId) setSessionId(saved.sessionId);
      });
    } catch {
      // Ignore malformed device-local preferences.
    }
    return () => { active = false; };
  }, []);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ baseUrl, tenant, actor, sessionId }));
  }, [baseUrl, tenant, actor, sessionId]);

  const identityHeaders = useCallback(() => {
    const headers: Record<string, string> = { "X-Tenant-ID": tenant, "X-Actor-ID": actor };
    if (correlation.trim()) headers["X-Correlation-ID"] = correlation.trim();
    return headers;
  }, [actor, correlation, tenant]);

  const api = useCallback(
    async (path: string, options: { method?: string; body?: unknown; headers?: Record<string, string>; quiet?: boolean } = {}) => {
      const method = options.method ?? "GET";
      const url = `${normalizeBaseUrl(baseUrl)}${path}`;
      const headers = { ...identityHeaders(), ...(options.body === undefined ? {} : { "Content-Type": "application/json" }), ...options.headers };
      const started = performance.now();
      let response: Response;
      try {
        response = await fetch(url, { method, headers, body: options.body === undefined ? undefined : JSON.stringify(options.body), cache: "no-store" });
      } catch (error) {
        const detail = error instanceof Error ? error.message : "network error";
        if (!options.quiet) setNotice(`连接失败：${detail}`);
        throw error;
      }
      const duration = Math.round(performance.now() - started);
      const traceparent = response.headers.get("traceparent") ?? "";
      setHistory((current) => [
        {
          id: createCommandId("request"), method, path, status: response.status, duration, traceparent,
          at: new Date().toISOString(), curl: safeCurl({ method, url, headers, body: options.body }),
        },
        ...current,
      ].slice(0, 80));
      if (response.status === 304) return { data: null, response };
      const text = await response.text();
      let data: unknown = null;
      if (text) {
        try { data = JSON.parse(text); } catch { data = { message: text }; }
      }
      if (!response.ok && response.status !== 202) {
        const payload = asJson(data);
        const error = new Error(String(payload.message ?? payload.detail ?? `HTTP ${response.status}`));
        if (!options.quiet) setNotice(`请求失败 ${response.status}：${error.message}`);
        throw error;
      }
      return { data, response };
    },
    [baseUrl, identityHeaders],
  );

  const checkHealth = useCallback(async () => {
    setBusy("health");
    const [live, ready] = await Promise.allSettled([api("/health/live", { quiet: true }), api("/health/ready", { quiet: true })]);
    const read = (result: PromiseSettledResult<{ data: unknown }>) => result.status === "fulfilled" ? String(asJson(result.value.data).status ?? "ok") : "offline";
    setHealth({ live: read(live), ready: read(ready), checkedAt: new Date().toISOString() });
    setNotice(live.status === "fulfilled" && ready.status === "fulfilled" ? "API 连接正常" : "健康检查未全部通过");
    setBusy("");
  }, [api]);

  const loadTask = useCallback(async (quiet = false) => {
    if (!sessionId.trim()) return;
    if (!quiet) setBusy("task");
    try {
      const headers = taskEtag.current ? { "If-None-Match": taskEtag.current } : undefined;
      const { data, response } = await api(`/v1/tasks/${encodeURIComponent(sessionId.trim())}`, { headers, quiet });
      if (response.status !== 304 && data) setTask(asJson(data));
      taskEtag.current = response.headers.get("etag") ?? taskEtag.current;
      if (!quiet) setNotice(response.status === 304 ? "任务视图没有变化" : "任务视图已刷新");
    } finally {
      if (!quiet) setBusy("");
    }
  }, [api, sessionId]);

  useEffect(() => {
    if (!autoRefresh || !sessionId) return;
    const timer = window.setInterval(() => void loadTask(true), 2500);
    return () => window.clearInterval(timer);
  }, [autoRefresh, loadTask, sessionId]);

  const createTask = async () => {
    if (!goal.trim()) return;
    setBusy("create");
    try {
      const { data } = await api("/v1/tasks", { method: "POST", body: { goal: goal.trim() }, headers: { "Idempotency-Key": createCommandId("create-task") } });
      const created = asJson(data);
      const id = String(created.session_id ?? "");
      setSessionId(id); setTask(created); setEvents([]); setTimeline([]); setResult(null); setStreamCursor(""); taskEtag.current = ""; lastEventId.current = "";
      const { data: view, response: viewResponse } = await api(`/v1/tasks/${encodeURIComponent(id)}`, { quiet: true });
      if (view) setTask(asJson(view));
      taskEtag.current = viewResponse.headers.get("etag") ?? "";
      setNotice(`任务已接纳：${id}`);
    } finally { setBusy(""); }
  };

  const command = async (operation: string, path: string, body?: unknown, confirmText?: string) => {
    if (!sessionId || !task) return;
    if (confirmText && !window.confirm(`${confirmText}\n\nTenant: ${tenant}\nSession: ${sessionId}`)) return;
    setBusy(operation);
    try {
      await api(path, {
        method: "POST", body,
        headers: { "Idempotency-Key": createCommandId(operation), "X-Expected-Version": String(task.projection_version ?? 0) },
      });
      taskEtag.current = "";
      await loadTask(true);
      setNotice(`${operation} 已提交`);
    } finally { setBusy(""); }
  };

  const loadResult = async () => {
    if (!sessionId) return;
    setBusy("result");
    try {
      const { data, response } = await api(`/v1/tasks/${encodeURIComponent(sessionId)}/result`);
      if (data) setResult(asJson(data));
      setNotice(response.status === 202 ? `结果尚未就绪，将在 ${response.headers.get("retry-after") ?? "稍后"} 秒后重试` : "结果已加载");
    } finally { setBusy(""); }
  };

  const loadChildren = async () => {
    if (!sessionId) return;
    setBusy("children");
    try {
      const { data } = await api(`/v1/tasks/${encodeURIComponent(sessionId)}/children`);
      setChildren(Array.isArray(asJson(data).children) ? (asJson(data).children as Json[]) : []);
      setNotice("Child Session 已加载");
    } finally { setBusy(""); }
  };

  const loadTimeline = async () => {
    if (!sessionId) return;
    setBusy("timeline");
    try {
      const { data } = await api(`/v1/operations/sessions/${encodeURIComponent(sessionId)}/timeline`);
      setTimeline(Array.isArray(asJson(data).entries) ? (asJson(data).entries as Json[]) : []);
      setActivePanel("timeline"); setNotice("Session Timeline 已加载");
    } finally { setBusy(""); }
  };

  const loadMetrics = useCallback(async (quiet = false) => {
    if (!quiet) setBusy("metrics");
    try {
      const { data } = await api("/v1/operations/metrics", { quiet });
      setMetrics(Array.isArray(asJson(data).metrics) ? (asJson(data).metrics as Json[]) : []);
      if (!quiet) { setActivePanel("metrics"); setNotice("Metrics 已刷新"); }
    } finally { if (!quiet) setBusy(""); }
  }, [api]);

  const stopStream = useCallback(() => {
    streamAbort.current?.abort(); streamAbort.current = null; setStreamState("stopped");
  }, []);

  useEffect(() => () => streamAbort.current?.abort(), []);

  const startStream = async () => {
    if (!sessionId || streamAbort.current) return;
    const controller = new AbortController();
    streamAbort.current = controller; setStreamState(lastEventId.current ? "reconnecting" : "connecting"); setActivePanel("stream");
    let attempt = 0;
    while (!controller.signal.aborted) {
      try {
        const headers = identityHeaders();
        if (lastEventId.current) headers["Last-Event-ID"] = lastEventId.current;
        const response = await fetch(`${normalizeBaseUrl(baseUrl)}/v1/streams/${encodeURIComponent(sessionId)}`, { headers, signal: controller.signal, cache: "no-store" });
        if (!response.ok || !response.body) throw new Error(`HTTP ${response.status}`);
        attempt = 0; setStreamState("live"); setNotice("Runtime Event 流已连接");
        const decoder = new TextDecoder();
        const parser = createSseParser((frame) => {
          let data: Json;
          try { data = asJson(JSON.parse(frame.data)); } catch { data = { raw: frame.data }; }
          const replay = Boolean(frame.id && lastEventId.current);
          if (frame.id) { lastEventId.current = frame.id; setStreamCursor(frame.id); }
          if (frame.event === "stream.reset") setNotice("事件游标已过期，请以 Task View 为准并重新同步");
          setEvents((current) => [...current, { id: frame.id || createCommandId("event"), event: frame.event, data, receivedAt: new Date().toISOString(), replay }].slice(-500));
        });
        const reader = response.body.getReader();
        while (!controller.signal.aborted) {
          const { value, done } = await reader.read();
          if (done) break;
          parser.push(decoder.decode(value, { stream: true }));
        }
      } catch {
        if (controller.signal.aborted) break;
        attempt += 1; setStreamState("reconnecting");
        setNotice(`实时流中断，正在第 ${attempt} 次重连`);
        await new Promise((resolve) => window.setTimeout(resolve, Math.min(1000 * 2 ** (attempt - 1), 10000)));
      }
    }
    streamAbort.current = null;
  };

  const visibleTimeline = useMemo(() => filterTimeline(timeline, timelineQuery, timelineKind), [timeline, timelineKind, timelineQuery]);
  const visibleEvents = useMemo(() => events.filter((event) => !eventFilter || event.event.toLowerCase().includes(eventFilter.toLowerCase())), [eventFilter, events]);
  const visibleMetrics = useMemo(() => metricSeries(metrics as Array<{ name: string; observed_at: string; value: number }>).filter((series) => !metricQuery || series.name.toLowerCase().includes(metricQuery.toLowerCase())), [metricQuery, metrics]);
  const progress = Number(task?.progress ?? 0);
  const taskStatus = String(task?.status ?? "未加载");

  return (
    <main className="console-shell">
      <header className="topbar">
        <div className="brand"><span className="brand-mark">AC</span><div><strong>AuraClaw</strong><small>operations console / M7</small></div></div>
        <div className="top-status"><span className={`signal ${health.live === "ok" ? "online" : ""}`} /><span>API {health.live === "ok" ? "online" : "unverified"}</span><span className="divider" /><code>{tenant}</code></div>
      </header>

      <section className="connection-bar" aria-label="连接配置">
        <label className="field wide"><span>API endpoint</span><input value={baseUrl} onChange={(e) => setBaseUrl(e.target.value)} /></label>
        <label className="field"><span>Tenant</span><input value={tenant} onChange={(e) => setTenant(e.target.value)} /></label>
        <label className="field"><span>Actor</span><input value={actor} onChange={(e) => setActor(e.target.value)} /></label>
        <label className="field"><span>Correlation ID</span><input value={correlation} onChange={(e) => setCorrelation(e.target.value)} placeholder="optional" /></label>
        <button className="button primary" onClick={() => void checkHealth()} disabled={busy === "health"}>{busy === "health" ? "检查中" : "检查连接"}</button>
      </section>

      <section className="health-strip">
        <div><span className={`health-dot ${statusTone(health.live)}`} /><small>Liveness</small><strong>{health.live}</strong></div>
        <div><span className={`health-dot ${statusTone(health.ready)}`} /><small>Readiness</small><strong>{health.ready}</strong></div>
        <div className="notice"><small>Console activity</small><strong>{notice}</strong></div>
        <div><small>Last check</small><strong>{displayTime(health.checkedAt)}</strong></div>
      </section>

      <div className="workspace-grid">
        <aside className="sidebar">
          <p className="eyebrow">Workspace</p>
          {[
            ["task", "01", "任务测试台"], ["stream", "02", "实时事件"], ["timeline", "03", "Timeline"],
            ["metrics", "04", "Metrics"], ["history", "05", "请求历史"],
          ].map(([id, number, label]) => <button key={id} className={activePanel === id ? "nav-item active" : "nav-item"} onClick={() => setActivePanel(id)}><span>{number}</span>{label}</button>)}
          <div className="sidebar-note"><span className="health-dot good" /><div><strong>Canonical first</strong><p>最终状态以 Task / Result API 为准</p></div></div>
        </aside>

        <section className="panel-stage">
          {activePanel === "task" && (
            <div className="panel-stack">
              <div className="panel-heading"><div><p className="eyebrow">Session control</p><h1>任务测试台</h1><p>创建、定位并控制一个 managed agent session。</p></div><div className="panel-actions"><label className="switch"><input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} /><span />自动刷新</label><button className="button" onClick={() => void loadTask()} disabled={!sessionId}>刷新</button></div></div>
              <div className="create-row"><label className="field grow"><span>Goal</span><textarea value={goal} onChange={(e) => setGoal(e.target.value)} rows={2} /></label><button className="button primary tall" onClick={() => void createTask()} disabled={busy === "create" || !goal.trim()}>创建任务</button></div>
              <div className="session-locator"><label className="field grow"><span>Session ID</span><input value={sessionId} onChange={(e) => { setSessionId(e.target.value); taskEtag.current = ""; }} placeholder="session_..." /></label><button className="button" onClick={() => void loadTask()} disabled={!sessionId}>打开 Session</button></div>
              {task ? (
                <>
                  <div className="stat-grid">
                    <article className="stat-card status-card"><small>Status</small><strong className={`status-text ${statusTone(taskStatus)}`}>{taskStatus}</strong><div className="progress-track"><span style={{ width: `${Math.max(0, Math.min(100, progress * 100))}%` }} /></div><p>{Math.round(progress * 100)}% · {String(task.current_stage ?? "—")}</p></article>
                    <article className="stat-card"><small>Projection</small><strong>v{String(task.projection_version ?? "—")}</strong><p>{displayTime(task.projected_at)}</p></article>
                    <article className="stat-card"><small>Run</small><strong className="mono compact">{String(task.run_id ?? "—")}</strong><p>root {String(task.root_session_id ?? "—").slice(0, 12)}</p></article>
                    <article className="stat-card"><small>Delivery</small><strong>{String(task.delivery_status ?? "not configured")}</strong><p>{String(task.delivery_attempt_count ?? 0)} attempts</p></article>
                  </div>
                  <div className="command-grid">
                    <article className="subpanel"><div className="subpanel-title"><h2>Session commands</h2><span>expected v{String(task.projection_version ?? 0)}</span></div><label className="field"><span>追加消息</span><textarea value={message} onChange={(e) => setMessage(e.target.value)} placeholder="为当前 Session 补充上下文" rows={3} /></label><div className="button-row"><button className="button primary" disabled={!message.trim()} onClick={() => void command("append-message", `/v1/sessions/${sessionId}/messages`, { message }).then(() => setMessage(""))}>追加消息</button><button className="button" onClick={() => void command("request-run", `/v1/sessions/${sessionId}/runs`)}>请求运行</button><button className="button danger" onClick={() => void command("cancel", `/v1/sessions/${sessionId}/cancel`, { reason: "cancelled from operations console" }, "确认取消任务？")}>取消</button><button className="button" onClick={() => void command("resume", `/v1/sessions/${sessionId}/resume`, undefined, "确认恢复任务？")}>恢复</button></div></article>
                    <article className="subpanel"><div className="subpanel-title"><h2>Approval response</h2><span>known ID only</span></div><label className="field"><span>Approval ID</span><input value={approvalId} onChange={(e) => setApprovalId(e.target.value)} placeholder="approval_..." /></label><label className="field"><span>反馈（可选）</span><input value={approvalFeedback} onChange={(e) => setApprovalFeedback(e.target.value)} /></label><div className="button-row"><button className="button approve" disabled={!approvalId} onClick={() => void command("approve", `/v1/sessions/${sessionId}/approvals/${approvalId}/responses`, { decision: "approved", feedback: approvalFeedback || null }, "确认批准该操作？")}>批准</button><button className="button danger" disabled={!approvalId} onClick={() => void command("reject", `/v1/sessions/${sessionId}/approvals/${approvalId}/responses`, { decision: "rejected", feedback: approvalFeedback || null }, "确认拒绝该操作？")}>拒绝</button></div></article>
                  </div>
                  <div className="button-row"><button className="button" onClick={() => void loadResult()}>查询结果</button><button className="button" onClick={() => void loadChildren()}>加载 Child Sessions</button><button className="button" onClick={() => void loadTimeline()}>打开 Timeline</button><button className="button" onClick={() => void startStream()} disabled={streamState === "live" || streamState === "connecting"}>连接实时流</button></div>
                  {(result || children.length > 0) && <div className="result-grid">{result && <article className="subpanel"><div className="subpanel-title"><h2>Result</h2><span className={`pill ${statusTone(result.status)}`}>{String(result.status ?? "unknown")}</span></div><JsonBlock value={result} /></article>}{children.length > 0 && <article className="subpanel"><div className="subpanel-title"><h2>Child Sessions</h2><span>{children.length}</span></div><div className="child-list">{children.map((child, index) => <button key={String(child.session_id ?? index)} onClick={() => { setSessionId(String(child.session_id ?? "")); taskEtag.current = ""; }}><span className={`health-dot ${statusTone(child.status)}`} /><span><strong>{String(child.session_id ?? "unknown")}</strong><small>{String(child.status ?? "unknown")} · {String(child.role ?? child.agent_role ?? "child")}</small></span></button>)}</div></article>}</div>}
                </>
              ) : <EmptyState title="还没有打开 Session" detail="创建新任务，或粘贴已有 Session ID 开始检查。" />}
            </div>
          )}

          {activePanel === "stream" && (
            <div className="panel-stack"><div className="panel-heading"><div><p className="eyebrow">Runtime event bus</p><h1>实时事件</h1><p>这是非权威实时信号；任务完成状态仍以 Task / Result API 为准。</p></div><div className="panel-actions"><span className={`stream-badge ${streamState}`}>{streamState}</span>{streamState === "live" || streamState === "reconnecting" ? <button className="button danger" onClick={stopStream}>断开</button> : <button className="button primary" onClick={() => void startStream()} disabled={!sessionId}>连接</button>}</div></div><div className="toolbar"><label className="field grow"><span>事件类型过滤</span><input value={eventFilter} onChange={(e) => setEventFilter(e.target.value)} placeholder="tool.progress" /></label><button className="button" onClick={() => setEvents([])}>清空本地显示</button><code>cursor {streamCursor || "none"}</code></div>{visibleEvents.length ? <div className="event-list">{visibleEvents.toReversed().map((entry, index) => <details className={`event-row ${entry.event === "stream.reset" ? "reset" : ""}`} key={`${entry.id}-${index}`}><summary><span className="sequence">{entry.id || "no-id"}</span><strong>{entry.event}</strong>{entry.replay && <span className="pill warn">replay</span>}<time>{displayTime(entry.receivedAt)}</time></summary><JsonBlock value={entry.data} /></details>)}</div> : <EmptyState title="等待 Runtime Event" detail="打开一个 Session 后连接实时流。断线重连会携带最后一个 Event ID。" />}</div>
          )}

          {activePanel === "timeline" && (
            <div className="panel-stack"><div className="panel-heading"><div><p className="eyebrow">Canonical + telemetry</p><h1>Session Timeline</h1><p>将事实、Trace、Audit 与 Alert 放在同一条排障时间线上。</p></div><button className="button primary" onClick={() => void loadTimeline()} disabled={!sessionId || busy === "timeline"}>刷新 Timeline</button></div><div className="toolbar"><label className="field"><span>Kind</span><select value={timelineKind} onChange={(e) => setTimelineKind(e.target.value)}><option value="all">全部</option><option value="canonical_event">Canonical Event</option><option value="trace_span">Trace Span</option><option value="audit_event">Audit Event</option><option value="alert">Alert</option></select></label><label className="field grow"><span>组件、状态或关联 ID</span><input value={timelineQuery} onChange={(e) => setTimelineQuery(e.target.value)} placeholder="run_id / trace_id / component" /></label><span className="count-label">{visibleTimeline.length} entries</span></div>{visibleTimeline.length ? <div className="timeline-list">{visibleTimeline.map((entry, index) => <article className={`timeline-row kind-${String(entry.kind)}`} key={index}><div className="timeline-rail"><span /></div><button className="timeline-content" onClick={() => setExpandedTimeline(expandedTimeline === index ? null : index)}><div><span className="pill">{String(entry.kind ?? "entry")}</span><time>{displayTime(entry.timestamp ?? entry.occurred_at ?? entry.started_at)}</time></div><strong>{String(entry.type ?? entry.operation ?? entry.action ?? entry.name ?? "Timeline entry")}</strong><p>{String(entry.component ?? entry.status ?? entry.outcome ?? entry.severity ?? "")}</p>{expandedTimeline === index && <JsonBlock value={entry} />}</button></article>)}</div> : <EmptyState title="暂无 Timeline 数据" detail="输入 Session ID 并刷新；跨 tenant 的 Session 将返回 404。" />}</div>
          )}

          {activePanel === "metrics" && (
            <div className="panel-stack"><div className="panel-heading"><div><p className="eyebrow">Operational signals</p><h1>Metrics</h1><p>前端仅展示后端指标，不生成新的权威告警事实。</p></div><button className="button primary" onClick={() => void loadMetrics()} disabled={busy === "metrics"}>刷新 Metrics</button></div><div className="toolbar"><label className="field grow"><span>指标名称过滤</span><input value={metricQuery} onChange={(e) => setMetricQuery(e.target.value)} placeholder="projection.lag" /></label><span className="count-label">{metrics.length} points</span></div>{visibleMetrics.length ? <div className="metric-grid">{visibleMetrics.map((series) => { const values = series.values.slice(-18); const max = Math.max(...values.map((value) => Number(value.value)), 1); const critical = CRITICAL_METRICS.has(series.name); return <article className={`metric-card ${critical ? "critical" : ""}`} key={series.name}><div className="metric-title"><div><span className={`health-dot ${critical && Number(series.latest.value) > 0 ? "bad" : "good"}`} /><strong>{series.name}</strong></div><span>{values.length} pts</span></div><div className="metric-value">{Number(series.latest.value).toLocaleString()}</div><div className="sparkline" aria-label={`${series.name} trend`}>{values.map((point, index) => <span key={index} style={{ height: `${Math.max(4, Number(point.value) / max * 100)}%` }} />)}</div><div className="metric-meta"><span>{displayTime(series.latest.observed_at)}</span><code>{JSON.stringify(series.latest.labels ?? {})}</code></div></article>; })}</div> : <EmptyState title="暂无 Metrics" detail="连接 API 后刷新指标快照。" />}</div>
          )}

          {activePanel === "history" && (
            <div className="panel-stack"><div className="panel-heading"><div><p className="eyebrow">Browser session only</p><h1>请求历史</h1><p>只保留在当前页面内；复制命令已移除潜在敏感字段。</p></div><button className="button" onClick={() => setHistory([])}>清空</button></div>{history.length ? <div className="history-table"><div className="history-head"><span>Time</span><span>Request</span><span>Status</span><span>Duration</span><span>Trace</span><span /></div>{history.map((entry) => <div className="history-row" key={entry.id}><time>{displayTime(entry.at)}</time><strong><span className={`method method-${entry.method.toLowerCase()}`}>{entry.method}</span>{entry.path}</strong><span className={`http-status status-${Math.floor(entry.status / 100)}`}>{entry.status}</span><span>{entry.duration} ms</span><code>{entry.traceparent ? entry.traceparent.slice(0, 20) : "—"}</code><button className="copy-button" onClick={() => void navigator.clipboard.writeText(entry.curl)}>复制 curl</button></div>)}</div> : <EmptyState title="还没有请求记录" detail="健康检查、任务操作和监控查询都会显示在这里。" />}</div>
          )}
        </section>
      </div>
    </main>
  );
}
