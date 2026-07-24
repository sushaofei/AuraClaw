"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  appendUniqueEvent,
  approvalFromTranscript,
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
  reconcileAssistantWithResult,
  resultText,
  retryAfterMs,
  applyChatDelta,
  finalizeChatRuns,
  runtimeDelta,
  runtimeEventRunId,
  safeCurl,
  upsertChatSessionIndex,
} from "./lib/protocol.mjs";
import type { ApprovalRequest, ChatSessionIndexEntry } from "./lib/protocol.d.mts";

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
type ChatMessage = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  streaming?: boolean;
  runId?: string;
};

const STORAGE_KEY = "auraclaw-console-v1";
const CRITICAL_METRICS = new Set([
  "projection.lag.seconds",
  "runtime.lease_lost.count",
  "tool.side_effect_unknown.count",
  "delivery.dlq.count",
]);
const TERMINAL_RUN_STATUSES = new Set(["completed", "failed", "cancelled"]);
const ACTIVE_RUN_STATUSES = new Set(["pending", "runnable", "running", "paused", "retry_wait"]);
const WAITING_FOR_HUMAN = "waiting_for_human";
const TERMINAL_SESSION_STATUSES = new Set(["closed"]);
const GENERATING_CHAT_STATUSES = new Set(["connecting", "generating", "reconnecting"]);
const PANELS = new Set(["chat", "create", "task", "stream", "timeline", "metrics", "history"]);

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
  const [activePanel, setActivePanel] = useState("chat");
  const [busy, setBusy] = useState("");
  const [notice, setNotice] = useState("等待连接 AuraClaw API");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [message, setMessage] = useState("");
  const [approvalId, setApprovalId] = useState("");
  const [approvalFeedback, setApprovalFeedback] = useState("");
  const [expandedTimeline, setExpandedTimeline] = useState<number | null>(null);
  const [chatInput, setChatInput] = useState("请介绍 AuraClaw 的核心架构边界");
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatStatus, setChatStatus] = useState("idle");
  const [chatResult, setChatResult] = useState<Json | null>(null);
  const [chatRawOpen, setChatRawOpen] = useState(false);
  const [chatSessions, setChatSessions] = useState<ChatSessionIndexEntry[]>([]);
  const [pendingApproval, setPendingApproval] = useState<ApprovalRequest | null>(null);
  const [chatApprovalFeedback, setChatApprovalFeedback] = useState("");
  const [createRequest, setCreateRequest] = useState<Json | null>(null);
  const [createResponse, setCreateResponse] = useState<Json | null>(null);
  const [createIdempotencyKey, setCreateIdempotencyKey] = useState(() => createCommandId("create-task"));
  const [resultEtag, setResultEtag] = useState("");
  const [resultPollMs, setResultPollMs] = useState(2000);
  const [resultAutoPoll, setResultAutoPoll] = useState(false);
  const taskEtag = useRef("");
  const streamAbort = useRef<AbortController | null>(null);
  const lastEventId = useRef("");
  const seenEventIds = useRef(new Set<string>());
  const finalizedRunIds = useRef(new Set<string>());
  const pendingApprovalRef = useRef<ApprovalRequest | null>(null);
  const taskRef = useRef<Json | null>(null);

  const markRunsFinalized = useCallback((...runIds: Array<string | undefined | null>) => {
    for (const value of runIds) {
      const runId = String(value ?? "").trim();
      if (runId) finalizedRunIds.current.add(runId);
    }
  }, []);

  const navigatePanel = useCallback((panel: string) => {
    setActivePanel(panel);
    if (typeof window !== "undefined") window.location.hash = panel;
  }, []);

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
        const tenantKey = String(saved.tenant || "local");
        setChatSessions(loadChatSessionIndex(localStorage, tenantKey));
      });
    } catch {
      // Ignore malformed device-local preferences.
    }
    return () => { active = false; };
  }, []);

  useEffect(() => {
    const applyHash = () => {
      const panel = window.location.hash.replace(/^#/, "");
      if (PANELS.has(panel)) setActivePanel(panel);
    };
    applyHash();
    window.addEventListener("hashchange", applyHash);
    return () => window.removeEventListener("hashchange", applyHash);
  }, []);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ baseUrl, tenant, actor, sessionId }));
  }, [baseUrl, tenant, actor, sessionId]);

  useEffect(() => {
    let active = true;
    queueMicrotask(() => {
      if (!active) return;
      setChatSessions(loadChatSessionIndex(localStorage, tenant));
    });
    return () => { active = false; };
  }, [tenant]);

  useEffect(() => {
    pendingApprovalRef.current = pendingApproval;
  }, [pendingApproval]);

  useEffect(() => {
    taskRef.current = task;
  }, [task]);

  const rememberSession = useCallback((
    id: string,
    meta: { title?: string; goal?: string; status?: unknown; runStatus?: unknown } = {},
  ) => {
    if (!id.trim()) return;
    setChatSessions(upsertChatSessionIndex(localStorage, tenant, {
      sessionId: id.trim(),
      title: meta.title || meta.goal || id.trim(),
      goal: meta.goal,
      status: meta.status ? String(meta.status) : "",
      runStatus: meta.runStatus ? String(meta.runStatus) : "",
      updatedAt: new Date().toISOString(),
    }));
  }, [tenant]);

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

  const loadTask = useCallback(async (quiet = false, targetSessionId = sessionId, force = false) => {
    if (!targetSessionId.trim()) return null;
    if (!quiet) setBusy("task");
    try {
      if (force) taskEtag.current = "";
      const headers = taskEtag.current ? { "If-None-Match": taskEtag.current } : undefined;
      const { data, response } = await api(`/v1/tasks/${encodeURIComponent(targetSessionId.trim())}`, { headers, quiet });
      if (response.status === 304) {
        if (!quiet) setNotice("任务视图没有变化");
        const cached = taskRef.current;
        return cached && String(cached.session_id ?? "") === targetSessionId.trim() ? cached : null;
      }
      const nextTask = data ? asJson(data) : null;
      if (nextTask) {
        setTask(nextTask);
        rememberSession(String(nextTask.session_id ?? targetSessionId), {
          goal: String(nextTask.goal ?? ""),
          status: nextTask.status,
          runStatus: nextTask.run_status,
        });
      }
      taskEtag.current = response.headers.get("etag") ?? taskEtag.current;
      if (!quiet) setNotice("任务视图已刷新");
      return nextTask;
    } finally {
      if (!quiet) setBusy("");
    }
  }, [api, rememberSession, sessionId]);

  const isVersionConflict = (error: unknown) => {
    const message = error instanceof Error ? error.message : String(error ?? "");
    return /expected Session version|version conflict|409/i.test(message);
  };

  const postSessionCommand = async (
    targetSessionId: string,
    path: string,
    body: unknown,
    operation: string,
    expectedVersion: number,
  ) => {
    try {
      await api(path, {
        method: "POST",
        body,
        headers: {
          "Idempotency-Key": createCommandId(operation),
          "X-Expected-Version": String(expectedVersion),
        },
      });
      return expectedVersion;
    } catch (error) {
      if (!isVersionConflict(error)) throw error;
      const refreshed = await loadTask(true, targetSessionId, true);
      const nextVersion = Number(refreshed?.projection_version ?? 0);
      await api(path, {
        method: "POST",
        body,
        headers: {
          "Idempotency-Key": createCommandId(`${operation}-retry`),
          "X-Expected-Version": String(nextVersion),
        },
      });
      return nextVersion;
    }
  };

  useEffect(() => {
    if (!autoRefresh || !sessionId) return;
    const timer = window.setInterval(() => void loadTask(true), 2500);
    return () => window.clearInterval(timer);
  }, [autoRefresh, loadTask, sessionId]);

  const createTask = async (query = goal, mode: "create" | "chat" = "create") => {
    if (!query.trim()) return null;
    setBusy("create");
    try {
      const request = { goal: query.trim() };
      const key = mode === "create" ? createIdempotencyKey : createCommandId("chat-task");
      if (mode === "create") setCreateRequest({ body: request, headers: { "Idempotency-Key": key, "X-Tenant-ID": tenant, "X-Actor-ID": actor } });
      const { data } = await api("/v1/tasks", { method: "POST", body: request, headers: { "Idempotency-Key": key } });
      const created = asJson(data);
      const id = String(created.session_id ?? "");
      setSessionId(id); setTask(created); setEvents([]); setTimeline([]); setResult(null); setChatResult(null); setStreamCursor(""); setResultEtag(""); taskEtag.current = ""; lastEventId.current = ""; seenEventIds.current.clear(); finalizedRunIds.current.clear();
      if (mode === "create") {
        setCreateResponse(created);
        setCreateIdempotencyKey(createCommandId("create-task"));
        setResultAutoPoll(true);
      }
      const view = await loadTask(true, id);
      rememberSession(id, {
        goal: query.trim(),
        status: view?.status ?? created.status,
        runStatus: view?.run_status,
      });
      setNotice(`任务已接纳：${id}`);
      return { id, created, view };
    } catch (error) {
      if (mode === "create") setNotice(`创建结果不确定；可使用原 Idempotency-Key 重试：${error instanceof Error ? error.message : "请求失败"}`);
      throw error;
    } finally { setBusy(""); }
  };

  const command = async (operation: string, path: string, body?: unknown, confirmText?: string) => {
    if (!sessionId || !task) return false;
    if (confirmText && !window.confirm(`${confirmText}\n\nTenant: ${tenant}\nSession: ${sessionId}`)) return false;
    setBusy(operation);
    try {
      await api(path, {
        method: "POST", body,
        headers: { "Idempotency-Key": createCommandId(operation), "X-Expected-Version": String(task.projection_version ?? 0) },
      });
      taskEtag.current = "";
      await loadTask(true);
      setNotice(`${operation} 已提交`);
      return true;
    } finally { setBusy(""); }
  };

  const loadResult = useCallback(async (quiet = false, targetSessionId = sessionId) => {
    if (!targetSessionId) return null;
    if (!quiet) setBusy("result");
    try {
      const headers = resultEtag ? { "If-None-Match": resultEtag } : undefined;
      const { data, response } = await api(`/v1/tasks/${encodeURIComponent(targetSessionId)}/result`, { headers, quiet });
      const nextResult = data ? asJson(data) : null;
      if (nextResult) setResult(nextResult);
      setResultEtag(response.headers.get("etag") ?? resultEtag);
      const delay = retryAfterMs(response.headers.get("retry-after"));
      setResultPollMs(delay);
      const resultStatus = String(nextResult?.status ?? result?.status ?? "");
      if (response.status !== 202 && TERMINAL_RUN_STATUSES.has(resultStatus)) setResultAutoPoll(false);
      if (!quiet) {
        if (response.status === 304) setNotice("Result 未变化，已命中 ETag 缓存");
        else setNotice(response.status === 202 ? `结果尚未就绪，将在 ${Math.round(delay / 1000)} 秒后重试` : "结果已加载");
      }
      return { result: nextResult, response };
    } finally { if (!quiet) setBusy(""); }
  }, [api, result, resultEtag, sessionId]);

  useEffect(() => {
    if (!resultAutoPoll || !sessionId) return;
    const timer = window.setTimeout(() => {
      void Promise.all([loadTask(true), loadResult(true)]).catch(() => setResultAutoPoll(false));
    }, resultPollMs);
    return () => window.clearTimeout(timer);
  }, [loadResult, loadTask, resultAutoPoll, resultPollMs, sessionId, task?.projection_version]);

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
      navigatePanel("timeline"); setNotice("Session Timeline 已加载");
    } finally { setBusy(""); }
  };

  const loadMetrics = useCallback(async (quiet = false) => {
    if (!quiet) setBusy("metrics");
    try {
      const { data } = await api("/v1/operations/metrics", { quiet });
      setMetrics(Array.isArray(asJson(data).metrics) ? (asJson(data).metrics as Json[]) : []);
      if (!quiet) { navigatePanel("metrics"); setNotice("Metrics 已刷新"); }
    } finally { if (!quiet) setBusy(""); }
  }, [api, navigatePanel]);

  const resolvePendingApproval = useCallback(async (
    targetSessionId: string,
    preloaded?: ApprovalRequest | null,
  ) => {
    if (!targetSessionId.trim()) return null;
    if (pendingApprovalRef.current) return pendingApprovalRef.current;
    try {
      const pending = preloaded ?? null;
      let resolved = pending;
      if (!resolved) {
        const { data } = await api(`/v1/operations/sessions/${encodeURIComponent(targetSessionId)}/timeline`, { quiet: true });
        const entries = Array.isArray(asJson(data).entries) ? (asJson(data).entries as Json[]) : [];
        setTimeline(entries);
        resolved = findPendingApproval(entries);
      }
      if (resolved) {
        setPendingApproval(resolved);
        setApprovalId(resolved.approvalId);
        setChatStatus("awaiting_approval");
        setChatMessages((current) => {
          if (current.some((item) => item.content.includes(resolved.approvalId))) return current;
          return [...current, {
            id: createCommandId("chat-system"),
            role: "system",
            content: `需要人工审批：${resolved.toolName || "tool"}（${resolved.approvalId}）${resolved.reason ? ` — ${resolved.reason}` : ""}`,
          }];
        });
      }
      return resolved;
    } catch {
      return null;
    }
  }, [api]);

  const applyApprovalFromEvent = useCallback((eventName: string, data: Json) => {
    const pending = extractApprovalRequest(eventName, data);
    if (!pending) return;
    if (pendingApprovalRef.current?.approvalId === pending.approvalId) return;
    setPendingApproval(pending);
    setApprovalId(pending.approvalId);
    setChatStatus("awaiting_approval");
    setChatMessages((current) => {
      if (current.some((item) => item.content.includes(pending.approvalId))) return current;
      return [...current, {
        id: createCommandId("chat-system"),
        role: "system",
        content: `需要人工审批：${pending.toolName || "tool"}（${pending.approvalId}）${pending.reason ? ` — ${pending.reason}` : ""}`,
      }];
    });
  }, []);

  const stopStream = useCallback(() => {
    streamAbort.current?.abort(); streamAbort.current = null; setStreamState("stopped");
    setChatStatus((current) => GENERATING_CHAT_STATUSES.has(current) ? "stopped" : current);
  }, []);

  useEffect(() => () => streamAbort.current?.abort(), []);

  const startStream = async (targetSessionId = sessionId, navigate = true) => {
    if (!targetSessionId || streamAbort.current) return;
    const controller = new AbortController();
    streamAbort.current = controller; setStreamState(lastEventId.current ? "reconnecting" : "connecting");
    if (navigate) navigatePanel("stream");
    else if (!pendingApprovalRef.current) setChatStatus(lastEventId.current ? "reconnecting" : "connecting");
    let attempt = 0;
    while (!controller.signal.aborted) {
      try {
        const headers = identityHeaders();
        const reconnectCursor = lastEventId.current;
        if (reconnectCursor) headers["Last-Event-ID"] = reconnectCursor;
        const response = await fetch(`${normalizeBaseUrl(baseUrl)}/v1/streams/${encodeURIComponent(targetSessionId)}`, { headers, signal: controller.signal, cache: "no-store" });
        if (!response.ok || !response.body) throw new Error(`HTTP ${response.status}`);
        attempt = 0; setStreamState("live");
        if (!pendingApprovalRef.current) setChatStatus("generating");
        setNotice("Runtime Event 流已连接");
        const decoder = new TextDecoder();
        const parser = createSseParser((frame) => {
          let data: Json;
          try { data = asJson(JSON.parse(frame.data)); } catch { data = { raw: frame.data }; }
          if (frame.id && seenEventIds.current.has(frame.id)) return;
          if (frame.id) seenEventIds.current.add(frame.id);
          const replay = Boolean(frame.id && reconnectCursor);
          if (frame.id) { lastEventId.current = frame.id; setStreamCursor(frame.id); }
          if (frame.event === "stream.reset") {
            setNotice("事件游标已过期，请以 Task View 为准并重新同步");
            setChatMessages((current) => [...current, { id: createCommandId("chat-system"), role: "system", content: "流式回放游标已过期，缺失内容不会被伪造；请以最终 Result 为准。" }]);
          }
          const entry = { id: frame.id || createCommandId("event"), event: frame.event, data, receivedAt: new Date().toISOString(), replay };
          setEvents((current) => appendUniqueEvent(current, entry));
          applyApprovalFromEvent(frame.event, data);
          const payloadType = typeof data.type === "string" ? data.type : "";
          if (payloadType) applyApprovalFromEvent(payloadType, data);
          const delta = runtimeDelta(frame.event, data);
          if (delta) {
            const runId = runtimeEventRunId(data);
            setChatMessages((current) => applyChatDelta(current, {
              delta,
              runId,
              createId: createCommandId,
              finalizedRunIds: finalizedRunIds.current,
            }));
          }
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
        if (!pendingApprovalRef.current) setChatStatus("reconnecting");
        setNotice(`实时流中断，正在第 ${attempt} 次重连`);
        await new Promise((resolve) => window.setTimeout(resolve, Math.min(1000 * 2 ** (attempt - 1), 10000)));
      }
    }
    streamAbort.current = null;
  };

  const sendChat = async () => {
    const query = chatInput.trim();
    if (!query || busy || GENERATING_CHAT_STATUSES.has(chatStatus) || chatStatus === "awaiting_approval") return;
    setBusy("chat-send");
    setPendingApproval(null);
    setChatApprovalFeedback("");
    try {
      let targetSessionId = sessionId.trim();
      const currentTask = targetSessionId ? await loadTask(true, targetSessionId, true) : null;
      const sessionStatus = String(currentTask?.status ?? "");
      const runStatusValue = String(currentTask?.run_status ?? "");

      if (targetSessionId && currentTask && (runStatusValue === WAITING_FOR_HUMAN || sessionStatus === WAITING_FOR_HUMAN)) {
        setChatStatus("awaiting_approval");
        await resolvePendingApproval(targetSessionId);
        setNotice("当前 Session 等待人工审批，请先批准或拒绝后再追问。");
        return;
      }

      if (targetSessionId && currentTask && ACTIVE_RUN_STATUSES.has(runStatusValue)) {
        setChatStatus("connecting");
        if (!streamAbort.current) void startStream(targetSessionId, false);
        setNotice("当前 Run 尚未结束，已重新连接实时流；请等待完成或点击停止生成后再追问。");
        return;
      }

      if (!targetSessionId || (currentTask && TERMINAL_SESSION_STATUSES.has(sessionStatus))) {
        const continuesAfterTerminal = Boolean(targetSessionId && currentTask && TERMINAL_SESSION_STATUSES.has(sessionStatus));
        const created = await createTask(query, "chat");
        if (!created) return;
        targetSessionId = created.id;
        if (continuesAfterTerminal) {
          setChatMessages((current) => [...current, { id: createCommandId("chat-system"), role: "system", content: "上一 Session 已显式关闭，本次问题已创建新的 Session。" }]);
        }
      } else {
        const expectedVersion = Number(currentTask?.projection_version ?? 0);
        await postSessionCommand(
          targetSessionId,
          `/v1/sessions/${encodeURIComponent(targetSessionId)}/messages`,
          { message: query },
          "chat-message",
          expectedVersion,
        );
        const afterMessage = await loadTask(true, targetSessionId, true);
        await postSessionCommand(
          targetSessionId,
          `/v1/sessions/${encodeURIComponent(targetSessionId)}/runs`,
          undefined,
          "chat-run",
          Number(afterMessage?.projection_version ?? expectedVersion + 1),
        );
        await loadTask(true, targetSessionId, true);
        rememberSession(targetSessionId, {
          title: query,
          status: afterMessage?.status,
          runStatus: afterMessage?.run_status,
        });
      }
      setChatMessages((current) => {
        for (const item of current) {
          if (item.role === "assistant" && item.runId) finalizedRunIds.current.add(item.runId);
        }
        return [
          ...finalizeChatRuns(current),
          { id: createCommandId("user"), role: "user", content: query },
        ];
      });
      setChatInput("");
      setChatResult(null);
      setChatStatus("connecting");
      if (!streamAbort.current) void startStream(targetSessionId, false);
      setNotice(`问题已提交：${targetSessionId}`);
    } catch (error) {
      setChatStatus("failed");
      setNotice(`发送失败，输入已保留：${error instanceof Error ? error.message : "请求失败"}`);
    } finally {
      setBusy("");
    }
  };

  const openChatSession = async (targetId = sessionId) => {
    const id = targetId.trim();
    if (!id) return;
    stopStream();
    taskEtag.current = "";
    setResultEtag("");
    lastEventId.current = "";
    seenEventIds.current.clear();
    finalizedRunIds.current.clear();
    setEvents([]);
    setPendingApproval(null);
    setChatApprovalFeedback("");
    setChatResult(null);
    setSessionId(id);
    setBusy("task");
    try {
      const [nextTask, loaded, transcriptPayload] = await Promise.all([
        loadTask(true, id),
        loadResult(true, id),
        api(`/v1/tasks/${encodeURIComponent(id)}/transcript`, { quiet: true })
          .then(({ data }) => asJson(data))
          .catch(() => null),
      ]);
      if (!nextTask) {
        setChatStatus("failed");
        setNotice(`无法恢复 Session：${id}`);
        return;
      }
      const summary = resultText(loaded?.result ?? null) || String(nextTask.result_summary ?? "");
      if (loaded?.result) setChatResult(loaded.result);
      let transcriptMessages = Array.isArray(transcriptPayload?.messages)
        ? (transcriptPayload.messages as Json[])
        : [];
      let pendingFromTranscript = approvalFromTranscript(
        transcriptPayload?.pending_approval && typeof transcriptPayload.pending_approval === "object"
          ? (transcriptPayload.pending_approval as Json)
          : null,
      );
      // Transcript may 404 on stale backends; fall back to Timeline so multi-turn
      // history is not reduced to goal + latest result_summary.
      let timelineEntries: Json[] = [];
      if (transcriptMessages.length === 0) {
        try {
          const { data } = await api(`/v1/operations/sessions/${encodeURIComponent(id)}/timeline`, { quiet: true });
          timelineEntries = Array.isArray(asJson(data).entries) ? (asJson(data).entries as Json[]) : [];
          setTimeline(timelineEntries);
          if (!pendingFromTranscript) {
            pendingFromTranscript = findPendingApproval(timelineEntries);
          }
        } catch {
          timelineEntries = [];
        }
      }
      const restored = buildRestoredTranscript({
        goal: String(nextTask.goal ?? ""),
        resultSummary: summary,
        sessionId: id,
        transcriptMessages,
        timelineEntries,
      }).map((item) => ({ ...item, id: createCommandId(`restore-${item.role}`) }));
      for (const item of restored) {
        if (item.role === "assistant" && item.runId) finalizedRunIds.current.add(item.runId);
      }
      const activeRunId = String(nextTask.run_id ?? "").trim();
      if (activeRunId && ACTIVE_RUN_STATUSES.has(String(nextTask.run_status ?? ""))) {
        finalizedRunIds.current.delete(activeRunId);
      }
      setChatMessages(restored);
      rememberSession(id, {
        goal: String(nextTask.goal ?? ""),
        status: nextTask.status,
        runStatus: nextTask.run_status,
      });
      const run = String(nextTask.run_status ?? "");
      const sessionStatus = String(nextTask.status ?? "");
      if (run === WAITING_FOR_HUMAN || sessionStatus === WAITING_FOR_HUMAN) {
        setChatStatus("awaiting_approval");
        await resolvePendingApproval(id, pendingFromTranscript);
        if (!streamAbort.current) void startStream(id, false);
        setNotice(`已恢复并等待人工审批：${id}`);
        return;
      }
      if (ACTIVE_RUN_STATUSES.has(run)) {
        setChatStatus("connecting");
        void startStream(id, false);
        setNotice(`已恢复并重连流：${id}`);
        return;
      }
      setChatStatus(TERMINAL_RUN_STATUSES.has(run) ? run : "ready");
      setNotice(`已恢复 Session：${id}`);
    } catch (error) {
      setChatStatus("failed");
      setNotice(`恢复失败：${error instanceof Error ? error.message : "请求失败"}`);
    } finally {
      setBusy("");
    }
  };

  const startNewChat = () => {
    stopStream();
    setSessionId("");
    setTask(null);
    setResult(null);
    setChatMessages([]);
    setEvents([]);
    setChatResult(null);
    setPendingApproval(null);
    setChatApprovalFeedback("");
    setChatStatus("idle");
    setStreamCursor("");
    setResultEtag("");
    taskEtag.current = "";
    lastEventId.current = "";
    seenEventIds.current.clear();
    finalizedRunIds.current.clear();
    setNotice("已开始新对话；历史会话索引仍保留在本机。");
  };

  const removeChatSession = (id: string) => {
    setChatSessions(removeChatSessionIndex(localStorage, tenant, id));
    if (sessionId === id) startNewChat();
  };

  const stopChat = async () => {
    if (!sessionId || !task) return;
    try {
      const cancelled = await command("cancel", `/v1/sessions/${encodeURIComponent(sessionId)}/cancel`, { reason: "stopped from streaming chat test" }, "确认停止当前问答生成？");
      if (!cancelled) return;
      stopStream();
      setChatStatus("cancelled");
      setPendingApproval(null);
      await loadResult(true);
      rememberSession(sessionId, {
        goal: String(task.goal ?? ""),
        status: "ready",
        runStatus: "cancelled",
      });
    } catch {
      // The shared request notice already contains the structured failure.
    }
  };

  const respondChatApproval = async (decision: "approved" | "rejected") => {
    if (!sessionId || !pendingApproval) return;
    const label = decision === "approved" ? "批准" : "拒绝";
    const approval = pendingApproval;
    const feedback = chatApprovalFeedback.trim();
    if (!window.confirm(`确认${label}该操作？\n\nTenant: ${tenant}\nSession: ${sessionId}\nApproval: ${approval.approvalId}`)) return;
    setBusy(decision);
    try {
      const currentTask = await loadTask(true, sessionId, true);
      if (!currentTask) throw new Error("无法刷新 Task View");
      await postSessionCommand(
        sessionId,
        `/v1/sessions/${encodeURIComponent(sessionId)}/approvals/${encodeURIComponent(approval.approvalId)}/responses`,
        { decision, feedback: feedback || null },
        decision,
        Number(currentTask.projection_version ?? 0),
      );
      setPendingApproval(null);
      setChatApprovalFeedback("");
      setChatMessages((current) => [...current, {
        id: createCommandId("chat-system"),
        role: "system",
        content: `已${label}审批 ${approval.approvalId}${feedback ? `：${feedback}` : ""}`,
      }]);
      await loadTask(true, sessionId, true);
      setChatStatus("connecting");
      if (!streamAbort.current) void startStream(sessionId, false);
      setNotice(`审批已${label}，继续等待 Runtime`);
    } catch (error) {
      setNotice(`审批失败：${error instanceof Error ? error.message : "请求失败"}`);
    } finally {
      setBusy("");
    }
  };

  useEffect(() => {
    if (!sessionId || !GENERATING_CHAT_STATUSES.has(chatStatus)) return;
    let cancelled = false;
    let timer: number | undefined;
    const pollUntilTerminal = async () => {
      try {
        const nextTask = await loadTask(true);
        const currentTask = nextTask ?? task;
        const status = String(currentTask?.run_status ?? "");
        const sessionStatus = String(currentTask?.status ?? "");
        if (status === WAITING_FOR_HUMAN || sessionStatus === WAITING_FOR_HUMAN) {
          setChatMessages((current) => finalizeChatRuns(current));
          setChatStatus("awaiting_approval");
          await resolvePendingApproval(sessionId);
          return;
        }
        if (!TERMINAL_RUN_STATUSES.has(status)) {
          if (!cancelled) timer = window.setTimeout(() => void pollUntilTerminal(), resultPollMs);
          return;
        }
        const loaded = await loadResult(true);
        const finalResult = loaded?.result ?? result;
        if (finalResult) setChatResult(finalResult);
        const completedRunId = String(currentTask?.run_id ?? finalResult?.run_id ?? "");
        markRunsFinalized(completedRunId);
        const summary = resultText(finalResult ?? null);
        setChatMessages((current) => reconcileAssistantWithResult(current, {
          runId: completedRunId,
          resultSummary: summary,
          createId: createCommandId,
        }));
        setChatStatus(status);
        setPendingApproval(null);
        streamAbort.current?.abort();
        streamAbort.current = null;
        setStreamState("stopped");
      } catch {
        if (!pendingApprovalRef.current) setChatStatus("reconnecting");
      }
    };
    timer = window.setTimeout(() => void pollUntilTerminal(), resultPollMs);
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [chatStatus, loadResult, loadTask, markRunsFinalized, resolvePendingApproval, result, resultPollMs, sessionId, task]);

  const copyJson = (value: unknown) => navigator.clipboard.writeText(JSON.stringify(redact(value), null, 2));

  const visibleTimeline = useMemo(() => filterTimeline(timeline, timelineQuery, timelineKind), [timeline, timelineKind, timelineQuery]);
  const visibleEvents = useMemo(() => events.filter((event) => !eventFilter || event.event.toLowerCase().includes(eventFilter.toLowerCase())), [eventFilter, events]);
  const visibleMetrics = useMemo(() => metricSeries(metrics as Array<{ name: string; observed_at: string; value: number }>).filter((series) => !metricQuery || series.name.toLowerCase().includes(metricQuery.toLowerCase())), [metricQuery, metrics]);
  const progress = Number(task?.progress ?? 0);
  const taskStatus = String(task?.status ?? "未加载");
  const runStatus = String(task?.run_status ?? "未加载");
  const streamedAnswer = [...chatMessages].reverse().find((item) => item.role === "assistant")?.content ?? "";
  const finalAnswer = resultText(chatResult);
  const answerMismatch = Boolean(streamedAnswer && finalAnswer && streamedAnswer.trim() !== finalAnswer.trim());
  const chatBusy = Boolean(busy) || GENERATING_CHAT_STATUSES.has(chatStatus) || chatStatus === "awaiting_approval";

  return (
    <main className="console-shell">
      <header className="topbar">
        <div className="brand"><span className="brand-mark">AC</span><div><strong>AuraClaw</strong><small>protocol test console / M7.1</small></div></div>
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
            ["chat", "01", "智能问答"], ["create", "02", "创建任务"], ["task", "03", "Session 详情"],
            ["stream", "04", "实时事件"], ["timeline", "05", "Timeline"], ["metrics", "06", "Metrics"], ["history", "07", "请求历史"],
          ].map(([id, number, label]) => <button key={id} className={activePanel === id ? "nav-item active" : "nav-item"} onClick={() => navigatePanel(id)}><span>{number}</span>{label}</button>)}
          <div className="sidebar-note"><span className="health-dot good" /><div><strong>Canonical first</strong><p>最终状态以 Task / Result API 为准</p></div></div>
        </aside>

        <section className="panel-stage">
          {activePanel === "chat" && (
            <div className="panel-stack chat-page">
              <div className="panel-heading"><div><p className="eyebrow">Streaming protocol lab</p><h1>智能问答</h1><p>支持打断生成、历史会话恢复，以及等待人工审批时的 human-in-the-loop。</p></div><div className="panel-actions"><span className={`stream-badge ${chatStatus}`}>{chatStatus}</span><button className="button" onClick={startNewChat}>新建对话</button><button className="button" onClick={() => setChatRawOpen((value) => !value)}>{chatRawOpen ? "收起事件" : "原始事件"}</button></div></div>
              <div className="session-locator"><label className="field grow"><span>恢复已有 Session</span><input value={sessionId} onChange={(e) => { setSessionId(e.target.value); taskEtag.current = ""; }} placeholder="ses_..." /></label><button className="button" onClick={() => void openChatSession()} disabled={!sessionId.trim() || busy === "task"}>恢复并重连</button></div>
              <div className="chat-layout with-history">
                <aside className="chat-history" aria-label="历史会话列表">
                  <div className="subpanel-title"><h2>历史会话</h2><span>{chatSessions.length}</span></div>
                  <p className="chat-history-note">仅本机保存 session 索引，不存问答正文。</p>
                  {chatSessions.length ? (
                    <div className="chat-history-list">
                      {chatSessions.map((entry) => (
                        <div className={`chat-history-item ${entry.sessionId === sessionId ? "active" : ""}`} key={entry.sessionId}>
                          <button type="button" className="chat-history-open" onClick={() => void openChatSession(entry.sessionId)}>
                            <strong>{entry.title}</strong>
                            <small>{entry.sessionId}</small>
                            <span>{displayTime(entry.updatedAt)} · {entry.runStatus || entry.status || "unknown"}</span>
                          </button>
                          <button type="button" className="chat-history-remove" aria-label="删除会话索引" onClick={() => removeChatSession(entry.sessionId)}>×</button>
                        </div>
                      ))}
                    </div>
                  ) : <EmptyState title="暂无历史会话" detail="发送问题后会在此留下可恢复的 Session 索引。" />}
                </aside>
                <section className="chat-surface" aria-label="智能问答消息">
                  <div className="chat-transcript" aria-live="polite">
                    {chatMessages.length ? chatMessages.map((item) => <article className={`chat-message ${item.role}`} key={item.id}><div className="chat-avatar">{item.role === "user" ? "YOU" : item.role === "assistant" ? "AC" : "SYS"}</div><div><small>{item.role === "user" ? "你" : item.role === "assistant" ? "AuraClaw" : "系统提示"}</small><p>{item.content}{item.streaming && <span className="typing-caret" aria-label="生成中" />}</p></div></article>) : <div className="chat-welcome"><span>STREAM / RESULT / HITL</span><h2>从一个真实问题开始</h2><p>首次发送会创建任务并自动连接 SSE。可通过左侧历史会话恢复；生成中可打断，审批等待时直接在对话内处理。</p></div>}
                    {pendingApproval && (
                      <article className="hitl-card" aria-label="人工审批">
                        <div className="subpanel-title"><h2>Human-in-the-loop</h2><span className="pill warn">waiting</span></div>
                        <dl className="protocol-dl">
                          <div><dt>Approval</dt><dd>{pendingApproval.approvalId}</dd></div>
                          <div><dt>Tool</dt><dd>{pendingApproval.toolName || "—"}</dd></div>
                          <div><dt>Risk</dt><dd>{pendingApproval.risk || "—"}</dd></div>
                          <div><dt>Reason</dt><dd>{pendingApproval.reason || "—"}</dd></div>
                        </dl>
                        {pendingApproval.redactedArguments != null && <JsonBlock value={pendingApproval.redactedArguments} />}
                        <label className="field"><span>反馈（可选）</span><input value={chatApprovalFeedback} onChange={(e) => setChatApprovalFeedback(e.target.value)} placeholder="批准或拒绝时的备注" /></label>
                        <div className="button-row">
                          <button className="button approve" disabled={Boolean(busy)} onClick={() => void respondChatApproval("approved")}>批准</button>
                          <button className="button danger" disabled={Boolean(busy)} onClick={() => void respondChatApproval("rejected")}>拒绝</button>
                        </div>
                      </article>
                    )}
                  </div>
                  <div className="chat-composer"><label className="field"><span>输入问题</span><textarea value={chatInput} onChange={(e) => setChatInput(e.target.value)} onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void sendChat(); } }} rows={4} placeholder="输入问题；Enter 发送，Shift+Enter 换行" disabled={chatBusy && chatStatus !== "awaiting_approval"} /></label><div className="chat-composer-actions"><span>{chatStatus === "awaiting_approval" ? "等待人工审批中" : "Runtime Event 仅用于实时体验"}</span><div className="button-row"><button className="button" onClick={() => { setChatMessages([]); setEvents([]); setChatResult(null); }}>清空显示</button>{GENERATING_CHAT_STATUSES.has(chatStatus) && <button className="button danger" onClick={() => void stopChat()}>停止生成</button>}<button className="button primary" onClick={() => void sendChat()} disabled={!chatInput.trim() || chatBusy}>发送</button></div></div></div>
                </section>
                <aside className="chat-inspector">
                  <article className="subpanel"><div className="subpanel-title"><h2>权威结果核对</h2><span className={`pill ${statusTone(chatResult?.status ?? chatStatus)}`}>{String(chatResult?.status ?? chatStatus)}</span></div>{chatResult ? <><div className={`consistency-note ${answerMismatch ? "mismatch" : "match"}`}><strong>{answerMismatch ? "流式内容与 Result 存在差异" : "流式内容与 Result 已核对"}</strong><p>{answerMismatch ? "最终回答以 Result API 为准。" : "Result 是任务完成与交付的事实来源。"}</p></div><div className="answer-block"><small>FINAL RESULT</small><p>{finalAnswer || "结果没有文本摘要，请查看结构化 JSON。"}</p></div><button className="copy-button" onClick={() => void copyJson(chatResult)}>复制脱敏 Result</button><JsonBlock value={chatResult} /></> : <EmptyState title="等待最终 Result" detail="页面会轮询 Task View，并按 Retry-After 获取最终结果。" />}</article>
                  <article className="subpanel protocol-facts"><div className="subpanel-title"><h2>当前协议状态</h2><span>canonical first</span></div><dl><div><dt>Session</dt><dd>{sessionId || "—"}</dd></div><div><dt>Session status</dt><dd>{taskStatus}</dd></div><div><dt>Run status</dt><dd>{runStatus}</dd></div><div><dt>Approval</dt><dd>{pendingApproval?.approvalId || "—"}</dd></div><div><dt>Event cursor</dt><dd>{streamCursor || "—"}</dd></div><div><dt>Streamed chars</dt><dd>{streamedAnswer.length}</dd></div></dl></article>
                </aside>
              </div>
              {chatRawOpen && <article className="subpanel"><div className="subpanel-title"><h2>Runtime Event 原始视图</h2><span>{events.length} events</span></div>{events.length ? <div className="event-list compact-events">{events.toReversed().map((entry) => <details className={`event-row ${entry.event === "stream.reset" ? "reset" : ""}`} key={entry.id}><summary><span className="sequence">{entry.id}</span><strong>{entry.event}</strong>{entry.replay && <span className="pill warn">replay</span>}<time>{displayTime(entry.receivedAt)}</time></summary><JsonBlock value={entry.data} /></details>)}</div> : <EmptyState title="暂无事件" detail="发送问题或恢复 Session 后会自动连接。" />}</article>}
            </div>
          )}

          {activePanel === "create" && (
            <div className="panel-stack create-page">
              <div className="panel-heading"><div><p className="eyebrow">Query / Result protocol lab</p><h1>创建任务</h1><p>核对 Query、Task View、Result 与 HTTP 缓存语义。</p></div><div className="panel-actions"><label className="switch"><input type="checkbox" checked={resultAutoPoll} onChange={(e) => setResultAutoPoll(e.target.checked)} /><span />按 Retry-After 轮询</label><button className="button" onClick={() => void Promise.all([loadTask(), loadResult()])} disabled={!sessionId}>立即刷新</button></div></div>
              <div className="create-row query-create-row"><label className="field grow"><span>Query / Goal</span><textarea value={goal} onChange={(e) => setGoal(e.target.value)} rows={3} /></label><button className="button primary tall" onClick={() => void createTask().catch(() => undefined)} disabled={busy === "create" || !goal.trim()}>提交 Query</button></div>
              <details className="advanced-panel"><summary>高级请求设置</summary><div className="advanced-content"><label className="field grow"><span>Idempotency-Key</span><input className="mono" value={createIdempotencyKey} onChange={(e) => setCreateIdempotencyKey(e.target.value)} /></label><button className="button" onClick={() => setCreateIdempotencyKey(createCommandId("create-task"))}>生成新 Key</button><p>网络结果不确定时保留原 Key 重试；明确创建新任务时生成新 Key。</p></div></details>
              <div className="session-locator"><label className="field grow"><span>打开已有 Session</span><input value={sessionId} onChange={(e) => { setSessionId(e.target.value); setTask(null); setResult(null); setResultEtag(""); taskEtag.current = ""; }} placeholder="ses_..." /></label><button className="button" onClick={() => void Promise.all([loadTask(), loadResult()]).then(() => setResultAutoPoll(true))} disabled={!sessionId.trim()}>打开任务</button><button className="button" onClick={() => navigatePanel("task")} disabled={!task}>Session 详情</button></div>
              <div className="protocol-grid">
                <article className="protocol-column"><header><span>01</span><div><small>REQUEST</small><h2>Query</h2></div>{createRequest && <button className="copy-button" onClick={() => void copyJson(createRequest)}>复制</button>}</header>{createRequest ? <><div className="protocol-summary"><strong>POST /v1/tasks</strong><span>202 Accepted</span></div><JsonBlock value={createRequest} />{createResponse && <><h3>创建响应</h3><JsonBlock value={createResponse} /></>}</> : <EmptyState title="尚未提交 Query" detail="填写目标并提交；实际脱敏请求会显示在这里。" />}</article>
                <article className="protocol-column"><header><span>02</span><div><small>AUTHORITATIVE VIEW</small><h2>Task</h2></div>{task && <button className="copy-button" onClick={() => void copyJson(task)}>复制</button>}</header>{task ? <><div className="task-phase"><span className={`health-dot ${statusTone(task.run_status ?? task.status)}`} /><div><strong>Session {String(task.status ?? "unknown")} · Run {String(task.run_status ?? "unknown")}</strong><small>{String(task.current_stage ?? "—")} · projection v{String(task.projection_version ?? "—")}</small></div></div><dl className="protocol-dl"><div><dt>Session</dt><dd>{String(task.session_id ?? sessionId)}</dd></div><div><dt>Run</dt><dd>{String(task.run_id ?? "—")}</dd></div><div><dt>Progress</dt><dd>{Math.round(Number(task.progress ?? 0) * 100)}%</dd></div><div><dt>Delivery</dt><dd>{String(task.delivery_status ?? "not configured")}</dd></div></dl><JsonBlock value={task} /></> : <EmptyState title="等待 Task View" detail="提交 Query 或通过 Session ID 打开任务。" />}</article>
                <article className="protocol-column result-column"><header><span>03</span><div><small>CONDITIONAL GET</small><h2>Result</h2></div>{result && <button className="copy-button" onClick={() => void copyJson(result)}>复制</button>}</header><div className="cache-strip"><span>ETag <code>{resultEtag || "—"}</code></span><span>next {Math.round(resultPollMs / 1000)}s</span></div>{result ? <><div className="task-phase"><span className={`health-dot ${statusTone(result.status)}`} /><div><strong>{String(result.status ?? "unknown")}</strong><small>{resultText(result) || "结构化结果"}</small></div></div><JsonBlock value={result} /></> : <EmptyState title="等待 Result" detail="处理中返回 202；完成后使用 ETag 避免重复下载。" />}</article>
              </div>
            </div>
          )}

          {activePanel === "task" && (
            <div className="panel-stack">
              <div className="panel-heading"><div><p className="eyebrow">Session control</p><h1>Session 详情</h1><p>定位并控制一个 managed agent session。</p></div><div className="panel-actions"><label className="switch"><input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} /><span />自动刷新</label><button className="button" onClick={() => void loadTask()} disabled={!sessionId}>刷新</button></div></div>
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
                    <article className="subpanel"><div className="subpanel-title"><h2>Session commands</h2><span>expected v{String(task.projection_version ?? 0)}</span></div><label className="field"><span>追加消息</span><textarea value={message} onChange={(e) => setMessage(e.target.value)} placeholder="为当前 Session 补充上下文" rows={3} /></label><div className="button-row"><button className="button primary" disabled={!message.trim()} onClick={() => void command("append-message", `/v1/sessions/${sessionId}/messages`, { message }).then(() => setMessage(""))}>追加消息</button><button className="button" onClick={() => void command("request-run", `/v1/sessions/${sessionId}/runs`)}>请求运行</button><button className="button danger" onClick={() => void command("cancel", `/v1/sessions/${sessionId}/cancel`, { reason: "cancelled from operations console" }, "确认取消当前 Run？")}>取消 Run</button><button className="button" onClick={() => void command("resume", `/v1/sessions/${sessionId}/resume`, undefined, "确认恢复任务？")}>恢复</button><button className="button danger" onClick={() => void command("close", `/v1/sessions/${sessionId}/close`, { reason: "closed from operations console" }, "确认永久关闭 Session？")}>关闭 Session</button></div></article>
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
