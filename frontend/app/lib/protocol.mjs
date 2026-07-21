const SENSITIVE_KEY = /authorization|cookie|secret|token|password|credential|api[-_]?key/i;

export function normalizeBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

export function createCommandId(operation = "command") {
  const suffix = globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `${operation}-${suffix}`;
}

export function redact(value) {
  if (Array.isArray(value)) return value.map(redact);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, SENSITIVE_KEY.test(key) ? "[REDACTED]" : redact(item)]),
  );
}

export function safeCurl({ method, url, headers = {}, body }) {
  const pieces = ["curl", "-X", method, `'${String(url).replaceAll("'", "'%27'")}'`];
  for (const [key, value] of Object.entries(headers)) {
    if (!SENSITIVE_KEY.test(key)) pieces.push("-H", `'${key}: ${String(value).replaceAll("'", "'%27'")}'`);
  }
  if (body !== undefined) {
    pieces.push("--data", `'${JSON.stringify(redact(body)).replaceAll("'", "'%27'")}'`);
  }
  return pieces.join(" ");
}

export function createSseParser(onEvent) {
  let buffer = "";
  return {
    push(chunk) {
      buffer += chunk.replaceAll("\r\n", "\n");
      const frames = buffer.split("\n\n");
      buffer = frames.pop() ?? "";
      for (const frame of frames) {
        let id = "";
        let event = "message";
        const data = [];
        for (const line of frame.split("\n")) {
          if (!line || line.startsWith(":")) continue;
          const separator = line.indexOf(":");
          const field = separator < 0 ? line : line.slice(0, separator);
          const value = separator < 0 ? "" : line.slice(separator + 1).replace(/^ /, "");
          if (field === "id") id = value;
          if (field === "event") event = value;
          if (field === "data") data.push(value);
        }
        if (data.length) onEvent({ id, event, data: data.join("\n") });
      }
    },
    remaining() {
      return buffer;
    },
  };
}

export function runtimeDelta(event, data) {
  if (event !== "model.output.delta") return "";
  const payload = data && typeof data === "object" && data.payload && typeof data.payload === "object"
    ? data.payload
    : data;
  return typeof payload?.delta === "string" ? payload.delta : "";
}

export function appendUniqueEvent(entries, entry, limit = 500) {
  if (entry.id && entries.some((item) => item.id === entry.id)) return entries;
  return [...entries, entry].slice(-limit);
}

export function resultText(result) {
  if (!result || typeof result !== "object") return "";
  if (typeof result.result_summary === "string") return result.result_summary;
  if (typeof result.output === "string") return result.output;
  if (typeof result.result === "string") return result.result;
  return "";
}

export function retryAfterMs(value, fallback = 2000) {
  const seconds = Number.parseFloat(String(value ?? ""));
  return Number.isFinite(seconds) && seconds >= 0 ? Math.max(250, seconds * 1000) : fallback;
}

export function filterTimeline(entries, query, kind) {
  const needle = String(query || "").trim().toLowerCase();
  return (entries || []).filter((entry) => {
    if (kind && kind !== "all" && entry.kind !== kind) return false;
    return !needle || JSON.stringify(entry).toLowerCase().includes(needle);
  });
}

export function metricSeries(points) {
  const grouped = new Map();
  for (const point of points || []) {
    const values = grouped.get(point.name) ?? [];
    values.push(point);
    grouped.set(point.name, values);
  }
  return [...grouped.entries()]
    .map(([name, values]) => ({
      name,
      values: values.toSorted((a, b) => String(a.observed_at).localeCompare(String(b.observed_at))),
      latest: values.toSorted((a, b) => String(b.observed_at).localeCompare(String(a.observed_at)))[0],
    }))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

const APPROVAL_TERMINAL = new Set([
  "approval.approved",
  "approval.rejected",
  "approval.expired",
  "approval.cancelled",
]);

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function pickApprovalPayload(data) {
  const root = asObject(data);
  const nested = asObject(root.payload);
  const detail = asObject(root.detail);
  const metadata = asObject(root.metadata);
  const approvalRequest = asObject(metadata.approval_request);
  return { ...root, ...nested, ...detail, ...approvalRequest };
}

export function extractApprovalRequest(event, data) {
  const type = String(event || "");
  const payload = pickApprovalPayload(data);
  const approvalId = typeof payload.approval_id === "string" ? payload.approval_id.trim() : "";
  const isApprovalEvent = type === "approval.requested"
    || type.endsWith(".approval.requested")
    || (Boolean(approvalId) && (type.includes("approval") || payload.error_code === "approval_required"));
  if (!approvalId || !isApprovalEvent) return null;
  if (APPROVAL_TERMINAL.has(type)) return null;
  return {
    approvalId,
    toolName: typeof payload.tool_name === "string"
      ? payload.tool_name
      : (typeof payload.name === "string" ? payload.name : ""),
    reason: typeof payload.reason === "string"
      ? payload.reason
      : (typeof payload.summary === "string" ? payload.summary : ""),
    risk: typeof payload.risk === "string" ? payload.risk : "",
    redactedArguments: payload.redacted_arguments ?? payload.arguments ?? null,
    expectedEffect: typeof payload.expected_effect === "string" ? payload.expected_effect : "",
    status: typeof payload.status === "string" ? payload.status : "waiting",
  };
}

export function findPendingApproval(timelineEntries) {
  const pending = new Map();
  const ordered = [...(timelineEntries || [])].toSorted((left, right) =>
    String(left.timestamp ?? left.occurred_at ?? "").localeCompare(String(right.timestamp ?? right.occurred_at ?? "")),
  );
  for (const entry of ordered) {
    const type = String(entry?.type ?? "");
    const payload = pickApprovalPayload(entry?.detail ?? entry);
    const approvalId = typeof payload.approval_id === "string" ? payload.approval_id.trim() : "";
    if (!approvalId) continue;
    if (type === "approval.requested") {
      pending.set(approvalId, {
        approvalId,
        toolName: typeof payload.tool_name === "string" ? payload.tool_name : "",
        reason: typeof payload.reason === "string" ? payload.reason : "",
        risk: typeof payload.risk === "string" ? payload.risk : "",
        redactedArguments: payload.redacted_arguments ?? null,
        expectedEffect: typeof payload.expected_effect === "string" ? payload.expected_effect : "",
        status: typeof payload.status === "string" ? payload.status : "waiting",
      });
      continue;
    }
    if (APPROVAL_TERMINAL.has(type)) pending.delete(approvalId);
  }
  const remaining = [...pending.values()];
  return remaining.at(-1) ?? null;
}

export const CHAT_SESSION_STORAGE_KEY = "auraclaw-chat-sessions-v1";
const CHAT_SESSION_LIMIT = 30;

export function truncateTitle(value, limit = 48) {
  const text = String(value || "").trim().replace(/\s+/g, " ");
  if (!text) return "未命名会话";
  return text.length > limit ? `${text.slice(0, limit - 1)}…` : text;
}

export function loadChatSessionIndex(storage, tenant) {
  try {
    const raw = storage?.getItem?.(CHAT_SESSION_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    const bucket = parsed?.[String(tenant || "")];
    return Array.isArray(bucket) ? bucket.filter((item) => item && typeof item.sessionId === "string") : [];
  } catch {
    return [];
  }
}

export function upsertChatSessionIndex(storage, tenant, entry, limit = CHAT_SESSION_LIMIT) {
  const key = String(tenant || "");
  const current = loadChatSessionIndex(storage, key);
  const sessionId = String(entry?.sessionId || "").trim();
  if (!sessionId) return current;
  const nextEntry = {
    sessionId,
    title: truncateTitle(entry.title || entry.goal || sessionId),
    updatedAt: entry.updatedAt || new Date().toISOString(),
    status: entry.status ? String(entry.status) : "",
    runStatus: entry.runStatus ? String(entry.runStatus) : "",
  };
  const next = [nextEntry, ...current.filter((item) => item.sessionId !== sessionId)].slice(0, limit);
  try {
    const raw = storage?.getItem?.(CHAT_SESSION_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    const store = parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    store[key] = next;
    storage?.setItem?.(CHAT_SESSION_STORAGE_KEY, JSON.stringify(store));
  } catch {
    // Ignore quota / private-mode failures; in-memory list still updates.
  }
  return next;
}

export function removeChatSessionIndex(storage, tenant, sessionId) {
  const key = String(tenant || "");
  const target = String(sessionId || "").trim();
  const next = loadChatSessionIndex(storage, key).filter((item) => item.sessionId !== target);
  try {
    const raw = storage?.getItem?.(CHAT_SESSION_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    const store = parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    store[key] = next;
    storage?.setItem?.(CHAT_SESSION_STORAGE_KEY, JSON.stringify(store));
  } catch {
    // Ignore persistence failures.
  }
  return next;
}

export function buildRestoredTranscript({ goal, resultSummary, sessionId }) {
  const messages = [];
  const goalText = String(goal || "").trim();
  if (goalText) messages.push({ role: "user", content: goalText });
  const summary = String(resultSummary || "").trim();
  if (summary) messages.push({ role: "assistant", content: summary });
  messages.push({
    role: "system",
    content: `已恢复 Session ${sessionId || ""}。历史正文以 Task / Result 为准；可重放的 Runtime Event 将重新显示，不会伪造中间轮次。`.trim(),
  });
  return messages;
}
