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
