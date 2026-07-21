export type ApprovalRequest = {
  approvalId: string;
  toolName: string;
  reason: string;
  risk: string;
  redactedArguments: unknown;
  expectedEffect: string;
  status: string;
};

export type ChatSessionIndexEntry = {
  sessionId: string;
  title: string;
  updatedAt: string;
  status: string;
  runStatus: string;
};

export function normalizeBaseUrl(value: unknown): string;
export function createCommandId(operation?: string): string;
export function redact(value: unknown): unknown;
export function safeCurl(input: { method: string; url: string; headers?: Record<string, string>; body?: unknown }): string;
export function createSseParser(onEvent: (event: { id: string; event: string; data: string }) => void): { push(chunk: string): void; remaining(): string };
export function runtimeDelta(event: string, data: Record<string, unknown>): string;
export function appendUniqueEvent<T extends { id?: string }>(entries: T[], entry: T, limit?: number): T[];
export function resultText(result: Record<string, unknown> | null): string;
export function retryAfterMs(value: string | null | undefined, fallback?: number): number;
export function filterTimeline<T extends Record<string, unknown>>(entries: T[], query: string, kind: string): T[];
export function metricSeries<T extends { name: string; observed_at: string; value: number }>(points: T[]): Array<{ name: string; values: T[]; latest: T }>;
export function extractApprovalRequest(event: string, data: Record<string, unknown> | null | undefined): ApprovalRequest | null;
export function findPendingApproval(timelineEntries: Array<Record<string, unknown>> | null | undefined): ApprovalRequest | null;
export const CHAT_SESSION_STORAGE_KEY: string;
export function truncateTitle(value: unknown, limit?: number): string;
export function loadChatSessionIndex(storage: Pick<Storage, "getItem"> | null | undefined, tenant: string): ChatSessionIndexEntry[];
export function upsertChatSessionIndex(
  storage: Pick<Storage, "getItem" | "setItem"> | null | undefined,
  tenant: string,
  entry: Partial<ChatSessionIndexEntry> & { sessionId: string; goal?: string },
  limit?: number,
): ChatSessionIndexEntry[];
export function removeChatSessionIndex(
  storage: Pick<Storage, "getItem" | "setItem"> | null | undefined,
  tenant: string,
  sessionId: string,
): ChatSessionIndexEntry[];
export function buildRestoredTranscript(input: {
  goal?: string;
  resultSummary?: string;
  sessionId?: string;
}): Array<{ role: "user" | "assistant" | "system"; content: string }>;
