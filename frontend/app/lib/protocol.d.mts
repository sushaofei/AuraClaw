export function normalizeBaseUrl(value: unknown): string;
export function createCommandId(operation?: string): string;
export function redact(value: unknown): unknown;
export function safeCurl(input: { method: string; url: string; headers?: Record<string, string>; body?: unknown }): string;
export function createSseParser(onEvent: (event: { id: string; event: string; data: string }) => void): { push(chunk: string): void; remaining(): string };
export function filterTimeline<T extends Record<string, unknown>>(entries: T[], query: string, kind: string): T[];
export function metricSeries<T extends { name: string; observed_at: string; value: number }>(points: T[]): Array<{ name: string; values: T[]; latest: T }>;
