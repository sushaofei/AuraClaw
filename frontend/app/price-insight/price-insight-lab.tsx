"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import styles from "./price-insight.module.css";

type Json = Record<string, unknown>;
type Kpi = {
  key: string;
  label: string;
  value: string | number;
  suffix: string;
};

const TERMINAL = new Set(["completed", "failed", "cancelled"]);
const STEP_DEFINITIONS = [
  ["能力发现", "auraclaw.capabilities.search"],
  ["Skill 装载", "auraclaw.capabilities.load"],
  ["Skill 激活", "skill.activated"],
  ["数据范围", "procurement.price.dataset.profile"],
  ["质量门禁", "procurement.price.dataset.quality.check"],
  ["原子指标", "procurement.price.metric."],
] as const;

function asJson(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Json)
    : {};
}

function asList(value: unknown): Json[] {
  return Array.isArray(value) ? value.map(asJson) : [];
}

function defaultApiBase(): string {
  if (typeof window === "undefined") return "/auraclaw-api";
  return `${window.location.origin}/auraclaw-api`;
}

function taskGoal(
  periodFrom: string,
  periodTo: string,
  anchor: string,
  threshold: string,
): string {
  const anchorLabel = {
    history: "历史价格",
    region: "跨区域价格",
    market: "行业市场均价",
  }[anchor] ?? "行业市场均价";
  return [
    "请使用价格洞察智能体完成采购价格分析。",
    `分析周期为 ${periodFrom} 至 ${periodTo}，主要对标 ${anchorLabel}，偏离阈值为 ${threshold}%。`,
    "必须按 Skill 3.0 SOP 先调用数据校验子 Skill，再通过八个独立指标 Tool 逐项生成关键指标；必要时调用证据 Tool。",
    "所有原子结果必须使用相同 source_revision，并输出历史/区域/市场指标、正负价格影响及证据。",
    "不要自行拼接 SQL，不要把正负影响金额抵消。",
  ].join("");
}

function extractSnapshot(entries: Json[]): Json | null {
  let profile: Json | null = null;
  let quality: Json | null = null;
  const metricResults = new Map<string, Json>();
  const evidence: Json[] = [];
  for (const entry of entries) {
    if (String(entry.type ?? "") !== "tool.call.completed") continue;
    const detail = asJson(entry.detail);
    const name = String(detail.name ?? "");
    const result = asJson(detail.result);
    const content = asJson(result.content);
    if (name === "procurement.price.dataset.profile") profile = content;
    if (name === "procurement.price.dataset.quality.check") quality = content;
    if (
      name.startsWith("procurement.price.metric.") &&
      name.endsWith(".compute")
    ) {
      const metric = asJson(content.metric);
      const key = String(metric.key ?? "");
      if (key) metricResults.set(key, content);
    }
    if (name === "procurement.price.metric.evidence.list") evidence.push(content);
  }
  if (!profile && metricResults.size === 0) return null;

  const kpis = [...metricResults.values()].map((item) => asJson(item.metric));
  const impactByAnchor: Json = {};
  for (const item of metricResults.values()) {
    const metric = asJson(item.metric);
    const context = asJson(item.context);
    const metricKey = String(metric.key ?? "");
    const targetAnchor = String(context.anchor ?? "");
    if (!targetAnchor) continue;
    const current = asJson(impactByAnchor[targetAnchor]);
    if (metricKey === "impact_amount") current.total_pos_amount = metric.value;
    if (metricKey === "impact_neg_amount") current.total_neg_amount = metric.value;
    if (metricKey === "impact_share_pct") current.share_pct = metric.value;
    if (metricKey === "impact_neg_share_pct") current.neg_share_pct = metric.value;
    current.line_cnt = context.line_count;
    impactByAnchor[targetAnchor] = current;
  }
  const marketMetric = asJson(metricResults.get("market_dev_pct")?.context);
  const marketEvidence = evidence.find(
    (item) => String(item.metric_key ?? "") === "market_dev_pct",
  );
  return {
    filter: {
      ...asJson(profile?.filter),
      records: profile?.records,
      comparisons: profile?.comparisons,
      source_revision:
        profile?.source_revision ??
        metricResults.values().next().value?.source_revision,
    },
    kpis,
    data_quality: quality ?? {},
    analytics: {
      price_compare_3d: {
        market: {
          hit_cnt: marketMetric.matched_line_count,
          top_materials: asList(marketEvidence?.rows),
        },
      },
      price_impact: { anchors: impactByAnchor },
    },
    evidence,
  };
}

function metricValue(snapshot: Json | null): Kpi[] {
  if (!snapshot) return [];
  return asList(snapshot.kpis).map((item) => ({
    key: String(item.key ?? ""),
    label: String(item.label ?? item.key ?? ""),
    value:
      typeof item.value === "number" || typeof item.value === "string"
        ? item.value
        : "—",
    suffix: String(item.suffix ?? ""),
  }));
}

function compactId(value: string): string {
  if (value.length <= 24) return value;
  return `${value.slice(0, 12)}…${value.slice(-8)}`;
}

export function PriceInsightLab() {
  const [apiBase, setApiBase] = useState(defaultApiBase);
  const [tenant, setTenant] = useState("development");
  const actor = "price-insight-debugger";
  const [periodFrom, setPeriodFrom] = useState("2026-01");
  const [periodTo, setPeriodTo] = useState("2026-02");
  const [anchor, setAnchor] = useState("market");
  const [threshold, setThreshold] = useState("8");
  const [health, setHealth] = useState("unknown");
  const [sessionId, setSessionId] = useState("");
  const [task, setTask] = useState<Json | null>(null);
  const [result, setResult] = useState<Json | null>(null);
  const [timeline, setTimeline] = useState<Json[]>([]);
  const [snapshot, setSnapshot] = useState<Json | null>(null);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");

  const request = useCallback(
    async (path: string, init: RequestInit = {}) => {
      const response = await fetch(
        `${apiBase.replace(/\/+$/, "")}${path}`,
        {
          ...init,
          headers: {
            "Content-Type": "application/json",
            "X-Tenant-ID": tenant,
            "X-Actor-ID": actor,
            ...(init.headers ?? {}),
          },
        },
      );
      const text = await response.text();
      let payload: unknown = {};
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch {
          payload = { detail: text };
        }
      }
      if (!response.ok && response.status !== 202) {
        throw new Error(
          String(asJson(payload).detail ?? `HTTP ${response.status}`),
        );
      }
      return asJson(payload);
    },
    [actor, apiBase, tenant],
  );

  const refresh = useCallback(
    async (targetSessionId: string) => {
      const encoded = encodeURIComponent(targetSessionId);
      const [nextTask, nextResult, nextTimeline] = await Promise.all([
        request(`/v1/tasks/${encoded}`),
        request(`/v1/tasks/${encoded}/result`),
        request(`/v1/operations/sessions/${encoded}/timeline`),
      ]);
      const entries = asList(nextTimeline.entries);
      setTask(nextTask);
      setResult(nextResult);
      setTimeline(entries);
      setSnapshot(extractSnapshot(entries));
      setError("");
      return String(nextTask.run_status ?? nextResult.status ?? "");
    },
    [request],
  );

  useEffect(() => {
    if (!sessionId) return;
    const runStatus = String(task?.run_status ?? "");
    if (TERMINAL.has(runStatus)) return;
    const timer = window.setInterval(() => {
      void refresh(sessionId).catch((reason: unknown) => {
        setError(reason instanceof Error ? reason.message : "刷新失败");
      });
    }, 1600);
    return () => window.clearInterval(timer);
  }, [refresh, sessionId, task?.run_status]);

  const checkHealth = async () => {
    setBusy("health");
    setError("");
    try {
      const payload = await request("/health/ready");
      setHealth(String(payload.status ?? "unknown"));
    } catch (reason) {
      setHealth("failed");
      setError(reason instanceof Error ? reason.message : "健康检查失败");
    } finally {
      setBusy("");
    }
  };

  const startRun = async () => {
    setBusy("create");
    setError("");
    setSessionId("");
    setTask(null);
    setResult(null);
    setTimeline([]);
    setSnapshot(null);
    try {
      const created = await request("/v1/tasks", {
        method: "POST",
        headers: {
          "Idempotency-Key": `price-insight-${crypto.randomUUID()}`,
        },
        body: JSON.stringify({
          goal: taskGoal(periodFrom, periodTo, anchor, threshold),
        }),
      });
      const id = String(created.session_id ?? "");
      if (!id) throw new Error("创建响应缺少 session_id");
      setSessionId(id);
      await refresh(id);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "任务创建失败");
    } finally {
      setBusy("");
    }
  };

  const kpis = useMemo(() => metricValue(snapshot), [snapshot]);
  const snapshotFilter = asJson(snapshot?.filter);
  const dataQuality = asJson(snapshot?.data_quality);
  const analytics = asJson(snapshot?.analytics);
  const compare3d = asJson(analytics.price_compare_3d);
  const impact = asJson(analytics.price_impact);
  const impactAnchors = asJson(impact.anchors);
  const selectedImpact = asJson(impactAnchors[anchor]);
  const market = asJson(compare3d.market);
  const topMaterials = asList(market.top_materials);
  const timelineText = JSON.stringify(timeline);
  const steps = STEP_DEFINITIONS.map(([label, marker]) => ({
    label,
    marker,
    done: timelineText.includes(marker),
  }));
  const runStatus = String(task?.run_status ?? result?.status ?? "idle");
  const sourceRevision = String(snapshotFilter.source_revision ?? "");
  const qualityStatus = String(dataQuality.status ?? "unknown");

  return (
    <main className={styles.shell}>
      <header className={styles.topbar}>
        <Link href="/" className={styles.brand}>
          <span>AC</span>
          <div>
            <strong>AuraClaw</strong>
            <small>Price Insight Control Tower</small>
          </div>
        </Link>
        <div className={styles.path}>
          全场景中心 <b>→</b> 成本 <b>→</b> 价格管理控制塔 <b>→</b>{" "}
          <strong>价格洞察智能体</strong>
        </div>
        <Link href="/model-skills" className={styles.secondaryLink}>
          Skill Lab
        </Link>
      </header>

      <section className={styles.hero}>
        <div>
          <p className={styles.eyebrow}>PROCUREMENT · PRICE INTELLIGENCE</p>
          <h1>价格洞察智能体</h1>
          <p>
            用真实 MySQL DWD 验证 Skill → Resource → Tool → Agent Loop，
            并直接查看八项指标、证据和数据质量。
          </p>
        </div>
        <div className={styles.sourceCard}>
          <span className={sourceRevision.startsWith("mysql-") ? styles.live : ""} />
          <div>
            <small>DATA SOURCE</small>
            <strong>
              {sourceRevision
                ? sourceRevision.startsWith("mysql-")
                  ? "MYSQL DWD"
                  : "NON-MYSQL"
                : "WAITING"}
            </strong>
            <code>{sourceRevision || "尚未执行范围画像"}</code>
          </div>
        </div>
      </section>

      <section className={styles.controlPanel}>
        <label>
          <span>API BASE</span>
          <input value={apiBase} onChange={(event) => setApiBase(event.target.value)} />
        </label>
        <label>
          <span>TENANT</span>
          <input value={tenant} onChange={(event) => setTenant(event.target.value)} />
        </label>
        <label>
          <span>起始月份</span>
          <input type="month" value={periodFrom} onChange={(event) => setPeriodFrom(event.target.value)} />
        </label>
        <label>
          <span>结束月份</span>
          <input type="month" value={periodTo} onChange={(event) => setPeriodTo(event.target.value)} />
        </label>
        <label>
          <span>对标锚点</span>
          <select value={anchor} onChange={(event) => setAnchor(event.target.value)}>
            <option value="market">市场</option>
            <option value="history">历史</option>
            <option value="region">区域</option>
          </select>
        </label>
        <label>
          <span>偏离阈值 %</span>
          <input type="number" min="0" max="1000" value={threshold} onChange={(event) => setThreshold(event.target.value)} />
        </label>
        <div className={styles.actions}>
          <button type="button" onClick={() => void checkHealth()} disabled={Boolean(busy)}>
            {busy === "health" ? "检查中…" : `后端 ${health}`}
          </button>
          <button className={styles.primary} type="button" onClick={() => void startRun()} disabled={Boolean(busy)}>
            {busy === "create" ? "创建中…" : "运行价格洞察"}
          </button>
        </div>
      </section>

      {error && <div className={styles.error}>{error}</div>}

      <section className={styles.runStrip}>
        <div>
          <small>RUN STATUS</small>
          <strong data-tone={TERMINAL.has(runStatus) ? "terminal" : "active"}>
            {runStatus}
          </strong>
        </div>
        <div>
          <small>SESSION</small>
          <code title={sessionId}>{sessionId ? compactId(sessionId) : "—"}</code>
        </div>
        <div>
          <small>QUALITY</small>
          <strong data-tone={qualityStatus}>{qualityStatus}</strong>
        </div>
        <button type="button" disabled={!sessionId || Boolean(busy)} onClick={() => void refresh(sessionId)}>
          刷新证据
        </button>
      </section>

      <section className={styles.steps} aria-label="Agent Loop 执行步骤">
        {steps.map((step, index) => (
          <div className={step.done ? styles.stepDone : ""} key={step.marker}>
            <span>{String(index + 1).padStart(2, "0")}</span>
            <div>
              <strong>{step.label}</strong>
              <small>{step.done ? "COMPLETED" : "WAITING"}</small>
            </div>
          </div>
        ))}
      </section>

      <section className={styles.kpiSection}>
        <div className={styles.sectionHeading}>
          <div>
            <p className={styles.eyebrow}>KEY METRICS</p>
            <h2>价格洞察关键指标</h2>
          </div>
          <span>{kpis.length ? `${kpis.length} / 8 已生成` : "等待原子指标计算"}</span>
        </div>
        <div className={styles.kpiGrid}>
          {(kpis.length ? kpis : Array.from({ length: 8 }, (_, index) => ({
            key: `pending-${index}`,
            label: "等待计算",
            value: "—",
            suffix: "",
          }))).map((kpi) => (
            <article key={kpi.key}>
              <small>{kpi.key}</small>
              <strong>
                {kpi.value}
                <em>{kpi.suffix}</em>
              </strong>
              <p>{kpi.label}</p>
            </article>
          ))}
        </div>
      </section>

      <section className={styles.detailGrid}>
        <article className={styles.panel}>
          <div className={styles.panelHeading}>
            <div>
              <small>MARKET BENCHMARK</small>
              <h3>市场偏离物料</h3>
            </div>
            <span>{String(market.hit_cnt ?? 0)} matched</span>
          </div>
          {topMaterials.length ? (
            <div className={styles.table}>
              <div className={styles.tableHead}>
                <span>物料</span><span>成交均价</span><span>行业基准</span><span>偏离</span>
              </div>
              {topMaterials.map((row) => (
                <div key={String(row.material_code ?? row.material_name)}>
                  <strong>{String(row.material_name ?? "—")}</strong>
                  <span>{String(row.avg_price ?? "—")}</span>
                  <span>{String(row.benchmark ?? "—")}</span>
                  <span>{String(row.deviation_pct ?? "—")}%</span>
                </div>
              ))}
            </div>
          ) : (
            <p className={styles.empty}>运行完成后显示行业基准对比。</p>
          )}
        </article>

        <article className={styles.panel}>
          <div className={styles.panelHeading}>
            <div>
              <small>PRICE IMPACT</small>
              <h3>{anchor.toUpperCase()} 价格影响</h3>
            </div>
            <span>{String(selectedImpact.line_cnt ?? 0)} lines</span>
          </div>
          <dl className={styles.impactList}>
            <div><dt>正偏移金额</dt><dd>{String(selectedImpact.total_pos_amount ?? "—")}</dd></div>
            <div><dt>负偏移金额</dt><dd>{String(selectedImpact.total_neg_amount ?? "—")}</dd></div>
            <div><dt>正偏移占比</dt><dd>{String(selectedImpact.share_pct ?? "—")}%</dd></div>
            <div><dt>负偏移占比</dt><dd>{String(selectedImpact.neg_share_pct ?? "—")}%</dd></div>
          </dl>
        </article>

        <article className={styles.panel}>
          <div className={styles.panelHeading}>
            <div>
              <small>DATA QUALITY</small>
              <h3>口径与证据</h3>
            </div>
            <span>{String(dataQuality.finding_count ?? 0)} findings</span>
          </div>
          <dl className={styles.evidenceList}>
            <div><dt>状态</dt><dd>{qualityStatus}</dd></div>
            <div><dt>记录数</dt><dd>{String(snapshotFilter.records ?? "—")}</dd></div>
            <div><dt>比对数</dt><dd>{String(snapshotFilter.comparisons ?? "—")}</dd></div>
            <div><dt>主导维度</dt><dd>{String(compare3d.dominant_dimension ?? "—")}</dd></div>
            <div><dt>规则版本</dt><dd>{String(snapshotFilter.rule_version ?? "默认")}</dd></div>
          </dl>
        </article>
      </section>

      <section className={styles.rawPanel}>
        <details open={Boolean(snapshot)}>
          <summary>原子分析聚合 JSON</summary>
          <pre>{snapshot ? JSON.stringify(snapshot, null, 2) : "尚无原子结果"}</pre>
        </details>
        <details>
          <summary>Agent 最终结果</summary>
          <pre>{result ? JSON.stringify(result, null, 2) : "尚无结果"}</pre>
        </details>
        <details>
          <summary>Timeline 证据</summary>
          <pre>{timeline.length ? JSON.stringify(timeline, null, 2) : "尚无事件"}</pre>
        </details>
      </section>
    </main>
  );
}
