"use client";

import { useMemo, useState } from "react";
import Link from "next/link";

type Json = Record<string, unknown>;
type TestMode = "demo" | "live";
type StepState = "idle" | "running" | "passed" | "failed";
type Step = {
  id: "config" | "skill" | "mcp" | "agent";
  label: string;
  detail: string;
  state: StepState;
};
type RequestLog = {
  id: number;
  method: string;
  status: "passed" | "failed";
  duration: number;
  detail: string;
};
type LoadedSkill = {
  id: string;
  name: string;
  title: string;
  version: string;
  prefix: string;
  manifest: Json;
  instructions: string;
  config: Json;
  resources: string[];
};

const DEMO_SKILLS: LoadedSkill[] = [
  {
    id: "supplier-risk-warning",
    name: "model.supplier-risk-warning",
    title: "供应风险预警模型",
    version: "1.0.0-draft.1",
    prefix: "skill://ct-model/model.supplier-risk-warning/1.0.0-draft.1",
    manifest: {
      name: "model.supplier-risk-warning",
      version: "1.0.0-draft.1",
      description: "供应风险预警模型：基于综合评分与重大异常约束形成供应风险预警。",
      publisher: "ct-model",
      risk_level: "medium",
      required_tools: [],
      data_classification: "internal",
      input_schema: {
        type: "object",
        properties: {
          summary_score: { type: "number", title: "综合评分" },
          constraint_anomalies: { type: "number", title: "重大异常约束" },
        },
      },
    },
    instructions: `# 供应风险预警模型

## 用途

这是由模型配置自动生成的预览草稿 Skill，适用于 SUPPLIER 对象。

## 当前限制

- 本阶段只贯通配置到 Agent 的加载流程，不提供权威计算或业务回写。
- 不执行自然语言公式，不自行补全缺失规则。`,
    config: {
      tenant_id: "1",
      model: {
        model_code: "SUPPLIER_RISK_WARNING",
        model_name: "供应风险预警模型",
        model_type: "RISK_WARNING",
        target_type: "SUPPLIER",
      },
      version: { id: 1, version_no: "1.0.0", status: "DRAFT" },
      sections: {
        input_features: [
          { feature_code: "summary_score", source_type: "MODEL_OUTPUT" },
          { feature_code: "constraint_anomalies", source_type: "VIEW" },
        ],
        weights: [
          { feature_code: "summary_score", weight_value: "0.700000" },
          { feature_code: "constraint_anomalies", weight_value: "0.300000" },
        ],
      },
      source_digest: "sha256:preview-risk-warning",
    },
    resources: ["manifest", "SKILL.md", "references/config.json", "references/model.md"],
  },
  {
    id: "supplier-score",
    name: "model.supplier-score",
    title: "供应商综合评分模型",
    version: "1.0.0-draft.2",
    prefix: "skill://ct-model/model.supplier-score/1.0.0-draft.2",
    manifest: {
      name: "model.supplier-score",
      version: "1.0.0-draft.2",
      description: "供应商综合评分模型：综合五类指标形成供应商评分。",
      publisher: "ct-model",
      risk_level: "medium",
      required_tools: [],
      data_classification: "internal",
      input_schema: {
        type: "object",
        properties: {
          quality_score: { type: "number", title: "质量表现" },
          delivery_score: { type: "number", title: "履约表现" },
          price_score: { type: "number", title: "价格表现" },
          external_risk_score: { type: "number", title: "外部风险" },
          cooperation_stability_score: { type: "number", title: "合作稳定性" },
        },
      },
    },
    instructions: `# 供应商综合评分模型

## 执行流程

1. 读取 references/config.json 并确认 source digest。
2. 只使用 input_schema 声明的字段收集上下文。
3. 根据权重、阈值、标签和量化说明解释模型配置。

## 当前限制

需要权威结果时应明确告知用户当前仅能解释配置并停止。`,
    config: {
      tenant_id: "1",
      model: {
        model_code: "supplier_score",
        model_name: "供应商综合评分模型",
        model_type: "SCORE",
        target_type: "SUPPLIER",
      },
      version: { id: 2, version_no: "1.0.0", status: "DRAFT" },
      sections: {
        weights: [
          { feature_code: "quality_score", weight_value: "0.240000" },
          { feature_code: "delivery_score", weight_value: "0.220000" },
          { feature_code: "price_score", weight_value: "0.180000" },
          { feature_code: "external_risk_score", weight_value: "0.260000" },
          { feature_code: "cooperation_stability_score", weight_value: "0.100000" },
        ],
        thresholds: [
          { threshold_code: "grade_A", min_value: "85", output_value: "A" },
          { threshold_code: "grade_B", min_value: "70", max_value: "85", output_value: "B" },
        ],
      },
      source_digest: "sha256:preview-supplier-score",
    },
    resources: ["manifest", "SKILL.md", "references/config.json", "references/model.md"],
  },
];

const INITIAL_STEPS: Step[] = [
  { id: "config", label: "CONFIG", detail: "ct_model_* source", state: "passed" },
  { id: "skill", label: "SKILL", detail: "signed preview package", state: "passed" },
  { id: "mcp", label: "MCP", detail: "skill:// resources", state: "passed" },
  { id: "agent", label: "AGENT", detail: "manifest + context loaded", state: "passed" },
];

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

function decodeContent(content: Json) {
  if (typeof content.text === "string") return content.text;
  if (typeof content.blob === "string") {
    return decodeURIComponent(
      Array.from(atob(content.blob))
        .map((character) => `%${character.charCodeAt(0).toString(16).padStart(2, "0")}`)
        .join(""),
    );
  }
  throw new Error("Resource 没有可读取的 text/blob 内容");
}

function stateLabel(state: StepState) {
  if (state === "passed") return "PASS";
  if (state === "failed") return "FAIL";
  if (state === "running") return "RUN";
  return "WAIT";
}

export function ModelSkillLab() {
  const [mode, setMode] = useState<TestMode>("demo");
  const [endpoint, setEndpoint] = useState("/auraclaw-mcp");
  const [token, setToken] = useState("");
  const [skills, setSkills] = useState<LoadedSkill[]>(DEMO_SKILLS);
  const [selectedId, setSelectedId] = useState(DEMO_SKILLS[0].id);
  const [activeTab, setActiveTab] = useState<"manifest" | "instructions" | "config">("manifest");
  const [steps, setSteps] = useState<Step[]>(INITIAL_STEPS);
  const [logs, setLogs] = useState<RequestLog[]>([
    { id: 1, method: "initialize", status: "passed", duration: 12, detail: "MCP 2025-11-25" },
    { id: 2, method: "resources/list", status: "passed", duration: 18, detail: "8 Model Skill resources" },
    { id: 3, method: "resources/read", status: "passed", duration: 9, detail: "manifest + SKILL.md + config" },
  ]);
  const [notice, setNotice] = useState("演示数据已就绪，可切换到 Live MCP 运行真实测试。");
  const [busy, setBusy] = useState(false);
  const [query, setQuery] = useState("");
  const [copied, setCopied] = useState(false);

  const selected = skills.find((skill) => skill.id === selectedId) ?? skills[0];
  const visibleSkills = useMemo(
    () =>
      skills.filter((skill) =>
        `${skill.name} ${skill.title} ${skill.version}`.toLowerCase().includes(query.toLowerCase()),
      ),
    [query, skills],
  );

  const resetDemo = () => {
    setMode("demo");
    setSkills(DEMO_SKILLS);
    setSelectedId(DEMO_SKILLS[0].id);
    setSteps(INITIAL_STEPS);
    setLogs([
      { id: 1, method: "initialize", status: "passed", duration: 12, detail: "MCP 2025-11-25" },
      { id: 2, method: "resources/list", status: "passed", duration: 18, detail: "8 Model Skill resources" },
      { id: 3, method: "resources/read", status: "passed", duration: 9, detail: "manifest + SKILL.md + config" },
    ]);
    setNotice("演示数据已恢复。");
  };

  const updateStep = (id: Step["id"], state: StepState, detail?: string) => {
    setSteps((current) =>
      current.map((step) => (step.id === id ? { ...step, state, detail: detail ?? step.detail } : step)),
    );
  };

  const runLiveTest = async () => {
    setBusy(true);
    setNotice("正在连接 Action Hands MCP…");
    setLogs([]);
    setSteps(INITIAL_STEPS.map((step) => ({ ...step, state: "idle" })));
    let requestId = 0;

    const call = async (method: string, params: Json = {}) => {
      const started = performance.now();
      const id = ++requestId;
      try {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json, text/event-stream",
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          body: JSON.stringify({ jsonrpc: "2.0", id, method, params }),
        });
        const payload = (await response.json()) as Json;
        const error = payload.error as Json | undefined;
        if (!response.ok || error) {
          throw new Error(String(error?.message ?? `HTTP ${response.status}`));
        }
        setLogs((current) => [
          ...current,
          {
            id,
            method,
            status: "passed",
            duration: Math.round(performance.now() - started),
            detail: method === "resources/read" ? String(params.uri ?? "") : "response accepted",
          },
        ]);
        return (payload.result ?? {}) as Json;
      } catch (error) {
        setLogs((current) => [
          ...current,
          {
            id,
            method,
            status: "failed",
            duration: Math.round(performance.now() - started),
            detail: error instanceof Error ? error.message : "request failed",
          },
        ]);
        throw error;
      }
    };

    const readText = async (uri: string) => {
      const result = await call("resources/read", { uri });
      const contents = Array.isArray(result.contents) ? (result.contents as Json[]) : [];
      if (!contents.length) throw new Error(`Resource 为空：${uri}`);
      return decodeContent(contents[0]);
    };

    try {
      updateStep("config", "running", "connecting to source-backed MCP");
      const initialized = await call("initialize", { protocolVersion: "2025-11-25" });
      updateStep("config", "passed", `protocol ${String(initialized.protocolVersion ?? "ready")}`);

      updateStep("skill", "running", "discovering generated packages");
      const listed = await call("resources/list");
      const resources = Array.isArray(listed.resources) ? (listed.resources as Json[]) : [];
      const modelUris = resources
        .map((resource) => String(resource.uri ?? ""))
        .filter((uri) => uri.startsWith("skill://ct-model/"));
      const manifestUris = modelUris.filter((uri) => uri.endsWith("/manifest"));
      if (!manifestUris.length) throw new Error("没有发现 skill://ct-model/.../manifest");
      updateStep("skill", "passed", `${manifestUris.length} generated packages`);

      updateStep("mcp", "running", "reading package resources");
      const loaded: LoadedSkill[] = [];
      for (const manifestUri of manifestUris) {
        const prefix = manifestUri.slice(0, -"/manifest".length);
        const [manifestText, instructions, configText] = await Promise.all([
          readText(manifestUri),
          readText(`${prefix}/SKILL.md`),
          readText(`${prefix}/references/config.json`),
        ]);
        const manifest = JSON.parse(manifestText) as Json;
        const config = JSON.parse(configText) as Json;
        const name = String(manifest.name ?? prefix.split("/").at(-2) ?? "model.unknown");
        loaded.push({
          id: `${name}-${String(manifest.version ?? "")}`,
          name,
          title: String(manifest.description ?? name).split("：")[0],
          version: String(manifest.version ?? "unknown"),
          prefix,
          manifest,
          instructions,
          config,
          resources: modelUris
            .filter((uri) => uri.startsWith(`${prefix}/`))
            .map((uri) => uri.slice(prefix.length + 1)),
        });
      }
      updateStep("mcp", "passed", `${modelUris.length} resources loaded`);

      updateStep("agent", "running", "validating agent context");
      if (loaded.some((skill) => !skill.instructions || !skill.manifest.name || !skill.config.source_digest)) {
        throw new Error("Agent 上下文缺少 manifest、instructions 或 source digest");
      }
      setSkills(loaded);
      setSelectedId(loaded[0].id);
      setMode("live");
      updateStep("agent", "passed", "manifest + instructions + config");
      setNotice(`真实链路通过：${loaded.length} 个 Skill 已加载到 Agent 测试上下文。`);
    } catch (error) {
      setSteps((current) =>
        current.map((step) =>
          step.state === "running"
            ? { ...step, state: "failed", detail: error instanceof Error ? error.message : "test failed" }
            : step,
        ),
      );
      setNotice(`真实测试失败：${error instanceof Error ? error.message : "未知错误"}`);
    } finally {
      setBusy(false);
    }
  };

  const displayed =
    activeTab === "manifest"
      ? formatJson(selected?.manifest ?? {})
      : activeTab === "config"
        ? formatJson(selected?.config ?? {})
        : selected?.instructions ?? "";

  const copyDisplayed = async () => {
    await navigator.clipboard.writeText(displayed);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  };

  return (
    <main className="skill-lab-shell">
      <header className="topbar skill-lab-topbar">
        <Link className="brand" href="/">
          <span className="brand-mark">AC</span>
          <div><strong>AuraClaw</strong><small>model skill delivery lab</small></div>
        </Link>
        <div className="top-status">
          <span className={`signal ${steps.every((step) => step.state === "passed") ? "online" : ""}`} />
          <span>{mode === "live" ? "Live MCP" : "Demo snapshot"}</span>
          <span className="divider" />
          <code>M10 / PREVIEW</code>
        </div>
      </header>

      <section className="skill-lab-hero">
        <div>
          <p className="eyebrow">Vertical slice test bench</p>
          <h1>配置如何抵达 Agent</h1>
          <p>验证 MySQL 模型配置被编译成 Skill，经 MCP Resource 暴露，并成为 Agent 可读取的上下文。</p>
        </div>
        <div className="mode-switch" role="group" aria-label="测试模式">
          <button className={mode === "demo" ? "active" : ""} onClick={resetDemo}>演示数据</button>
          <button className={mode === "live" ? "active" : ""} onClick={() => setMode("live")}>Live MCP</button>
        </div>
      </section>

      <section className="delivery-flow" aria-label="配置到 Agent 流程">
        {steps.map((step, index) => (
          <div className="flow-segment" key={step.id}>
            <article className={`flow-node ${step.state}`}>
              <div><span>{String(index + 1).padStart(2, "0")}</span><strong>{step.label}</strong></div>
              <p>{step.detail}</p>
              <small>{stateLabel(step.state)}</small>
            </article>
            {index < steps.length - 1 && <span className="flow-arrow" aria-hidden="true">→</span>}
          </div>
        ))}
      </section>

      <section className="mcp-connection-card">
        <div className="connection-copy">
          <span className={`health-dot ${mode === "live" ? "good" : "warn"}`} />
          <div><strong>{mode === "live" ? "连接真实 Action Hands MCP" : "当前使用内置冒烟快照"}</strong><p>{notice}</p></div>
        </div>
        <label className="field"><span>MCP endpoint</span><input value={endpoint} onChange={(event) => setEndpoint(event.target.value)} disabled={busy} /></label>
        <label className="field"><span>Bearer token</span><input type="password" value={token} onChange={(event) => setToken(event.target.value)} placeholder="仅本次请求使用" disabled={busy} /></label>
        <button className="button primary" onClick={() => void runLiveTest()} disabled={busy || !endpoint.trim()}>{busy ? "测试中…" : "运行真实测试"}</button>
      </section>

      <section className="skill-lab-grid">
        <aside className="skill-catalog">
          <div className="catalog-heading"><div><p className="eyebrow">Discovered packages</p><h2>Model Skills</h2></div><span>{skills.length}</span></div>
          <label className="skill-search"><span aria-hidden="true">⌕</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="筛选模型或版本" /></label>
          <div className="skill-list">
            {visibleSkills.map((skill) => (
              <button className={selected?.id === skill.id ? "active" : ""} key={skill.id} onClick={() => setSelectedId(skill.id)}>
                <span className="skill-type">MODEL</span>
                <strong>{skill.title}</strong>
                <code>{skill.name}</code>
                <footer><span>{skill.version}</span><span>{skill.resources.length} resources</span></footer>
              </button>
            ))}
          </div>
        </aside>

        <section className="skill-inspector">
          {selected ? (
            <>
              <header className="inspector-heading">
                <div><p className="eyebrow">Selected Skill</p><h2>{selected.title}</h2><code>{selected.prefix}</code></div>
                <div><span className="pill warn">DRAFT PREVIEW</span><span className="pill good">SIGNED</span></div>
              </header>
              <div className="skill-facts">
                <div><small>Publisher</small><strong>{String(selected.manifest.publisher ?? "ct-model")}</strong></div>
                <div><small>Version</small><strong>{selected.version}</strong></div>
                <div><small>Risk</small><strong>{String(selected.manifest.risk_level ?? "medium")}</strong></div>
                <div><small>Tools</small><strong>{Array.isArray(selected.manifest.required_tools) ? selected.manifest.required_tools.length : 0}</strong></div>
              </div>
              <nav className="resource-tabs" aria-label="Skill resources">
                <button className={activeTab === "manifest" ? "active" : ""} onClick={() => setActiveTab("manifest")}>manifest.json</button>
                <button className={activeTab === "instructions" ? "active" : ""} onClick={() => setActiveTab("instructions")}>SKILL.md</button>
                <button className={activeTab === "config" ? "active" : ""} onClick={() => setActiveTab("config")}>config.json</button>
                <button className="copy-resource" onClick={() => void copyDisplayed()}>{copied ? "已复制" : "复制"}</button>
              </nav>
              <pre className={`resource-viewer ${activeTab === "instructions" ? "markdown" : ""}`}>{displayed}</pre>
            </>
          ) : (
            <div className="empty-state"><span className="empty-mark">···</span><strong>没有匹配的 Skill</strong><p>清除筛选条件或运行真实测试。</p></div>
          )}
        </section>

        <aside className="test-evidence">
          <div className="subpanel-title"><h2>测试证据</h2><span>{steps.filter((step) => step.state === "passed").length}/{steps.length}</span></div>
          <div className="assertion-list">
            {steps.map((step) => (
              <div key={step.id}><span className={`assertion-mark ${step.state}`}>{step.state === "passed" ? "✓" : step.state === "failed" ? "×" : "·"}</span><div><strong>{step.label}</strong><p>{step.detail}</p></div></div>
            ))}
          </div>
          <div className="subpanel-title request-title"><h2>JSON-RPC</h2><span>{logs.length} calls</span></div>
          <div className="request-log">
            {logs.map((log) => (
              <div key={`${log.id}-${log.method}`}>
                <span className={log.status}>{log.status === "passed" ? "200" : "ERR"}</span>
                <div><strong>{log.method}</strong><p>{log.detail}</p></div>
                <time>{log.duration}ms</time>
              </div>
            ))}
          </div>
          <div className="safety-note"><span>!</span><div><strong>预览边界</strong><p>页面只验证上下文交付，不执行自然语言公式，也不触发模型回写。</p></div></div>
        </aside>
      </section>
    </main>
  );
}
