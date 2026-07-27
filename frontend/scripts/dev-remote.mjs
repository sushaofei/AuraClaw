#!/usr/bin/env node
/**
 * Start the protocol console against a remote AuraClaw ingress.
 * Reads AURACLAW_HOST from ../.host.env (default port 8080).
 * Override with AURACLAW_DEV_API_TARGET if set.
 */
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const frontendRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const hostEnvPath = resolve(frontendRoot, "../.host.env");

function readHostEnv(path) {
  try {
    const text = readFileSync(path, "utf8");
    const out = {};
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq <= 0) continue;
      out[trimmed.slice(0, eq).trim()] = trimmed.slice(eq + 1).trim();
    }
    return out;
  } catch {
    return {};
  }
}

const hostEnv = readHostEnv(hostEnvPath);
const host = hostEnv.AURACLAW_HOST || "10.244.16.131";
const port = process.env.AURACLAW_INGRESS_PORT || "8080";
const target =
  process.env.AURACLAW_DEV_API_TARGET || `http://${host}:${port}`;

const noProxyParts = new Set(
  `${process.env.NO_PROXY || ""},${process.env.no_proxy || ""}`
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean),
);
for (const part of [host, "127.0.0.1", "localhost", "::1"]) {
  noProxyParts.add(part);
}
const noProxy = [...noProxyParts].join(",");

const args = process.argv.slice(2);
const npmArgs = ["run", "dev", "--", ...args];
if (!args.includes("--port") && !args.some((a) => a.startsWith("--port="))) {
  npmArgs.push("--port", "3000");
}

console.log(`[dev:remote] proxy /auraclaw-api -> ${target}`);
console.log(`[dev:remote] page API endpoint: http://localhost:3000/auraclaw-api`);

const child = spawn("npm", npmArgs, {
  cwd: frontendRoot,
  stdio: "inherit",
  env: {
    ...process.env,
    AURACLAW_DEV_API_TARGET: target,
    NO_PROXY: noProxy,
    no_proxy: noProxy,
  },
});

child.on("exit", (code, signal) => {
  if (signal) process.kill(process.pid, signal);
  process.exit(code ?? 1);
});
