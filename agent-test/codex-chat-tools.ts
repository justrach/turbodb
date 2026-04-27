#!/usr/bin/env bun
// Agent Slack: Codex-subagent-style coordination test using real MCP tools.
//
// This is intentionally separate from chat.ts, which uses the Claude Agent SDK.
// Here each phase is a fresh `codex exec` process with only the local Agent Slack MCP
// server configured. The MCP server stamps the agent id and writes every tool
// call to an audit log, so verification can distinguish tool-mediated coord
// from shell/curl writes.

import { spawn, type ChildProcess } from "node:child_process";
import { appendFile, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

const REPO_ROOT = resolve(import.meta.dir, "..");
const HARNESS_DIR = import.meta.dir;
const TURBODB_BIN = resolve(REPO_ROOT, "zig-out/bin/turbodb");
const MCP_AGENT_SLACK = resolve(HARNESS_DIR, "mcp-chat.ts");
const CODEX_BIN = process.env.CODEX_BIN ?? "codex";
const DATA_DIR = resolve(HARNESS_DIR, "agent-test-data-agent-slack");
const LOG_FILE = resolve(HARNESS_DIR, "turbodb-agent-slack.log");
const CODEX_LOG_FILE = resolve(HARNESS_DIR, "agent-slack.log");
const AUDIT_FILE = resolve(HARNESS_DIR, "agent-slack.audit.ndjson");
const WIRE_PORT = 27317;
const HTTP_PORT = WIRE_PORT + 1;
const BASE = `http://localhost:${HTTP_PORT}`;
const READY_TIMEOUT_MS = 10_000;
const PHASE_TIMEOUT_MS = Number(process.env.PHASE_TIMEOUT_MS ?? 240_000);

const ARCHITECT = "architect";
const IMPLEMENTER = "implementer";
const REVIEWER = "reviewer";
const AGENT_IDS = [ARCHITECT, IMPLEMENTER, REVIEWER] as const;

type CheckResult = { ok: boolean; label: string; detail?: string };
const checks: CheckResult[] = [];

function pass(label: string, detail?: string) {
  checks.push({ ok: true, label, detail });
  console.log(`[PASS] ${label}${detail ? `  ${detail}` : ""}`);
}

function fail(label: string, detail?: string) {
  checks.push({ ok: false, label, detail });
  console.log(`[FAIL] ${label}${detail ? `  ${detail}` : ""}`);
}

function info(label: string, detail?: string) {
  console.log(`[INFO] ${label}${detail ? `  ${detail}` : ""}`);
}

async function startTurbodb(): Promise<ChildProcess> {
  await rm(DATA_DIR, { recursive: true, force: true });
  await mkdir(DATA_DIR, { recursive: true });
  await mkdir(dirname(LOG_FILE), { recursive: true });
  await writeFile(LOG_FILE, "");
  await writeFile(CODEX_LOG_FILE, "");
  await rm(AUDIT_FILE, { force: true });

  const child = spawn(
    TURBODB_BIN,
    ["--port", String(WIRE_PORT), "--both", "--data", DATA_DIR],
    { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] },
  );

  await appendFile(LOG_FILE, `--- turbodb [fresh] pid=${child.pid} ${new Date().toISOString()} ---\n`);
  child.stdout?.on("data", (b) => { void appendFile(LOG_FILE, b); });
  child.stderr?.on("data", (b) => { void appendFile(LOG_FILE, b); });

  const deadline = Date.now() + READY_TIMEOUT_MS;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) {
        await r.text();
        return child;
      }
    } catch (e) {
      lastErr = e;
    }
    await sleep(100);
  }
  child.kill("SIGKILL");
  throw new Error(`turbodb did not become ready in ${READY_TIMEOUT_MS}ms (last err: ${lastErr})`);
}

async function stopTurbodb(child: ChildProcess): Promise<void> {
  if (child.killed || child.exitCode !== null) return;
  await new Promise<void>((res) => {
    child.once("exit", () => res());
    child.kill("SIGTERM");
    setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    }, 3000);
  });
}

function tomlString(s: string): string {
  return JSON.stringify(s);
}

function codexArgs(agentId: string, prompt: string): string[] {
  // Non-interactive Codex cancels MCP calls that would prompt for approval.
  return [
    "--dangerously-bypass-approvals-and-sandbox",
    "exec",
    "--json",
    "--ephemeral",
    "--ignore-user-config",
    "-C",
    HARNESS_DIR,
    "-c",
    `mcp_servers.agent_slack.command=${tomlString("bun")}`,
    "-c",
    `mcp_servers.agent_slack.args=[${tomlString("run")},${tomlString(MCP_AGENT_SLACK)}]`,
    "-c",
    `mcp_servers.agent_slack.env.BASE=${tomlString(BASE)}`,
    "-c",
    `mcp_servers.agent_slack.env.AGENT_ID=${tomlString(agentId)}`,
    "-c",
    `mcp_servers.agent_slack.env.AUDIT_FILE=${tomlString(AUDIT_FILE)}`,
    prompt,
  ];
}

function basePrompt(agentId: string): string {
  return `You are agent_id="${agentId}" in a turbodb coordination test.

You must communicate only through the MCP server named "agent_slack". Use these tools, not shell commands and not curl:
- mcp__agent_slack__health
- mcp__agent_slack__chat_read
- mcp__agent_slack__chat_post
- mcp__agent_slack__file_write
- mcp__agent_slack__file_read
- mcp__agent_slack__file_list

The chat_post tool stamps your from-field as "${agentId}". Do not claim another identity.
Keep chat messages short. End with a concise final status.`;
}

function architectPrompt(): string {
  return `${basePrompt(ARCHITECT)}

Phase 1, architect:
1. Call mcp__agent_slack__health.
2. Post a brief spec to "all" for a Python FastAPI service in main.py:
   - GET /health returns JSON {"ok": true}
   - POST /echo returns the JSON request body verbatim
3. Post a second note that the implementer should use FastAPI Request or equivalent so arbitrary JSON values are echoed, not just objects. Arbitrary JSON includes objects, arrays, strings, numbers, booleans, and null; do not narrow it to object-only payloads.
4. Read chat back with mcp__agent_slack__chat_read to confirm both messages exist.
Do not write files.`;
}

function implementerPrompt(): string {
  return `${basePrompt(IMPLEMENTER)}

Phase 2, implementer:
1. Call mcp__agent_slack__health.
2. Read all chat with mcp__agent_slack__chat_read and follow the architect spec.
3. Write main.py with mcp__agent_slack__file_write. It must be syntactically valid Python:
   from fastapi import FastAPI, Request
   app = FastAPI()
   GET /health returns {"ok": True}
   POST /echo returns await request.json()
4. Confirm with mcp__agent_slack__file_read and mcp__agent_slack__file_list.
5. Post one chat message to "all" summarizing main.py and both endpoints.`;
}

function reviewerPrompt(): string {
  return `${basePrompt(REVIEWER)}

Phase 3, reviewer:
1. Call mcp__agent_slack__health.
2. Read all chat.
3. List files and read main.py with the MCP file tools.
4. Review the actual main.py content, especially whether /echo handles arbitrary JSON values, not only objects. Directly returning await request.json() is correct for this spec because arbitrary JSON includes non-object JSON too.
5. Post one chat review to "all" citing main.py and either approving or naming a concrete bug.`;
}

function implementerPrompt2(): string {
  return `${basePrompt(IMPLEMENTER)}

Phase 4, implementer follow-up:
1. Call mcp__agent_slack__health.
2. Read all chat and inspect the reviewer feedback.
3. If the reviewer approved, make no file changes and post "no changes needed" referencing main.py.
4. If the reviewer found a concrete bug, fix main.py with mcp__agent_slack__file_write and post the fix summary.`;
}

function reviewerPrompt2(): string {
  return `${basePrompt(REVIEWER)}

Phase 5, final reviewer:
1. Call mcp__agent_slack__health.
2. Read all chat, including the implementer follow-up.
3. Read main.py if useful.
4. Post one final approval to "all" referencing main.py.`;
}

type PhaseResult = {
  phase: string;
  agentId: string;
  code: number | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  durationMs: number;
};

async function runPhase(phase: string, agentId: string, prompt: string): Promise<PhaseResult> {
  await appendFile(CODEX_LOG_FILE, `\n--- ${phase} (${agentId}) ${new Date().toISOString()} ---\n`);
  const startedAt = Date.now();
  const child = spawn(CODEX_BIN, codexArgs(agentId, prompt), {
    cwd: HARNESS_DIR,
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env },
  });
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (b) => { stdout += String(b); });
  child.stderr?.on("data", (b) => { stderr += String(b); });

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill("SIGTERM");
    setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    }, 3000);
  }, PHASE_TIMEOUT_MS);

  const code = await new Promise<number | null>((res) => {
    child.once("exit", (c) => res(c));
  });
  clearTimeout(timer);

  await appendFile(CODEX_LOG_FILE, stdout);
  if (stderr.length > 0) await appendFile(CODEX_LOG_FILE, `\n[stderr]\n${stderr}\n`);
  return { phase, agentId, code, stdout, stderr, timedOut, durationMs: Date.now() - startedAt };
}

type ChatMsg = { key: string; from: string; to: string; content: string };
type FileDoc = { path: string; content: string; by: string };

async function scanCol(col: string, limit = 500) {
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}?limit=${limit}`);
  if (r.status === 404) return { docs: [] };
  if (!r.ok) throw new Error(`scan ${col} -> ${r.status} ${await r.text()}`);
  return await r.json() as { docs: { doc_id: number; key: string; value: Record<string, unknown> }[] };
}

async function fetchChatLog(): Promise<ChatMsg[]> {
  const scan = await scanCol("chat", 500);
  return scan.docs
    .filter((d) => d.key.startsWith("msg:"))
    .map((d) => ({
      key: d.key,
      from: String((d.value as { from?: unknown }).from ?? ""),
      to: String((d.value as { to?: unknown }).to ?? ""),
      content: String((d.value as { content?: unknown }).content ?? ""),
    }))
    .sort((a, b) => a.key.localeCompare(b.key));
}

async function fetchFiles(): Promise<FileDoc[]> {
  const scan = await scanCol("files", 500);
  const byPath = new Map<string, { doc_id: number; file: FileDoc }>();
  for (const d of scan.docs) {
    const v = d.value as { content?: unknown; by?: unknown };
    const prev = byPath.get(d.key);
    if (prev && prev.doc_id > d.doc_id) continue;
    byPath.set(d.key, { doc_id: d.doc_id, file: {
      path: d.key,
      content: String(v.content ?? ""),
      by: String(v.by ?? ""),
    } });
  }
  return [...byPath.values()].map((v) => v.file);
}

function tokenize(s: string): Set<string> {
  return new Set(
    s
      .toLowerCase()
      .replace(/[^a-z0-9_\.\/]+/g, " ")
      .split(/\s+/)
      .filter((w) => w.length >= 4),
  );
}

function coordinationSignal(messages: ChatMsg[], filenames: string[]): { count: number; total: number } {
  if (messages.length <= 1) return { count: 0, total: 0 };
  const filenameSet = new Set(filenames.map((f) => f.toLowerCase()));
  const agentIdSet = new Set<string>(AGENT_IDS as readonly string[]);
  const priorVocab = new Set<string>();
  const first = messages[0];
  if (!first) return { count: 0, total: 0 };
  for (const t of tokenize(first.content)) priorVocab.add(t);

  let count = 0;
  let total = 0;
  for (let i = 1; i < messages.length; i++) {
    const m = messages[i];
    if (!m) continue;
    total += 1;
    const cl = m.content.toLowerCase();
    let referenced = false;
    for (const a of agentIdSet) {
      if (a !== m.from && cl.includes(a)) referenced = true;
    }
    for (const fn of filenameSet) {
      if (cl.includes(fn)) referenced = true;
    }
    let overlapCount = 0;
    for (const t of tokenize(m.content)) {
      if (priorVocab.has(t)) overlapCount += 1;
    }
    if (overlapCount >= 2) referenced = true;
    if (referenced) count += 1;
    for (const t of tokenize(m.content)) priorVocab.add(t);
  }
  return { count, total };
}

function pyCompile(content: string): { ok: boolean; detail: string } {
  const tmp = `${tmpdir()}/agent-slack-${Date.now()}-${Math.random().toString(36).slice(2)}.py`;
  try {
    require("node:fs").writeFileSync(tmp, content, "utf8");
    const r = spawnSync("python3", ["-m", "py_compile", tmp], { encoding: "utf8" });
    if (r.status === 0) return { ok: true, detail: "py_compile ok" };
    return { ok: false, detail: `py_compile rc=${r.status}: ${r.stderr || r.stdout}` };
  } finally {
    try { require("node:fs").unlinkSync(tmp); } catch {}
  }
}

type AuditEvent = {
  agent_id: string;
  tool: string;
  input: unknown;
  output: unknown;
};

type UsageTotals = {
  input: number;
  cached: number;
  output: number;
  reasoning: number;
};

function extractUsage(phaseResults: PhaseResult[]): UsageTotals {
  const totals: UsageTotals = { input: 0, cached: 0, output: 0, reasoning: 0 };
  for (const phase of phaseResults) {
    for (const line of phase.stdout.split(/\n+/)) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line) as {
          type?: string;
          usage?: {
            input_tokens?: number;
            cached_input_tokens?: number;
            output_tokens?: number;
            reasoning_output_tokens?: number;
          };
        };
        if (event.type !== "turn.completed" || !event.usage) continue;
        totals.input += event.usage.input_tokens ?? 0;
        totals.cached += event.usage.cached_input_tokens ?? 0;
        totals.output += event.usage.output_tokens ?? 0;
        totals.reasoning += event.usage.reasoning_output_tokens ?? 0;
      } catch {}
    }
  }
  return totals;
}

function auditPath(input: unknown): string {
  if (!input || typeof input !== "object" || !("path" in input)) return "";
  return String((input as { path?: unknown }).path ?? "");
}

async function readAudit(): Promise<AuditEvent[]> {
  try {
    const raw = await readFile(AUDIT_FILE, "utf8");
    return raw
      .split(/\n+/)
      .filter(Boolean)
      .map((line) => JSON.parse(line) as AuditEvent);
  } catch {
    return [];
  }
}

async function main(): Promise<number> {
  console.log(`agent-slack: data=${DATA_DIR} http=${BASE}`);
  try {
    await stat(CODEX_BIN);
  } catch {
    // command -v style lookup is handled by spawn; this only helps absolute paths.
  }

  const child = await startTurbodb();
  console.log(`turbodb up (pid ${child.pid})`);

  const phaseResults: PhaseResult[] = [];
  try {
    const phases = [
      { phase: "1-architect", agentId: ARCHITECT, prompt: architectPrompt() },
      { phase: "2-implementer", agentId: IMPLEMENTER, prompt: implementerPrompt() },
      { phase: "3-reviewer", agentId: REVIEWER, prompt: reviewerPrompt() },
      { phase: "4-implementer-2", agentId: IMPLEMENTER, prompt: implementerPrompt2() },
      { phase: "5-reviewer-2", agentId: REVIEWER, prompt: reviewerPrompt2() },
    ] as const;

    for (const ph of phases) {
      info(`-- phase ${ph.phase} (${ph.agentId}) --`);
      const result = await runPhase(ph.phase, ph.agentId, ph.prompt);
      phaseResults.push(result);
      if (result.code === 0 && !result.timedOut) {
        pass(`${ph.phase} codex exec exited 0`, `${(result.durationMs / 1000).toFixed(1)}s`);
      } else {
        fail(`${ph.phase} codex exec exited 0`, `code=${result.code} timedOut=${result.timedOut}`);
      }
    }

    const messages = await fetchChatLog();
    const files = await fetchFiles();
    const audit = await readAudit();

    const mainPy = files.find((f) => f.path === "main.py");
    if (mainPy) pass("main.py exists in files collection");
    else fail("main.py exists in files collection", `files=[${files.map((f) => f.path).join(",")}]`);

    if (mainPy) {
      const r = pyCompile(mainPy.content);
      if (r.ok) pass("main.py is syntactically valid Python");
      else fail("main.py is syntactically valid Python", r.detail);
    } else {
      fail("main.py is syntactically valid Python", "no main.py to compile");
    }

    const senders = new Set(messages.map((m) => m.from));
    const expectedAll = AGENT_IDS.every((id) => senders.has(id));
    if (expectedAll && senders.size === AGENT_IDS.length) {
      pass("all 3 agents posted to chat", `senders=[${[...senders].join(",")}]`);
    } else {
      fail("all 3 agents posted to chat", `senders=[${[...senders].join(",")}], expected=[${AGENT_IDS.join(",")}]`);
    }

    if (messages.length >= 6) pass("chat log >= 6 messages", `actual: ${messages.length}`);
    else fail("chat log >= 6 messages", `actual: ${messages.length}`);

    const signal = coordinationSignal(messages, files.map((f) => f.path));
    if (signal.total > 0 && signal.count >= Math.ceil(signal.total / 2)) {
      pass("coordination signal present", `${signal.count}/${signal.total} messages reference prior context`);
    } else {
      fail("coordination signal present", `${signal.count}/${signal.total} messages reference prior context`);
    }

    const auditTools = new Set(audit.map((e) => e.tool));
    for (const tool of ["health", "chat_post", "chat_read", "file_write", "file_read", "file_list"]) {
      if (auditTools.has(tool)) pass(`MCP tool used: ${tool}`);
      else fail(`MCP tool used: ${tool}`);
    }

    const auditSenders = new Set(audit.map((e) => e.agent_id));
    const allAuditSenders = AGENT_IDS.every((id) => auditSenders.has(id));
    if (allAuditSenders) pass("all 3 agents invoked MCP tools", `agents=[${[...auditSenders].join(",")}]`);
    else fail("all 3 agents invoked MCP tools", `agents=[${[...auditSenders].join(",")}]`);

    const implementerWroteMain = audit.some((e) =>
      e.agent_id === IMPLEMENTER && e.tool === "file_write" && auditPath(e.input) === "main.py"
    );
    const reviewerReadMain = audit.some((e) =>
      e.agent_id === REVIEWER && e.tool === "file_read" && auditPath(e.input) === "main.py"
    );
    if (implementerWroteMain && reviewerReadMain) {
      pass("artifact handoff happened through Agent Slack tools");
    } else {
      fail("artifact handoff happened through Agent Slack tools", `implementer_write=${implementerWroteMain} reviewer_read=${reviewerReadMain}`);
    }

    console.log(`\n=== chat log ===`);
    for (const m of messages) {
      console.log(`[${m.key}] ${m.from} -> ${m.to}: ${m.content}`);
    }

    if (mainPy) {
      console.log(`\n=== main.py (by ${mainPy.by}) ===`);
      console.log(mainPy.content);
    }

    console.log(`\n=== Agent Slack audit summary ===`);
    const byTool = new Map<string, number>();
    for (const e of audit) byTool.set(e.tool, (byTool.get(e.tool) ?? 0) + 1);
    for (const [tool, count] of [...byTool.entries()].sort()) {
      console.log(`${tool}: ${count}`);
    }

    const totalPhaseMs = phaseResults.reduce((sum, r) => sum + r.durationMs, 0);
    const usage = extractUsage(phaseResults);
    console.log(`\n=== effectiveness counters ===`);
    console.log(`phases: ${phaseResults.length}`);
    console.log(`elapsed_agent_seconds: ${(totalPhaseMs / 1000).toFixed(1)}`);
    console.log(`messages: ${messages.length}`);
    console.log(`files: ${files.length}`);
    console.log(`tool_calls: ${audit.length}`);
    console.log(`coordination_signal: ${signal.count}/${signal.total}`);
    console.log(`tokens: input=${usage.input} cached=${usage.cached} output=${usage.output} reasoning=${usage.reasoning}`);
  } finally {
    await stopTurbodb(child);
  }

  console.log("turbodb stopped");
  const failed = checks.filter((c) => !c.ok);
  console.log(`\n=== summary: ${checks.length - failed.length}/${checks.length} checks passed ===`);
  if (failed.length > 0) {
    console.log(`codex phase logs: ${CODEX_LOG_FILE}`);
    console.log(`Agent Slack audit log: ${AUDIT_FILE}`);
  }
  return failed.length === 0 ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((e) => {
    console.error("agent-slack harness crashed:", e);
    process.exit(2);
  });
