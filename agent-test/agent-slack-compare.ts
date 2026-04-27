#!/usr/bin/env bun
// Compare three coordination styles for the same five-phase agent task:
// 1. no-bus: isolated agents, no shared state
// 2. direct-http: agents coordinate by hand-written HTTP calls to turbodb
// 3. agent-slack: agents coordinate through MCP tools backed by turbodb

import { spawn, type ChildProcess } from "node:child_process";
import { appendFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

const REPO_ROOT = resolve(import.meta.dir, "..");
const HARNESS_DIR = import.meta.dir;
const TURBODB_BIN = resolve(REPO_ROOT, "zig-out/bin/turbodb");
const MCP_AGENT_SLACK = resolve(HARNESS_DIR, "mcp-chat.ts");
const CODEX_BIN = process.env.CODEX_BIN ?? "codex";
const READY_TIMEOUT_MS = 10_000;
const PHASE_TIMEOUT_MS = Number(process.env.PHASE_TIMEOUT_MS ?? 240_000);
const COMPARE_LOG = resolve(HARNESS_DIR, "agent-slack-compare.log");

const ARCHITECT = "architect";
const IMPLEMENTER = "implementer";
const REVIEWER = "reviewer";
const AGENT_IDS = [ARCHITECT, IMPLEMENTER, REVIEWER] as const;
type AgentId = typeof AGENT_IDS[number];
type Mode = "no-bus" | "direct-http" | "agent-slack";

type PhaseDef = { phase: string; agentId: AgentId; prompt: string };
type PhaseResult = {
  phase: string;
  agentId: AgentId;
  code: number | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  durationMs: number;
};
type ChatMsg = { key: string; from: string; to: string; content: string };
type FileDoc = { path: string; content: string; by: string };
type UsageTotals = { input: number; cached: number; output: number; reasoning: number };
type AuditEvent = { agent_id: string; tool: string; input: unknown; output: unknown };
type ModeResult = {
  mode: Mode;
  phaseOk: boolean;
  mainExists: boolean;
  pyCompile: boolean;
  allSenders: boolean;
  messages: number;
  files: number;
  coordination: string;
  artifactHandoff: boolean;
  toolEvents: number;
  toolBreakdown: string;
  elapsedSeconds: number;
  tokens: UsageTotals;
  notes: string;
};

function selectedModes(): Mode[] {
  const arg = process.argv.find((a) => a.startsWith("--modes="));
  if (!arg) return ["no-bus", "direct-http", "agent-slack"];
  const modes = arg.slice("--modes=".length).split(",").map((m) => m.trim()).filter(Boolean);
  const valid = new Set<Mode>(["no-bus", "direct-http", "agent-slack"]);
  return modes.filter((m): m is Mode => valid.has(m as Mode));
}

function tomlString(s: string): string {
  return JSON.stringify(s);
}

async function startTurbodb(mode: Mode, wirePort: number): Promise<{ child: ChildProcess; base: string }> {
  const dataDir = resolve(HARNESS_DIR, `agent-test-data-${mode}`);
  const logFile = resolve(HARNESS_DIR, `turbodb-${mode}.log`);
  await rm(dataDir, { recursive: true, force: true });
  await mkdir(dataDir, { recursive: true });
  await mkdir(dirname(logFile), { recursive: true });
  await writeFile(logFile, "");

  const child = spawn(
    TURBODB_BIN,
    ["--port", String(wirePort), "--both", "--data", dataDir],
    { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] },
  );
  await appendFile(logFile, `--- turbodb [${mode}] pid=${child.pid} ${new Date().toISOString()} ---\n`);
  child.stdout?.on("data", (b) => { void appendFile(logFile, b); });
  child.stderr?.on("data", (b) => { void appendFile(logFile, b); });

  const base = `http://localhost:${wirePort + 1}`;
  const deadline = Date.now() + READY_TIMEOUT_MS;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${base}/health`);
      if (r.ok) {
        await r.text();
        return { child, base };
      }
    } catch (e) {
      lastErr = e;
    }
    await sleep(100);
  }
  child.kill("SIGKILL");
  throw new Error(`${mode}: turbodb did not become ready (last err: ${lastErr})`);
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

function codexArgs(mode: Mode, prompt: string, opts: { base?: string; agentId?: AgentId; auditFile?: string }): string[] {
  const args = [
    "--dangerously-bypass-approvals-and-sandbox",
    "exec",
    "--json",
    "--ephemeral",
    "--ignore-user-config",
    "-C",
    HARNESS_DIR,
  ];
  if (mode === "agent-slack") {
    args.push(
      "-c", `mcp_servers.agent_slack.command=${tomlString("bun")}`,
      "-c", `mcp_servers.agent_slack.args=[${tomlString("run")},${tomlString(MCP_AGENT_SLACK)}]`,
      "-c", `mcp_servers.agent_slack.env.BASE=${tomlString(opts.base ?? "")}`,
      "-c", `mcp_servers.agent_slack.env.AGENT_ID=${tomlString(opts.agentId ?? "unknown")}`,
      "-c", `mcp_servers.agent_slack.env.AUDIT_FILE=${tomlString(opts.auditFile ?? "")}`,
    );
  }
  args.push(prompt);
  return args;
}

async function runPhase(mode: Mode, def: PhaseDef, opts: { base?: string; auditFile?: string; logFile: string }): Promise<PhaseResult> {
  await appendFile(opts.logFile, `\n--- ${mode} ${def.phase} (${def.agentId}) ${new Date().toISOString()} ---\n`);
  const startedAt = Date.now();
  const child = spawn(CODEX_BIN, codexArgs(mode, def.prompt, {
    base: opts.base,
    agentId: def.agentId,
    auditFile: opts.auditFile,
  }), {
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
  const code = await new Promise<number | null>((res) => child.once("exit", (c) => res(c)));
  clearTimeout(timer);

  await appendFile(opts.logFile, stdout);
  if (stderr) await appendFile(opts.logFile, `\n[stderr]\n${stderr}\n`);
  return { ...def, code, stdout, stderr, timedOut, durationMs: Date.now() - startedAt };
}

function noBusPrompts(): PhaseDef[] {
  return [
    {
      phase: "1-architect",
      agentId: ARCHITECT,
      prompt: `You are architect. There is no shared bus, no shared files, no database, and no way to talk to other agents. Do not run commands. Return a short FastAPI spec for main.py: GET /health returns {"ok": true}; POST /echo returns the JSON request body verbatim; /echo should handle arbitrary JSON values, not only objects.`,
    },
    {
      phase: "2-implementer",
      agentId: IMPLEMENTER,
      prompt: `You are implementer. There is no shared bus and you cannot read architect output. Do not run commands or write files. Return only a Python code block for main.py implementing FastAPI GET /health -> {"ok": True} and POST /echo -> await request.json().`,
    },
    {
      phase: "3-reviewer",
      agentId: REVIEWER,
      prompt: `You are reviewer. There is no shared bus and no artifact to inspect. Do not run commands. State whether you can genuinely review main.py. Be honest if you cannot inspect the file.`,
    },
    {
      phase: "4-implementer-2",
      agentId: IMPLEMENTER,
      prompt: `You are implementer round 2. There is no shared bus and no reviewer feedback available. Do not run commands. State that no coordinated changes can be made.`,
    },
    {
      phase: "5-reviewer-2",
      agentId: REVIEWER,
      prompt: `You are reviewer round 2. There is no shared bus and no artifact to inspect. Do not run commands. Give a final status reflecting that limitation.`,
    },
  ];
}

function directPromptBase(agentId: AgentId, base: string): string {
  return `You are agent_id="${agentId}" in a direct-HTTP coordination test.

Use only shell commands that call turbodb HTTP at ${base}. Do not use MCP tools and do not use local files as handoff.
Collections:
- chat: POST ${base}/db/chat with body {"_id":"msg:NNNN","from":"${agentId}","to":"all","content":"...","ts":<integer>}
- files: POST ${base}/db/files/txn with {"ops":[{"op":"upsert","key":"main.py","value":{"content":"...","by":"${agentId}","ts":<integer>}}]}
Read chat with GET ${base}/db/chat?limit=500. Read files with GET ${base}/db/files and GET ${base}/db/files/main.py.
Keep chat messages short. End with a concise final status.`;
}

function directHttpPrompts(base: string): PhaseDef[] {
  return [
    {
      phase: "1-architect",
      agentId: ARCHITECT,
      prompt: `${directPromptBase(ARCHITECT, base)}

Phase 1:
1. Check GET /health.
2. Insert msg:0001 with a FastAPI main.py spec: GET /health returns {"ok": true}; POST /echo returns JSON body verbatim.
3. Insert msg:0002 noting /echo should use Request or equivalent so arbitrary JSON values are echoed, not only objects.
4. Read chat back and confirm.`,
    },
    {
      phase: "2-implementer",
      agentId: IMPLEMENTER,
      prompt: `${directPromptBase(IMPLEMENTER, base)}

Phase 2:
1. Check health.
2. Read chat.
3. Upsert files/main.py with valid FastAPI code using FastAPI and Request. /health returns {"ok": True}; /echo returns await request.json().
4. Read files/main.py and list files to confirm.
5. Insert msg:0003 summarizing main.py and both endpoints.`,
    },
    {
      phase: "3-reviewer",
      agentId: REVIEWER,
      prompt: `${directPromptBase(REVIEWER, base)}

Phase 3:
1. Check health.
2. Read chat.
3. Read files/main.py over HTTP.
4. Review actual code, especially arbitrary JSON handling. Directly returning await request.json() is correct because arbitrary JSON includes objects, arrays, strings, numbers, booleans, and null.
5. Insert msg:0004 approving or naming a concrete bug, citing main.py.`,
    },
    {
      phase: "4-implementer-2",
      agentId: IMPLEMENTER,
      prompt: `${directPromptBase(IMPLEMENTER, base)}

Phase 4:
1. Check health.
2. Read chat and inspect msg:0004.
3. If approved, do not change main.py and insert msg:0005 saying no changes needed. If a bug was listed, fix files/main.py and summarize.`,
    },
    {
      phase: "5-reviewer-2",
      agentId: REVIEWER,
      prompt: `${directPromptBase(REVIEWER, base)}

Phase 5:
1. Check health.
2. Read chat and files/main.py.
3. Insert msg:0006 with final approval referencing main.py.`,
    },
  ];
}

function agentSlackBase(agentId: AgentId): string {
  return `You are agent_id="${agentId}" in an Agent Slack coordination test.

Communicate only through the MCP server named "agent_slack":
- mcp__agent_slack__health
- mcp__agent_slack__chat_read
- mcp__agent_slack__chat_post
- mcp__agent_slack__file_write
- mcp__agent_slack__file_read
- mcp__agent_slack__file_list
Do not use curl or shell for coordination. The tool server stamps your identity.`;
}

function agentSlackPrompts(): PhaseDef[] {
  return [
    {
      phase: "1-architect",
      agentId: ARCHITECT,
      prompt: `${agentSlackBase(ARCHITECT)}
Post exactly two short messages to all:
1. Spec: build main.py with FastAPI, GET /health returning {"ok": true}, and POST /echo returning the JSON request body verbatim.
2. Note: /echo must use Request or equivalent and must preserve arbitrary JSON values; arbitrary JSON includes objects, arrays, strings, numbers, booleans, and null. Do not narrow it to object-only payloads.
Read chat back to confirm. Do not write files.`,
    },
    {
      phase: "2-implementer",
      agentId: IMPLEMENTER,
      prompt: `${agentSlackBase(IMPLEMENTER)}
Read chat, write main.py with file_write, confirm with file_read and file_list, then post one summary. main.py must use FastAPI and Request; /health returns {"ok": True}; /echo returns await request.json().`,
    },
    {
      phase: "3-reviewer",
      agentId: REVIEWER,
      prompt: `${agentSlackBase(REVIEWER)}
Read chat, list files, read main.py, review actual code for arbitrary JSON handling, then post one review citing main.py. Directly returning await request.json() is correct for this spec because arbitrary JSON includes non-object JSON too; do not request object-only validation.`,
    },
    {
      phase: "4-implementer-2",
      agentId: IMPLEMENTER,
      prompt: `${agentSlackBase(IMPLEMENTER)}
Read chat and reviewer feedback. If approved, post no changes needed. If a bug was listed, fix main.py through file_write and post the fix summary.`,
    },
    {
      phase: "5-reviewer-2",
      agentId: REVIEWER,
      prompt: `${agentSlackBase(REVIEWER)}
Read chat, read main.py if useful, then post final approval referencing main.py.`,
    },
  ];
}

async function scanCol(base: string, col: string, limit = 500) {
  const r = await fetch(`${base}/db/${encodeURIComponent(col)}?limit=${limit}`);
  if (r.status === 404) return { docs: [] };
  if (!r.ok) throw new Error(`scan ${col} -> ${r.status} ${await r.text()}`);
  return await r.json() as { docs: { doc_id: number; key: string; value: Record<string, unknown> }[] };
}

async function fetchChatLog(base: string): Promise<ChatMsg[]> {
  const scan = await scanCol(base, "chat", 500);
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

async function fetchFiles(base: string): Promise<FileDoc[]> {
  const scan = await scanCol(base, "files", 500);
  const byPath = new Map<string, { doc_id: number; file: FileDoc }>();
  for (const d of scan.docs) {
    const v = d.value as { content?: unknown; by?: unknown };
    const prev = byPath.get(d.key);
    if (prev && prev.doc_id > d.doc_id) continue;
    byPath.set(d.key, {
      doc_id: d.doc_id,
      file: { path: d.key, content: String(v.content ?? ""), by: String(v.by ?? "") },
    });
  }
  return [...byPath.values()].map((v) => v.file);
}

function extractAgentMessages(results: PhaseResult[]): string {
  const chunks: string[] = [];
  for (const r of results) {
    for (const line of r.stdout.split(/\n+/)) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line) as { type?: string; item?: { type?: string; text?: string } };
        if (event.type === "item.completed" && event.item?.type === "agent_message" && event.item.text) {
          chunks.push(event.item.text);
        }
      } catch {}
    }
  }
  return chunks.join("\n\n");
}

function extractPythonFence(text: string): string | null {
  const py = /```(?:python|py)\n([\s\S]*?)```/i.exec(text);
  if (py?.[1]) return py[1].trim() + "\n";
  const any = /```\n([\s\S]*?)```/i.exec(text);
  return any?.[1] ? any[1].trim() + "\n" : null;
}

function pyCompile(content: string | null): boolean {
  if (!content) return false;
  const tmp = `${tmpdir()}/agent-slack-compare-${Date.now()}-${Math.random().toString(36).slice(2)}.py`;
  try {
    require("node:fs").writeFileSync(tmp, content, "utf8");
    const r = spawnSync("python3", ["-m", "py_compile", tmp], { encoding: "utf8" });
    return r.status === 0;
  } finally {
    try { require("node:fs").unlinkSync(tmp); } catch {}
  }
}

function tokenize(s: string): Set<string> {
  return new Set(s.toLowerCase().replace(/[^a-z0-9_\.\/]+/g, " ").split(/\s+/).filter((w) => w.length >= 4));
}

function coordinationSignal(messages: ChatMsg[], filenames: string[]): { count: number; total: number } {
  if (messages.length <= 1) return { count: 0, total: 0 };
  const prior = new Set<string>();
  for (const t of tokenize(messages[0]?.content ?? "")) prior.add(t);
  const filenameSet = new Set(filenames.map((f) => f.toLowerCase()));
  let count = 0;
  let total = 0;
  for (let i = 1; i < messages.length; i++) {
    const m = messages[i];
    if (!m) continue;
    total += 1;
    const cl = m.content.toLowerCase();
    let refs = filenames.some((f) => filenameSet.has(f.toLowerCase()) && cl.includes(f.toLowerCase()));
    let overlap = 0;
    for (const t of tokenize(m.content)) if (prior.has(t)) overlap += 1;
    if (overlap >= 2) refs = true;
    if (refs) count += 1;
    for (const t of tokenize(m.content)) prior.add(t);
  }
  return { count, total };
}

function extractUsage(results: PhaseResult[]): UsageTotals {
  const totals: UsageTotals = { input: 0, cached: 0, output: 0, reasoning: 0 };
  for (const phase of results) {
    for (const line of phase.stdout.split(/\n+/)) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line) as {
          type?: string;
          usage?: { input_tokens?: number; cached_input_tokens?: number; output_tokens?: number; reasoning_output_tokens?: number };
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

function toolEventBreakdown(results: PhaseResult[]): { count: number; summary: string } {
  const byType = new Map<string, number>();
  for (const r of results) {
    for (const line of r.stdout.split(/\n+/)) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line) as { type?: string; item?: { type?: string; status?: string } };
        if (event.type !== "item.started" || !event.item?.type) continue;
        const itemType = event.item.type;
        if (itemType === "agent_message") continue;
        byType.set(itemType, (byType.get(itemType) ?? 0) + 1);
      } catch {}
    }
  }
  return {
    count: [...byType.values()].reduce((a, b) => a + b, 0),
    summary: [...byType.entries()].map(([k, v]) => `${k}:${v}`).join(",") || "-",
  };
}

async function readAudit(auditFile: string): Promise<AuditEvent[]> {
  try {
    const raw = await readFile(auditFile, "utf8");
    return raw.split(/\n+/).filter(Boolean).map((line) => JSON.parse(line) as AuditEvent);
  } catch {
    return [];
  }
}

function auditPath(input: unknown): string {
  if (!input || typeof input !== "object" || !("path" in input)) return "";
  return String((input as { path?: unknown }).path ?? "");
}

async function runMode(mode: Mode): Promise<ModeResult> {
  const logFile = resolve(HARNESS_DIR, `agent-slack-compare.${mode}.log`);
  await writeFile(logFile, "");
  let child: ChildProcess | null = null;
  let base = "";
  let auditFile = "";
  try {
    let phases: PhaseDef[];
    if (mode === "no-bus") {
      phases = noBusPrompts();
    } else if (mode === "direct-http") {
      const started = await startTurbodb(mode, 27417);
      child = started.child;
      base = started.base;
      phases = directHttpPrompts(base);
    } else {
      const started = await startTurbodb(mode, 27517);
      child = started.child;
      base = started.base;
      auditFile = resolve(HARNESS_DIR, "agent-slack-compare.audit.ndjson");
      await rm(auditFile, { force: true });
      phases = agentSlackPrompts();
    }

    const phaseResults: PhaseResult[] = [];
    for (const ph of phases) {
      console.log(`[${mode}] ${ph.phase} (${ph.agentId})`);
      phaseResults.push(await runPhase(mode, ph, { base, auditFile, logFile }));
    }

    const phaseOk = phaseResults.every((r) => r.code === 0 && !r.timedOut);
    const elapsedSeconds = phaseResults.reduce((sum, r) => sum + r.durationMs, 0) / 1000;
    const tokens = extractUsage(phaseResults);
    const toolEvents = toolEventBreakdown(phaseResults);

    let messages: ChatMsg[] = [];
    let files: FileDoc[] = [];
    let mainContent: string | null = null;
    let artifactHandoff = false;
    let notes = "";

    if (mode === "no-bus") {
      const text = extractAgentMessages(phaseResults);
      mainContent = extractPythonFence(text);
      notes = "negative control; no shared state, reviewer cannot inspect artifact";
    } else {
      messages = await fetchChatLog(base);
      files = await fetchFiles(base);
      mainContent = files.find((f) => f.path === "main.py")?.content ?? null;
      if (mode === "agent-slack") {
        const audit = await readAudit(auditFile);
        artifactHandoff = audit.some((e) => e.agent_id === IMPLEMENTER && e.tool === "file_write" && auditPath(e.input) === "main.py")
          && audit.some((e) => e.agent_id === REVIEWER && e.tool === "file_read" && auditPath(e.input) === "main.py");
      } else {
        artifactHandoff = messages.some((m) => m.from === REVIEWER && /main\.py/.test(m.content) && /request\.json|arbitrary json/i.test(m.content));
      }
    }

    const signal = coordinationSignal(messages, files.map((f) => f.path));
    const senders = new Set(messages.map((m) => m.from));
    const allSenders = mode !== "no-bus" && AGENT_IDS.every((id) => senders.has(id));
    return {
      mode,
      phaseOk,
      mainExists: Boolean(mainContent),
      pyCompile: pyCompile(mainContent),
      allSenders,
      messages: messages.length,
      files: files.length,
      coordination: `${signal.count}/${signal.total}`,
      artifactHandoff,
      toolEvents: toolEvents.count,
      toolBreakdown: toolEvents.summary,
      elapsedSeconds,
      tokens,
      notes,
    };
  } finally {
    if (child) await stopTurbodb(child);
  }
}

function bool(v: boolean): string {
  return v ? "yes" : "no";
}

async function main(): Promise<number> {
  await writeFile(COMPARE_LOG, "");
  const modes = selectedModes();
  if (modes.length === 0) throw new Error("no valid modes selected");
  console.log(`agent-slack compare: modes=${modes.join(",")}`);
  const results: ModeResult[] = [];
  for (const mode of modes) {
    const result = await runMode(mode);
    results.push(result);
    await appendFile(COMPARE_LOG, `${JSON.stringify(result)}\n`);
  }

  console.log("\n=== comparison ===");
  console.log("mode | phases | artifact | py | senders | msgs | files | coord | handoff | tool_events | sec | tokens_out | notes");
  console.log("---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---");
  for (const r of results) {
    console.log([
      r.mode,
      bool(r.phaseOk),
      bool(r.mainExists),
      bool(r.pyCompile),
      bool(r.allSenders),
      String(r.messages),
      String(r.files),
      r.coordination,
      bool(r.artifactHandoff),
      `${r.toolEvents} (${r.toolBreakdown})`,
      r.elapsedSeconds.toFixed(1),
      String(r.tokens.output),
      r.notes,
    ].join(" | "));
  }

  const failedCore = results.filter((r) => r.mode !== "no-bus" && !(r.phaseOk && r.mainExists && r.pyCompile && r.allSenders && r.artifactHandoff));
  return failedCore.length === 0 ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((e) => {
    console.error("agent-slack compare crashed:", e);
    process.exit(2);
  });
