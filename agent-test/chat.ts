// Multi-agent chat/coordination test for turbodb.
//
// Three Haiku agents with distinct roles (Architect, Implementer, Reviewer)
// collaborate to build a small Python FastAPI service. They cannot speak
// directly — every message and every produced file goes through turbodb.
//
// The interesting outcome is whether they actually reference each other's
// messages (real coordination) vs writing in monologue. The harness scores
// this with a simple keyword-overlap heuristic at the end.
//
// Phases run SEQUENTIALLY (not in parallel) so each phase sees the prior
// phase's chat log. Total upper bound: ~27 turns across 5 phases.
//
// Usage:
//   bun run chat.ts            # full flow with Haiku agents
//   bun run chat.ts --no-llm   # seed fake chat + main.py, run verification only
//
// Mirror harness.ts / swarm.ts for boot/restart/auth patterns.
//
// Requires the host turbodb binary at ../zig-out/bin/turbodb.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, rm, stat, appendFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";
import {
  query,
  createSdkMcpServer,
  tool,
  type SDKMessage,
  type SDKResultMessage,
} from "@anthropic-ai/claude-agent-sdk";
import { z } from "zod";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const REPO_ROOT = resolve(import.meta.dir, "..");
const HARNESS_DIR = import.meta.dir;
const TURBODB_BIN = resolve(REPO_ROOT, "zig-out/bin/turbodb");
const DATA_DIR = resolve(HARNESS_DIR, "agent-test-data-chat");
const LOG_FILE = resolve(HARNESS_DIR, "turbodb-chat.log");
const WIRE_PORT = 27117;
const HTTP_PORT = WIRE_PORT + 1; // 27118
const BASE = `http://localhost:${HTTP_PORT}`;
const CHAT_COL = "chat";
const FILES_COL = "files";
const READY_TIMEOUT_MS = 10_000;
const NO_LLM = process.argv.includes("--no-llm");
const CLAUDE_BIN = process.env.CLAUDE_BIN ?? "/Users/rachpradhan/.local/bin/claude";

const ARCHITECT = "architect";
const IMPLEMENTER = "implementer";
const REVIEWER = "reviewer";
const AGENT_IDS = [ARCHITECT, IMPLEMENTER, REVIEWER] as const;

// ---------------------------------------------------------------------------
// Result accounting
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// turbodb child management (mirrors harness.ts / swarm.ts)
// ---------------------------------------------------------------------------

async function startTurbodb(opts: { fresh: boolean }): Promise<ChildProcess> {
  if (opts.fresh) {
    await rm(DATA_DIR, { recursive: true, force: true });
  }
  await mkdir(DATA_DIR, { recursive: true });
  await mkdir(dirname(LOG_FILE), { recursive: true });

  const child = spawn(
    TURBODB_BIN,
    ["--port", String(WIRE_PORT), "--both", "--data", DATA_DIR],
    { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] },
  );

  const tag = opts.fresh ? "[fresh]" : "[restart]";
  await appendFile(LOG_FILE, `\n--- turbodb ${tag} pid=${child.pid} ${new Date().toISOString()} ---\n`);
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

// ---------------------------------------------------------------------------
// Direct turbodb client helpers
// ---------------------------------------------------------------------------

type ScanDoc = {
  doc_id: number;
  key: string;
  version: number;
  ts: number;
  value: Record<string, unknown>;
};

type ScanResp = {
  tenant: string;
  collection: string;
  count: number;
  docs: ScanDoc[];
};

async function scanCol(col: string, limit = 500): Promise<ScanResp> {
  // AGENTS.md: scan endpoint is GET /db/:col?limit=N (no /scan segment).
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}?limit=${limit}`);
  if (r.status === 404) {
    return { tenant: "default", collection: col, count: 0, docs: [] };
  }
  if (!r.ok) throw new Error(`scan ${col} → ${r.status} ${await r.text()}`);
  return (await r.json()) as ScanResp;
}

async function getDoc(col: string, key: string): Promise<ScanDoc | null> {
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}/${encodeURIComponent(key)}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`get ${col}/${key} → ${r.status} ${await r.text()}`);
  return (await r.json()) as ScanDoc;
}

async function insertDoc(col: string, key: string, value: Record<string, unknown>): Promise<{ ok: boolean; status: number; body?: string }> {
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ _id: key, ...value }),
  });
  if (!r.ok) return { ok: false, status: r.status, body: await r.text() };
  await r.text();
  return { ok: true, status: r.status };
}

async function upsertDoc(col: string, key: string, value: Record<string, unknown>): Promise<{ ok: boolean; status: number; body?: string }> {
  // upsert via single-op txn — server.zig supports {op:"upsert"} at /db/:col/txn.
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}/txn`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ops: [{ op: "upsert", key, value }] }),
  });
  if (!r.ok) return { ok: false, status: r.status, body: await r.text() };
  await r.text();
  return { ok: true, status: r.status };
}

// ---------------------------------------------------------------------------
// Shared chat-sequence counter
//
// We want append-only message keys with stable ordering across phases. The
// counter is harness-managed (in-process); each chat_post call increments it
// before issuing the insert. This keeps key generation centralized and lets
// the harness stamp the `from` field.
// ---------------------------------------------------------------------------

let chatSeq = 0;

function nextChatKey(): string {
  chatSeq += 1;
  return `msg:${String(chatSeq).padStart(4, "0")}`;
}

// ---------------------------------------------------------------------------
// In-process MCP server: chat + file tools, agent_id bound in closure
// ---------------------------------------------------------------------------
//
// Tool names exposed to the model:
//   mcp__chat__chat_post
//   mcp__chat__chat_read
//   mcp__chat__file_write
//   mcp__chat__file_read
//   mcp__chat__file_list
//
// The agent's `from` is captured here, NOT taken from the model's input —
// an agent cannot post as another agent.

function jsonResult(payload: unknown) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(payload) }],
  };
}

function makeMcpServer(agentId: string) {
  return createSdkMcpServer({
    name: "chat",
    version: "0.1.0",
    tools: [
      tool(
        "chat_post",
        `Post a chat message. \`to\` is "all" or another agent's id. The from-field is set automatically to "${agentId}". Storage: collection "${CHAT_COL}", key auto-assigned.`,
        {
          to: z.string(),
          content: z.string(),
        },
        async ({ to, content }) => {
          const key = nextChatKey();
          const body = {
            from: agentId,
            to: String(to),
            content: String(content),
            ts: Date.now(),
          };
          const r = await insertDoc(CHAT_COL, key, body);
          if (!r.ok) {
            return jsonResult({ ok: false, status: r.status, error: r.body });
          }
          return jsonResult({ ok: true, key });
        },
      ),
      tool(
        "chat_read",
        `Read recent chat messages (up to 50). If \`since\` is given (e.g. "msg:0007"), returns only messages with key strictly greater than that. Sorted ascending by key.`,
        {
          since: z.string().optional(),
        },
        async ({ since }) => {
          try {
            const scan = await scanCol(CHAT_COL, 500);
            let msgs = scan.docs
              .filter((d) => d.key.startsWith("msg:"))
              .map((d) => ({
                key: d.key,
                from: String((d.value as { from?: unknown }).from ?? ""),
                to: String((d.value as { to?: unknown }).to ?? ""),
                content: String((d.value as { content?: unknown }).content ?? ""),
              }));
            msgs.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
            if (typeof since === "string" && since.length > 0) {
              msgs = msgs.filter((m) => m.key > since);
            }
            if (msgs.length > 50) msgs = msgs.slice(-50);
            return jsonResult({ ok: true, count: msgs.length, messages: msgs });
          } catch (e) {
            return jsonResult({ ok: false, error: String((e as Error).message) });
          }
        },
      ),
      tool(
        "file_write",
        `Write or replace a file in the shared file store. \`path\` is just a filename like "main.py" — no slashes. Upserts so re-writes overwrite.`,
        {
          path: z.string(),
          content: z.string(),
        },
        async ({ path, content }) => {
          const cleaned = String(path).replace(/^\.?\/+/, "");
          if (cleaned.includes("/") || cleaned.length === 0) {
            return jsonResult({ ok: false, error: "path must be a bare filename (no slashes)" });
          }
          const r = await upsertDoc(FILES_COL, cleaned, {
            content: String(content),
            by: agentId,
            ts: Date.now(),
          });
          if (!r.ok) return jsonResult({ ok: false, status: r.status, error: r.body });
          return jsonResult({ ok: true, path: cleaned });
        },
      ),
      tool(
        "file_read",
        `Read a file from the shared file store. Returns {content, by, ts} or {not_found:true}.`,
        {
          path: z.string(),
        },
        async ({ path }) => {
          const cleaned = String(path).replace(/^\.?\/+/, "");
          const d = await getDoc(FILES_COL, cleaned);
          if (!d) return jsonResult({ not_found: true });
          const v = d.value as { content?: unknown; by?: unknown; ts?: unknown };
          return jsonResult({
            ok: true,
            content: String(v.content ?? ""),
            by: String(v.by ?? ""),
            ts: typeof v.ts === "number" ? v.ts : 0,
          });
        },
      ),
      tool(
        "file_list",
        `List files in the shared file store. Returns just paths and authors — no content. Cheap.`,
        {},
        async () => {
          try {
            const scan = await scanCol(FILES_COL, 500);
            const files = scan.docs.map((d) => ({
              path: d.key,
              by: String((d.value as { by?: unknown }).by ?? ""),
            }));
            return jsonResult({ ok: true, count: files.length, files });
          } catch (e) {
            return jsonResult({ ok: false, error: String((e as Error).message) });
          }
        },
      ),
    ],
  });
}

const ALLOWED_TOOLS = [
  "mcp__chat__chat_post",
  "mcp__chat__chat_read",
  "mcp__chat__file_write",
  "mcp__chat__file_read",
  "mcp__chat__file_list",
];

// ---------------------------------------------------------------------------
// System prompts (per role)
// ---------------------------------------------------------------------------

const TEAM_BLURB = `You are part of a 3-agent team building a small Python FastAPI service together.
Roles:
  - architect:    writes the spec only. Does NOT write code.
  - implementer:  writes Python files based on the spec. Does NOT review.
  - reviewer:     reads the chat AND the files, posts a review citing filenames and content.

You can ONLY communicate via the chat tool. You cannot speak directly to other agents. Read the chat (mcp__chat__chat_read) BEFORE posting so you don't duplicate work.

Be terse. One short message per chat post. Don't repeat yourself.

Tool call shape (literal):
  mcp__chat__chat_post({to:"all", content:"..."})
  mcp__chat__chat_read({})                          // all messages
  mcp__chat__chat_read({since:"msg:0007"})          // only newer
  mcp__chat__file_write({path:"main.py", content:"..."})
  mcp__chat__file_read({path:"main.py"})
  mcp__chat__file_list({})
`;

function architectPrompt(): string {
  return `You are agent_id="${ARCHITECT}".
${TEAM_BLURB}
YOUR JOB THIS PHASE:
Post a brief spec to "all" for a Python FastAPI service with these two endpoints:
  - GET /health  → returns JSON {"ok": true}
  - POST /echo   → returns the JSON request body verbatim
Pick what file(s) the implementer should produce (at minimum main.py). Do NOT write code yourself. Post the spec as one or two short chat messages and stop.`;
}

function implementerPrompt(): string {
  return `You are agent_id="${IMPLEMENTER}".
${TEAM_BLURB}
YOUR JOB THIS PHASE:
1. Call mcp__chat__chat_read({}) to read the architect's spec.
2. Use mcp__chat__file_write to create at minimum main.py implementing a Python FastAPI app per the spec. Real, runnable code — \`from fastapi import FastAPI\`, GET /health returning {"ok": true}, POST /echo returning the request body.
3. Call mcp__chat__file_list({}) to verify your write landed.
4. Post ONE short chat message to "all" summarizing what you wrote (filename + key endpoints). Then stop.`;
}

function reviewerPrompt(): string {
  return `You are agent_id="${REVIEWER}".
${TEAM_BLURB}
YOUR JOB THIS PHASE:
1. Call mcp__chat__chat_read({}) to read the architect's spec and the implementer's summary.
2. Call mcp__chat__file_list({}) and mcp__chat__file_read for each file to inspect the code.
3. Post ONE review to "all". Cite specific filenames and content (e.g. "main.py defines GET /health that returns {\\"ok\\":true} — looks good"). If you spot a bug, list it concretely. If clean, post explicit "approved".`;
}

function implementerPrompt2(): string {
  return `You are agent_id="${IMPLEMENTER}".
${TEAM_BLURB}
YOUR JOB THIS PHASE:
1. Call mcp__chat__chat_read({}) to read the latest reviewer feedback.
2. If the reviewer requested concrete changes, fix the file via mcp__chat__file_write and post a one-line follow-up to "all".
3. If the reviewer approved, just post "no changes needed" to "all" and stop.
Be terse. One round only.`;
}

function reviewerPrompt2(): string {
  return `You are agent_id="${REVIEWER}".
${TEAM_BLURB}
YOUR JOB THIS PHASE:
1. Call mcp__chat__chat_read({}) to see if the implementer made changes.
2. If changes were made, optionally re-read main.py with mcp__chat__file_read.
3. Post ONE final short message to "all": either "final approval" or one last comment.`;
}

// ---------------------------------------------------------------------------
// Agent runner
// ---------------------------------------------------------------------------

type PhaseResult = {
  phase: string;
  agentId: string;
  executed: boolean;
  reason: string;
  result?: SDKResultMessage;
  postsBefore: number;
  postsAfter: number;
  filesBefore: number;
  filesAfter: number;
};

async function countPosts(): Promise<number> {
  try {
    const s = await scanCol(CHAT_COL, 500);
    return s.docs.filter((d) => d.key.startsWith("msg:")).length;
  } catch { return 0; }
}

async function countFiles(): Promise<number> {
  try {
    const s = await scanCol(FILES_COL, 500);
    return s.docs.length;
  } catch { return 0; }
}

async function runOnePhase(opts: {
  phase: string;
  agentId: string;
  systemPrompt: string;
  maxTurns: number;
}): Promise<PhaseResult> {
  const { phase, agentId, systemPrompt, maxTurns } = opts;
  const postsBefore = await countPosts();
  const filesBefore = await countFiles();
  const mcp = makeMcpServer(agentId);

  let lastResult: SDKResultMessage | undefined;
  try {
    const stream = query({
      prompt: "Begin.",
      options: {
        model: "claude-haiku-4-5-20251001",
        systemPrompt,
        mcpServers: { chat: mcp },
        allowedTools: ALLOWED_TOOLS,
        maxTurns,
        permissionMode: "bypassPermissions",
        allowDangerouslySkipPermissions: true,
        cwd: HARNESS_DIR,
        pathToClaudeCodeExecutable: CLAUDE_BIN,
        settingSources: [],
        persistSession: false,
      },
    });
    for await (const msg of stream as AsyncIterable<SDKMessage>) {
      if (msg.type === "result") {
        lastResult = msg as SDKResultMessage;
      }
    }
  } catch (e) {
    return {
      phase,
      agentId,
      executed: false,
      reason: `agent threw: ${(e as Error).message}`,
      postsBefore,
      postsAfter: await countPosts(),
      filesBefore,
      filesAfter: await countFiles(),
    };
  }

  const postsAfter = await countPosts();
  const filesAfter = await countFiles();
  if (!lastResult) {
    return { phase, agentId, executed: false, reason: "no result message", postsBefore, postsAfter, filesBefore, filesAfter };
  }
  if (lastResult.subtype !== "success") {
    return { phase, agentId, executed: false, reason: `subtype=${lastResult.subtype}`, result: lastResult, postsBefore, postsAfter, filesBefore, filesAfter };
  }
  return { phase, agentId, executed: true, reason: "ok", result: lastResult, postsBefore, postsAfter, filesBefore, filesAfter };
}

// ---------------------------------------------------------------------------
// No-LLM seed path — for sanity testing the verification logic without
// burning tokens. Posts a fake chat log + writes a real main.py.
// ---------------------------------------------------------------------------

const FAKE_MAIN_PY = `from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/echo")
async def echo(req: Request):
    body = await req.json()
    return body
`;

async function seedFake(): Promise<void> {
  const messages: { from: string; to: string; content: string }[] = [
    { from: ARCHITECT, to: "all", content: "Spec: FastAPI app in main.py. GET /health returns {\"ok\": true}. POST /echo returns the request body verbatim." },
    { from: ARCHITECT, to: "all", content: "Use FastAPI's Request to read the JSON body for /echo." },
    { from: IMPLEMENTER, to: "all", content: "Wrote main.py with GET /health and POST /echo as specced." },
    { from: REVIEWER, to: "all", content: "main.py: /health returns {\"ok\": true}. /echo reads request JSON and returns it. Approved." },
    { from: IMPLEMENTER, to: "all", content: "no changes needed" },
    { from: REVIEWER, to: "all", content: "final approval" },
  ];
  for (const m of messages) {
    const key = nextChatKey();
    const r = await insertDoc(CHAT_COL, key, { ...m, ts: Date.now() });
    if (!r.ok) throw new Error(`seedFake: insert ${key} failed: ${r.status} ${r.body}`);
  }
  const r = await upsertDoc(FILES_COL, "main.py", { content: FAKE_MAIN_PY, by: IMPLEMENTER, ts: Date.now() });
  if (!r.ok) throw new Error(`seedFake: upsert main.py failed: ${r.status} ${r.body}`);
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

type ChatMsg = { key: string; from: string; to: string; content: string };

async function fetchChatLog(): Promise<ChatMsg[]> {
  const scan = await scanCol(CHAT_COL, 500);
  const msgs = scan.docs
    .filter((d) => d.key.startsWith("msg:"))
    .map((d) => ({
      key: d.key,
      from: String((d.value as { from?: unknown }).from ?? ""),
      to: String((d.value as { to?: unknown }).to ?? ""),
      content: String((d.value as { content?: unknown }).content ?? ""),
    }));
  msgs.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return msgs;
}

async function fetchFiles(): Promise<{ path: string; content: string; by: string }[]> {
  const scan = await scanCol(FILES_COL, 500);
  return scan.docs.map((d) => {
    const v = d.value as { content?: unknown; by?: unknown };
    return {
      path: d.key,
      content: String(v.content ?? ""),
      by: String(v.by ?? ""),
    };
  });
}

function pyCompile(content: string): { ok: boolean; detail: string } {
  try {
    const tmp = `${tmpdir()}/turbodb-chat-${Date.now()}-${Math.random().toString(36).slice(2)}.py`;
    // sync write — small, no need for async here
    require("node:fs").writeFileSync(tmp, content, "utf8");
    const r = spawnSync("python3", ["-m", "py_compile", tmp], { encoding: "utf8" });
    try { require("node:fs").unlinkSync(tmp); } catch {}
    if (r.status === 0) return { ok: true, detail: "py_compile ok" };
    return { ok: false, detail: `py_compile rc=${r.status}: ${r.stderr || r.stdout}` };
  } catch (e) {
    return { ok: false, detail: `pyCompile error: ${(e as Error).message}` };
  }
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

function coordinationSignal(messages: ChatMsg[], filenames: string[]): { count: number; total: number; refByMsg: Map<string, string[]> } {
  // For each message after the first, check whether its content contains:
  //   - any other agent's id, OR
  //   - any filename from the files collection, OR
  //   - any non-trivial token (>=4 chars) that appeared in any prior message.
  // Count messages that "reference" prior context. This is heuristic — a
  // signal-of-coord, not a proof.
  const refByMsg = new Map<string, string[]>();
  if (messages.length <= 1) return { count: 0, total: 0, refByMsg };
  const agentIdSet = new Set<string>(AGENT_IDS as readonly string[]);
  const filenameSet = new Set(filenames.map((f) => f.toLowerCase()));
  let priorVocab = new Set<string>();
  // seed with first message's vocab
  for (const t of tokenize(messages[0].content)) priorVocab.add(t);

  let count = 0;
  let total = 0;
  for (let i = 1; i < messages.length; i++) {
    total += 1;
    const m = messages[i];
    const cl = m.content.toLowerCase();
    const refs: string[] = [];
    for (const a of agentIdSet) if (a !== m.from && cl.includes(a)) refs.push(`@${a}`);
    for (const fn of filenameSet) if (cl.includes(fn)) refs.push(`#${fn}`);
    const myToks = tokenize(m.content);
    let overlapCount = 0;
    const overlapSamples: string[] = [];
    for (const t of myToks) {
      if (priorVocab.has(t)) {
        overlapCount += 1;
        if (overlapSamples.length < 3) overlapSamples.push(t);
      }
    }
    if (overlapCount >= 2) refs.push(`overlap[${overlapSamples.join(",")}]`);
    if (refs.length > 0) count += 1;
    if (refs.length > 0) refByMsg.set(m.key, refs);
    for (const t of myToks) priorVocab.add(t);
  }
  return { count, total, refByMsg };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  console.log(`chat-swarm: data=${DATA_DIR}  http=${BASE}  no-llm=${NO_LLM}`);
  console.log(`[CHAT-SWARM] 3 agents, 5 sequential phases`);

  const child = await startTurbodb({ fresh: true });
  console.log(`turbodb up (pid ${child.pid}) — fresh data dir`);

  const phaseResults: PhaseResult[] = [];

  try {
    if (NO_LLM) {
      await seedFake();
      info("seeded fake chat log + main.py (no-llm mode)");
    } else {
      // claude binary check
      let claudeOk = false;
      try { await stat(CLAUDE_BIN); claudeOk = true; } catch {}
      if (!claudeOk) {
        fail("claude binary present", `not found at ${CLAUDE_BIN}`);
        return 1;
      }

      // Run phases sequentially.
      const phases = [
        { phase: "1-architect",     agentId: ARCHITECT,   systemPrompt: architectPrompt(),    maxTurns: 5 },
        { phase: "2-implementer",   agentId: IMPLEMENTER, systemPrompt: implementerPrompt(),  maxTurns: 8 },
        { phase: "3-reviewer",      agentId: REVIEWER,    systemPrompt: reviewerPrompt(),     maxTurns: 6 },
        { phase: "4-implementer-2", agentId: IMPLEMENTER, systemPrompt: implementerPrompt2(), maxTurns: 5 },
        { phase: "5-reviewer-2",    agentId: REVIEWER,    systemPrompt: reviewerPrompt2(),    maxTurns: 3 },
      ] as const;

      for (const ph of phases) {
        info(`-- phase ${ph.phase} (${ph.agentId}, maxTurns=${ph.maxTurns}) --`);
        const r = await runOnePhase(ph);
        phaseResults.push(r);
        if (r.executed && r.result?.subtype === "success") {
          const cost = r.result.total_cost_usd.toFixed(4);
          const posts = r.postsAfter - r.postsBefore;
          const files = r.filesAfter - r.filesBefore;
          info(`${ph.phase}: turns=${r.result.num_turns} cost=$${cost} posts=+${posts} files=+${files}`);
        } else {
          info(`${ph.phase}: did not complete cleanly — ${r.reason}`);
        }
      }
    }

    // ---- Verification ----
    const messages = await fetchChatLog();
    const files = await fetchFiles();

    const mainPy = files.find((f) => f.path === "main.py");
    if (mainPy) {
      pass("main.py exists in files collection");
    } else {
      fail("main.py exists in files collection", `files=[${files.map((f) => f.path).join(",")}]`);
    }

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

    if (messages.length >= 6) {
      pass(`chat log >= 6 messages`, `actual: ${messages.length}`);
    } else {
      fail(`chat log >= 6 messages`, `actual: ${messages.length}`);
    }

    const sig = coordinationSignal(messages, files.map((f) => f.path));
    info(`coordination_signal: ${sig.count}/${sig.total} messages reference prior context`);

    // Cost summary
    let totalCost = 0;
    let totalIn = 0, totalOut = 0, totalCacheRead = 0;
    for (const r of phaseResults) {
      if (r.result?.subtype === "success") {
        totalCost += r.result.total_cost_usd;
        totalIn += r.result.usage.input_tokens ?? 0;
        totalOut += r.result.usage.output_tokens ?? 0;
        totalCacheRead += r.result.usage.cache_read_input_tokens ?? 0;
      }
    }
    if (!NO_LLM) {
      info(`total cost: $${totalCost.toFixed(4)}, total tokens: in=${totalIn} out=${totalOut} cache_read=${totalCacheRead}`);
    }

    // Print chat log (truncated)
    console.log(`\n=== chat log (truncated to 300 chars/msg) ===`);
    for (const m of messages) {
      const refs = sig.refByMsg.get(m.key);
      const refStr = refs && refs.length > 0 ? `  refs: ${refs.join(" ")}` : "";
      const truncated = m.content.length > 300 ? m.content.slice(0, 300) + "…" : m.content;
      console.log(`[${m.key}] ${m.from} → ${m.to}: ${truncated}${refStr}`);
    }

    // Print main.py (full)
    if (mainPy) {
      console.log(`\n=== main.py (by ${mainPy.by}) ===`);
      console.log(mainPy.content);
    } else {
      console.log(`\n=== main.py === (NOT WRITTEN)`);
    }

    if (files.length > 1) {
      console.log(`\n=== other files ===`);
      for (const f of files) {
        if (f.path === "main.py") continue;
        console.log(`--- ${f.path} (by ${f.by}) ---`);
        console.log(f.content);
      }
    }
  } finally {
    await stopTurbodb(child);
  }
  console.log("turbodb stopped");

  const failed = checks.filter((c) => !c.ok);
  console.log(`\n=== summary: ${checks.length - failed.length}/${checks.length} checks passed ===`);
  return failed.length === 0 ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((e) => {
    console.error("chat harness crashed:", e);
    process.exit(2);
  });
