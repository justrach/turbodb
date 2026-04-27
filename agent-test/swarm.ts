// Multi-agent swarm coordination test for turbodb.
//
// Boots a fresh turbodb child, seeds 6 work items into collection "coord",
// then spawns 3 Haiku agents in parallel that race to atomically claim
// 2 tasks each via the /db/:col/txn endpoint. Verifies exactly-once
// semantics: every task has exactly one claim doc, every claim has a
// matching result doc, and claim.by == result.by.
//
// This exercises the ACID-parity work (atomic txn + duplicate-insert
// detection) under concurrent LLM-driven traffic, not synthetic load.
//
// Usage:
//   bun run swarm.ts            # full flow with 3 Haiku agents
//   bun run swarm.ts --no-llm   # seed + verify without LLMs (sanity check)

import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, rm, stat, appendFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
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
const DATA_DIR = resolve(HARNESS_DIR, "agent-test-data-swarm");
const LOG_FILE = resolve(HARNESS_DIR, "turbodb-swarm.log");
const WIRE_PORT = 27117;
const HTTP_PORT = WIRE_PORT + 1; // 27118
const BASE = `http://localhost:${HTTP_PORT}`;
const COLLECTION = "coord";
const TASKS = ["t1", "t2", "t3", "t4", "t5", "t6"] as const;
const AGENT_IDS = ["agent-A", "agent-B", "agent-C"] as const;
const READY_TIMEOUT_MS = 10_000;
const NO_LLM = process.argv.includes("--no-llm");
const CLAUDE_BIN = process.env.CLAUDE_BIN ?? "/Users/rachpradhan/.local/bin/claude";

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
// turbodb child management (mirrors harness.ts)
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
// Direct turbodb client helpers (seed + ground-truth verify)
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

async function scanCollection(): Promise<ScanResp> {
  const r = await fetch(`${BASE}/db/${COLLECTION}?limit=200`);
  if (!r.ok) throw new Error(`scan → ${r.status} ${await r.text()}`);
  return (await r.json()) as ScanResp;
}

async function seedTasks(): Promise<void> {
  // Simple inserts, one per task. Atomicity not needed here — these are
  // pre-race work items, no contention yet.
  for (let i = 0; i < TASKS.length; i++) {
    const id = TASKS[i];
    const r = await fetch(`${BASE}/db/${COLLECTION}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ _id: `task:${id}`, payload: String(i + 1) }),
    });
    if (!r.ok) throw new Error(`seed task:${id} → ${r.status} ${await r.text()}`);
  }
}

// ---------------------------------------------------------------------------
// In-process MCP server: turbodb tools the agents use to act
// ---------------------------------------------------------------------------
//
// Tool names exposed to the model take the form
//   mcp__<server-name>__<tool-name>
// The server name we register here is "turbodb", so the four tool names
// the agent must whitelist are:
//   mcp__turbodb__turbodb_scan
//   mcp__turbodb__turbodb_get
//   mcp__turbodb__turbodb_insert
//   mcp__turbodb__turbodb_txn

function jsonResult(payload: unknown) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(payload) }],
  };
}

function makeMcpServer() {
  return createSdkMcpServer({
    name: "turbodb",
    version: "0.1.0",
    tools: [
      tool(
        "turbodb_scan",
        "Scan a turbodb collection. Returns up to `limit` docs.",
        { collection: z.string(), limit: z.number().int().positive().max(500).optional() },
        async ({ collection, limit }) => {
          const lim = limit ?? 200;
          try {
            const r = await fetch(`${BASE}/db/${encodeURIComponent(collection)}?limit=${lim}`);
            if (!r.ok) {
              const body = await r.text();
              return jsonResult({ ok: false, status: r.status, error: body });
            }
            const j = (await r.json()) as ScanResp;
            // Project to just key + value to keep token usage low.
            const docs = j.docs.map((d) => ({ key: d.key, value: d.value }));
            return jsonResult({ ok: true, count: docs.length, docs });
          } catch (e) {
            return jsonResult({ ok: false, error: String((e as Error).message) });
          }
        },
      ),
      tool(
        "turbodb_get",
        "Get one document by key from a turbodb collection.",
        { collection: z.string(), key: z.string() },
        async ({ collection, key }) => {
          try {
            const r = await fetch(
              `${BASE}/db/${encodeURIComponent(collection)}/${encodeURIComponent(key)}`,
            );
            if (r.status === 404) return jsonResult({ not_found: true });
            if (!r.ok) {
              const body = await r.text();
              return jsonResult({ ok: false, status: r.status, error: body });
            }
            return jsonResult({ ok: true, doc: await r.json() });
          } catch (e) {
            return jsonResult({ ok: false, error: String((e as Error).message) });
          }
        },
      ),
      tool(
        "turbodb_insert",
        "Insert one document with a caller-supplied key. Fails with status:409 if the key already exists.",
        {
          collection: z.string(),
          key: z.string(),
          value: z.record(z.string(), z.unknown()),
        },
        async ({ collection, key, value }) => {
          try {
            const r = await fetch(`${BASE}/db/${encodeURIComponent(collection)}`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ _id: key, ...value }),
            });
            if (r.status === 409) {
              return jsonResult({ ok: false, status: 409, error: "duplicate" });
            }
            if (!r.ok) {
              const body = await r.text();
              return jsonResult({ ok: false, status: r.status, error: body });
            }
            const j = (await r.json()) as { doc_id: number };
            return jsonResult({ ok: true, doc_id: j.doc_id });
          } catch (e) {
            return jsonResult({ ok: false, error: String((e as Error).message) });
          }
        },
      ),
      tool(
        "turbodb_txn",
        "Atomic batch on a single collection. ops is an array of {op, key, value} where op is 'insert'|'update'|'upsert'|'delete'. All-or-nothing. Returns ok:false with status:409 on duplicate-key conflict (another agent claimed first).",
        {
          collection: z.string(),
          ops: z.array(
            z.object({
              op: z.enum(["insert", "update", "upsert", "delete"]),
              key: z.string(),
              value: z.record(z.string(), z.unknown()).optional(),
            }),
          ),
        },
        async ({ collection, ops }) => {
          try {
            const r = await fetch(`${BASE}/db/${encodeURIComponent(collection)}/txn`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ ops }),
            });
            if (r.status === 409) {
              const body = await r.text();
              return jsonResult({ ok: false, status: 409, error: body || "duplicate" });
            }
            if (!r.ok) {
              const body = await r.text();
              return jsonResult({ ok: false, status: r.status, error: body });
            }
            const j = (await r.json()) as { applied: number };
            return jsonResult({ ok: true, applied: j.applied });
          } catch (e) {
            return jsonResult({ ok: false, error: String((e as Error).message) });
          }
        },
      ),
    ],
  });
}

// ---------------------------------------------------------------------------
// Agent prompt + spawn
// ---------------------------------------------------------------------------

const ALLOWED_TOOLS = [
  "mcp__turbodb__turbodb_scan",
  "mcp__turbodb__turbodb_get",
  "mcp__turbodb__turbodb_insert",
  "mcp__turbodb__turbodb_txn",
];

function buildSystemPrompt(agentId: string): string {
  return `You are ${agentId} in a coordination test. Goal: claim and complete 2 tasks from collection "coord".

Tasks have keys "task:t1".."task:t6". To claim a task atomically, call mcp__turbodb__turbodb_txn with collection="coord" and TWO ops in one call:
  1. {"op":"insert","key":"claim:<task_id>","value":{"by":"${agentId}"}}
  2. {"op":"insert","key":"result:<task_id>","value":{"by":"${agentId}","done":true}}

Concrete example for task t3:
  mcp__turbodb__turbodb_txn({
    collection: "coord",
    ops: [
      {op:"insert", key:"claim:t3", value:{by:"${agentId}"}},
      {op:"insert", key:"result:t3", value:{by:"${agentId}", done:true}}
    ]
  })

If the txn returns ok:true, you successfully claimed and completed it (both writes committed atomically).
If the txn returns ok:false with status 409 or "duplicate", another agent beat you. Pick a different task and retry.

Strategy: First call mcp__turbodb__turbodb_scan with collection="coord" to see which task: keys exist (the available pool) and which claim: keys exist (already taken). Pick 2 unclaimed tasks (skip any task whose claim:<id> already exists) and attempt them with separate txn calls. Don't reclaim a task that already has a claim doc.

Stop after 2 successful claims. Be terse — short tool calls only, minimal commentary.`;
}

const USER_PROMPT = "Begin.";

type AgentRunResult = {
  agentId: string;
  executed: boolean;
  reason: string;
  result?: SDKResultMessage;
  contentionObserved: boolean;
  txnCallCount: number;
  txnConflictCount: number;
};

async function runOneAgent(agentId: string): Promise<AgentRunResult> {
  // Each agent gets its own MCP server instance. Reusing one across
  // parallel queries is risky — the SDK may bind it to a single
  // transport. Fresh instance per agent is the safe pattern.
  const mcp = makeMcpServer();

  let lastResult: SDKResultMessage | undefined;
  let txnCallCount = 0;
  let txnConflictCount = 0;

  try {
    const stream = query({
      prompt: USER_PROMPT,
      options: {
        model: "claude-haiku-4-5-20251001",
        systemPrompt: buildSystemPrompt(agentId),
        mcpServers: { turbodb: mcp },
        allowedTools: ALLOWED_TOOLS,
        maxTurns: 15,
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
        continue;
      }
      // Inspect tool_result envelopes on user messages to detect 409s.
      if (msg.type === "user" && msg.message && typeof msg.message === "object") {
        const content = (msg.message as { content?: unknown }).content;
        if (Array.isArray(content)) {
          for (const block of content) {
            if (block && typeof block === "object" && (block as { type?: string }).type === "tool_result") {
              const tr = block as {
                content?: unknown;
                is_error?: boolean;
                tool_use_id?: string;
              };
              const text = Array.isArray(tr.content)
                ? tr.content
                    .map((c) =>
                      typeof c === "object" && c !== null && "text" in c
                        ? String((c as { text: string }).text)
                        : "",
                    )
                    .join("")
                : typeof tr.content === "string"
                  ? tr.content
                  : "";
              // Heuristic: only count tool results that look like our txn tool's payload.
              // The tool_use that produced this result lives in the prior assistant message;
              // we keep it simple and just check the JSON payload shape.
              try {
                const parsed = JSON.parse(text);
                if (parsed && typeof parsed === "object" && "ok" in parsed) {
                  // We can't tell scan/get/insert/txn apart from the result alone, but
                  // the agent only retries on ok:false from txn (per system prompt).
                  // Counting any ok:false 409 as txn-contention is close enough.
                  if (parsed.ok === false && (parsed.status === 409 || /duplicate/i.test(String(parsed.error ?? "")))) {
                    txnConflictCount += 1;
                  }
                  if (parsed.ok === true && typeof parsed.applied === "number") {
                    txnCallCount += 1;
                  }
                }
              } catch {
                // not json — ignore
              }
            }
          }
        }
      }
    }
  } catch (e) {
    return {
      agentId,
      executed: false,
      reason: `agent threw: ${(e as Error).message}`,
      contentionObserved: false,
      txnCallCount,
      txnConflictCount,
    };
  }

  if (!lastResult) {
    return {
      agentId,
      executed: false,
      reason: "agent stream ended with no result message",
      contentionObserved: false,
      txnCallCount,
      txnConflictCount,
    };
  }
  if (lastResult.subtype !== "success") {
    return {
      agentId,
      executed: false,
      reason: `agent result subtype=${lastResult.subtype}`,
      result: lastResult,
      contentionObserved: txnConflictCount > 0,
      txnCallCount,
      txnConflictCount,
    };
  }
  return {
    agentId,
    executed: true,
    reason: "ok",
    result: lastResult,
    contentionObserved: txnConflictCount > 0,
    txnCallCount,
    txnConflictCount,
  };
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

function summarizeClaims(
  scan: ScanResp,
): {
  taskCount: number;
  claimByTask: Map<string, string>;
  resultByTask: Map<string, string>;
} {
  const claimByTask = new Map<string, string>();
  const resultByTask = new Map<string, string>();
  let taskCount = 0;
  for (const d of scan.docs) {
    if (d.key.startsWith("task:")) {
      taskCount += 1;
      continue;
    }
    if (d.key.startsWith("claim:")) {
      const tid = d.key.slice("claim:".length);
      const by = String((d.value as { by?: unknown }).by ?? "");
      claimByTask.set(tid, by);
      continue;
    }
    if (d.key.startsWith("result:")) {
      const tid = d.key.slice("result:".length);
      const by = String((d.value as { by?: unknown }).by ?? "");
      resultByTask.set(tid, by);
      continue;
    }
  }
  return { taskCount, claimByTask, resultByTask };
}

function verifySwarmState(scan: ScanResp): { byAgent: Record<string, string[]> } {
  const { taskCount, claimByTask, resultByTask } = summarizeClaims(scan);

  if (taskCount === TASKS.length) {
    pass(`${TASKS.length} task seed docs present`);
  } else {
    fail(`${TASKS.length} task seed docs present`, `got ${taskCount}`);
  }

  const missingClaims = TASKS.filter((t) => !claimByTask.has(t));
  if (missingClaims.length === 0) {
    pass("all 6 tasks have a claim doc");
  } else {
    fail("all 6 tasks have a claim doc", `missing claims for: ${missingClaims.join(",")}`);
  }

  const missingResults = TASKS.filter((t) => !resultByTask.has(t));
  if (missingResults.length === 0) {
    pass("all 6 claims have a matching result doc");
  } else {
    fail("all 6 claims have a matching result doc", `missing results for: ${missingResults.join(",")}`);
  }

  // No double-claims is a structural property — the DB couldn't store
  // duplicate _ids if it tried. We assert by counting claim docs.
  let claimDocCount = 0;
  for (const d of scan.docs) if (d.key.startsWith("claim:")) claimDocCount += 1;
  if (claimDocCount === TASKS.length) {
    pass("no double-claims (atomicity proof)");
  } else {
    fail("no double-claims (atomicity proof)", `expected ${TASKS.length} claim docs, got ${claimDocCount}`);
  }

  // claim.by == result.by, per task.
  let allMatch = true;
  const mismatches: string[] = [];
  for (const t of TASKS) {
    const c = claimByTask.get(t);
    const r = resultByTask.get(t);
    if (c && r && c !== r) {
      allMatch = false;
      mismatches.push(`${t}: claim=${c} result=${r}`);
    }
  }
  if (allMatch) {
    pass("claim.by == result.by for all tasks");
  } else {
    fail("claim.by == result.by for all tasks", mismatches.join("; "));
  }

  // Per-agent breakdown.
  const byAgent: Record<string, string[]> = {};
  for (const id of AGENT_IDS) byAgent[id] = [];
  for (const t of TASKS) {
    const c = claimByTask.get(t);
    if (c && byAgent[c]) byAgent[c].push(t);
    else if (c) {
      // Some other actor claimed it — shouldn't happen.
      byAgent[c] = byAgent[c] ?? [];
      byAgent[c].push(t);
    }
  }
  return { byAgent };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  console.log(`swarm: data=${DATA_DIR}  http=${BASE}  no-llm=${NO_LLM}`);

  const child = await startTurbodb({ fresh: true });
  console.log(`turbodb up (pid ${child.pid}) — fresh data dir`);

  let agentResults: AgentRunResult[] = [];
  try {
    // Seed phase — direct HTTP, no agents.
    await seedTasks();
    info(`seeded ${TASKS.length} tasks into "${COLLECTION}"`);

    if (NO_LLM) {
      // Sanity path: simulate what the agents would do, two tasks per agent,
      // round-robin, all via the txn endpoint. No retry — the synthetic
      // assignment can't conflict.
      pass("seed phase complete (no-llm mode)");
      let tIdx = 0;
      for (const id of AGENT_IDS) {
        for (let k = 0; k < 2; k++) {
          const tid = TASKS[tIdx++];
          const r = await fetch(`${BASE}/db/${COLLECTION}/txn`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              ops: [
                { op: "insert", key: `claim:${tid}`, value: { by: id } },
                { op: "insert", key: `result:${tid}`, value: { by: id, done: true } },
              ],
            }),
          });
          if (!r.ok) {
            fail(`no-llm: synthetic claim ${id}/${tid}`, `status ${r.status}: ${await r.text()}`);
            return 1;
          }
        }
      }
    } else {
      // LLM path: claude binary required.
      let claudeOk = false;
      try {
        await stat(CLAUDE_BIN);
        claudeOk = true;
      } catch {}
      if (!claudeOk) {
        fail("claude binary present", `not found at ${CLAUDE_BIN}`);
        return 1;
      }

      console.log(`[SWARM] ${AGENT_IDS.length} agents launched in parallel`);
      const t0 = Date.now();
      agentResults = await Promise.all(AGENT_IDS.map((id) => runOneAgent(id)));
      const elapsed = Date.now() - t0;
      info(`all agents settled in ${elapsed}ms`);
    }

    // Verification — direct HTTP scan.
    const scan = await scanCollection();
    const { byAgent } = verifySwarmState(scan);

    if (!NO_LLM) {
      for (const r of agentResults) {
        const claimed = byAgent[r.agentId] ?? [];
        if (r.executed && r.result?.subtype === "success") {
          const cost = r.result.total_cost_usd.toFixed(4);
          info(
            `${r.agentId}: turns=${r.result.num_turns}, cost=$${cost}, claimed=[${claimed.join(", ")}], txn_ok=${r.txnCallCount}, txn_conflict=${r.txnConflictCount}`,
          );
        } else {
          info(`${r.agentId}: did not complete cleanly — ${r.reason}, claimed=[${claimed.join(", ")}]`);
        }
      }
      const totalCost = agentResults.reduce((acc, r) => {
        if (r.result?.subtype === "success") return acc + r.result.total_cost_usd;
        return acc;
      }, 0);
      const totalIn = agentResults.reduce((acc, r) => {
        if (r.result?.subtype === "success") return acc + (r.result.usage.input_tokens ?? 0);
        return acc;
      }, 0);
      const totalOut = agentResults.reduce((acc, r) => {
        if (r.result?.subtype === "success") return acc + (r.result.usage.output_tokens ?? 0);
        return acc;
      }, 0);
      const totalCacheRead = agentResults.reduce((acc, r) => {
        if (r.result?.subtype === "success") return acc + (r.result.usage.cache_read_input_tokens ?? 0);
        return acc;
      }, 0);
      info(
        `total cost: $${totalCost.toFixed(4)}, total tokens: in=${totalIn} out=${totalOut} cache_read=${totalCacheRead}`,
      );
      const anyConflict = agentResults.some((r) => r.txnConflictCount > 0);
      info(`contention observed: ${anyConflict ? "yes" : "no"}${anyConflict ? "" : " (note: agents may have picked disjoint tasks; atomicity not exercised)"}`);
    } else {
      info("contention observed: n/a (--no-llm sanity path uses non-conflicting assignments)");
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
    console.error("swarm crashed:", e);
    process.exit(2);
  });
