// End-to-end retention test for turbodb.
//
// Boots a fresh turbodb child, drives it with a Haiku-powered Claude Agent SDK
// session that does insert + atomic txn upsert, independently verifies the
// resulting state, then kills the server, restarts it against the same data
// directory, and re-verifies — proving WAL durability across process restart.
//
// Usage:
//   bun run harness.ts            # full flow with LLM
//   bun run harness.ts --no-llm   # skip the agent, seed via direct curl,
//                                 # still run boot + restart + retention check

import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, rm, stat, appendFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { query, type SDKMessage, type SDKResultMessage } from "@anthropic-ai/claude-agent-sdk";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const REPO_ROOT = resolve(import.meta.dir, "..");
const HARNESS_DIR = import.meta.dir;
const TURBODB_BIN = resolve(REPO_ROOT, "zig-out/bin/turbodb");
const DATA_DIR = resolve(HARNESS_DIR, "agent-test-data");
const LOG_FILE = resolve(HARNESS_DIR, "turbodb.log");
const WIRE_PORT = 27117;
const HTTP_PORT = WIRE_PORT + 1; // 27118
const BASE = `http://localhost:${HTTP_PORT}`;
const COLLECTION = "agent_test";
const KEYS = ["alpha", "beta", "gamma"] as const;
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

// ---------------------------------------------------------------------------
// turbodb child management
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

  // Wait for /health.
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
    // Hard kill if it doesn't exit quickly.
    setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL");
    }, 3000);
  });
}

// ---------------------------------------------------------------------------
// Direct turbodb client helpers (ground-truth verification)
// ---------------------------------------------------------------------------

type GetResp = {
  doc_id: number;
  key: string;
  version: number;
  ts: number;
  value: Record<string, unknown>;
};

async function getDoc(key: string): Promise<GetResp | null> {
  const r = await fetch(`${BASE}/db/${COLLECTION}/${encodeURIComponent(key)}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`GET ${key} → ${r.status} ${await r.text()}`);
  return (await r.json()) as GetResp;
}

async function seedDirectly(): Promise<void> {
  // Used only in --no-llm mode: replicate what the agent would do.
  for (const id of KEYS) {
    const r = await fetch(`${BASE}/db/${COLLECTION}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ _id: id, name: id, n: 1 }),
    });
    if (!r.ok) throw new Error(`insert ${id} → ${r.status} ${await r.text()}`);
  }
  const r = await fetch(`${BASE}/db/${COLLECTION}/txn`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ops: KEYS.map((k) => ({ op: "upsert", key: k, value: { name: k, n: 2 } })),
    }),
  });
  if (!r.ok) throw new Error(`txn → ${r.status} ${await r.text()}`);
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

async function verifyState(phase: string, opts: { tsExpected: boolean }): Promise<void> {
  // Fetch each key directly. This is the ground truth — the scan endpoint
  // returns full version history, which is fine but harder to assert against.
  const docs: GetResp[] = [];
  let missing = 0;
  for (const k of KEYS) {
    const d = await getDoc(k);
    if (d === null) { missing += 1; continue; }
    docs.push(d);
  }

  if (missing === 0 && docs.length === KEYS.length) {
    pass(`${phase}: 3 docs present`);
  } else {
    fail(`${phase}: 3 docs present`, `got ${docs.length}/${KEYS.length}, missing=${missing}`);
    return;
  }

  const allTwo = docs.every((d) => (d.value as { n?: number }).n === 2);
  if (allTwo) pass(`${phase}: n=2 for all (atomic txn applied)`);
  else fail(`${phase}: n=2 for all`, `values=${JSON.stringify(docs.map((d) => d.value))}`);

  // Engine ts. AGENTS.md §7: ts is per-process, not durable. Restart
  // resets to 0; the ts on a doc loaded from WAL but not yet re-mutated
  // in the current process is 0 by design. So we only require ts > 0
  // when we expect it (round-1, immediately after writes).
  if (opts.tsExpected) {
    const allHaveTs = docs.every((d) => typeof d.ts === "number" && d.ts > 0);
    if (allHaveTs) pass(`${phase}: ts field present and > 0`);
    else fail(`${phase}: ts field present and > 0`, `ts=${JSON.stringify(docs.map((d) => d.ts))}`);

    const tsList = docs.map((d) => d.ts).sort((a, b) => a - b);
    const distinct = new Set(tsList).size === tsList.length;
    if (distinct) pass(`${phase}: ts values distinct/monotonic`);
    else fail(`${phase}: ts values distinct/monotonic`, `ts=${JSON.stringify(tsList)}`);
  } else {
    const tsZero = docs.every((d) => d.ts === 0);
    if (tsZero) {
      pass(`${phase}: ts=0 post-restart (expected per AGENTS.md §7)`);
    } else {
      console.log(`[INFO] ${phase}: ts=${JSON.stringify(docs.map((d) => d.ts))} (per-process counter; non-zero pre-write would be a bug)`);
    }
  }
}

// ---------------------------------------------------------------------------
// Agent step
// ---------------------------------------------------------------------------

type AgentOutcome = {
  executed: boolean;
  reason: string;
  result?: SDKResultMessage;
};

const SYSTEM_PROMPT = `You are testing turbodb, a Zig embedded database, at ${BASE}.
Use the Bash tool with curl only — never any other host. Keep output minimal.
Do exactly the three tasks the user gives you, in order, then stop.
On any HTTP error (status >= 400), print the response body and stop.`;

const USER_PROMPT = `Run these three tasks against ${BASE} using curl, in order.

1. Insert three documents into collection ${COLLECTION}:
   curl -s -X POST ${BASE}/db/${COLLECTION} -H 'Content-Type: application/json' -d '{"_id":"alpha","name":"alpha","n":1}'
   curl -s -X POST ${BASE}/db/${COLLECTION} -H 'Content-Type: application/json' -d '{"_id":"beta","name":"beta","n":1}'
   curl -s -X POST ${BASE}/db/${COLLECTION} -H 'Content-Type: application/json' -d '{"_id":"gamma","name":"gamma","n":1}'

2. Atomically upsert all three to n=2 with one POST:
   curl -s -X POST ${BASE}/db/${COLLECTION}/txn -H 'Content-Type: application/json' -d '{"ops":[{"op":"upsert","key":"alpha","value":{"name":"alpha","n":2}},{"op":"upsert","key":"beta","value":{"name":"beta","n":2}},{"op":"upsert","key":"gamma","value":{"name":"gamma","n":2}}]}'

3. List the collection and report what you got back:
   curl -s '${BASE}/db/${COLLECTION}?limit=10'

Report a one-line summary then stop.`;

async function runAgent(): Promise<AgentOutcome> {
  // Quick sanity check: does the configured claude binary exist?
  const claudeBin = CLAUDE_BIN;
  let claudePathOk = false;
  try {
    await stat(claudeBin);
    claudePathOk = true;
  } catch {}

  if (!claudePathOk) {
    return {
      executed: false,
      reason: `claude binary not found at ${claudeBin} — set CLAUDE_BIN or install claude-code`,
    };
  }

  let lastResult: SDKResultMessage | undefined;
  try {
    const stream = query({
      prompt: USER_PROMPT,
      options: {
        model: "claude-haiku-4-5-20251001",
        systemPrompt: SYSTEM_PROMPT,
        allowedTools: ["Bash"],
        tools: ["Bash"],
        maxTurns: 10,
        permissionMode: "bypassPermissions",
        allowDangerouslySkipPermissions: true,
        cwd: HARNESS_DIR,
        pathToClaudeCodeExecutable: claudeBin,
        // Don't load any project/user/local settings — keep this hermetic.
        settingSources: [],
        persistSession: false,
      },
    });

    for await (const msg of stream as AsyncIterable<SDKMessage>) {
      if (msg.type === "result") {
        lastResult = msg as SDKResultMessage;
      }
      // Don't log the noisy intermediate messages; the agent's curl output
      // shows up in turbodb.log via the WAL anyway.
    }
  } catch (e) {
    return { executed: false, reason: `agent threw: ${(e as Error).message}` };
  }

  if (!lastResult) {
    return { executed: false, reason: "agent stream ended with no result message" };
  }
  if (lastResult.subtype !== "success") {
    return { executed: false, reason: `agent result subtype=${lastResult.subtype}`, result: lastResult };
  }
  return { executed: true, reason: "ok", result: lastResult };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  console.log(`harness: data=${DATA_DIR}  http=${BASE}  no-llm=${NO_LLM}`);

  // Phase 1: fresh boot.
  let child = await startTurbodb({ fresh: true });
  console.log(`turbodb up (pid ${child.pid}) — fresh data dir`);

  let agentOutcome: AgentOutcome = { executed: false, reason: "skipped (--no-llm)" };
  try {
    if (NO_LLM) {
      await seedDirectly();
      pass("seeded via direct curl (no-llm mode)");
    } else {
      agentOutcome = await runAgent();
      if (agentOutcome.executed) {
        pass("agent ran to completion", `turns=${agentOutcome.result?.num_turns ?? "?"}`);
      } else {
        fail("agent ran to completion", agentOutcome.reason);
        // Fall back to direct seeding so we can still exercise the retention
        // path. This makes the harness useful even when auth is missing.
        try {
          await seedDirectly();
          console.log("(fallback) seeded directly so retention check still runs");
        } catch (e) {
          console.log(`(fallback) direct seed also failed: ${(e as Error).message}`);
        }
      }
    }

    // Round 1: verify with turbodb still running.
    await verifyState("round-1 (live)", { tsExpected: true });
  } finally {
    await stopTurbodb(child);
  }
  console.log("turbodb stopped");

  // Phase 2: restart against the same data dir.
  child = await startTurbodb({ fresh: false });
  console.log(`turbodb up (pid ${child.pid}) — restarted, replaying WAL`);
  try {
    await verifyState("round-2 (after restart)", { tsExpected: false });
  } finally {
    await stopTurbodb(child);
  }
  console.log("turbodb stopped");

  // Final report.
  const failed = checks.filter((c) => !c.ok);
  console.log(`\n=== summary: ${checks.length - failed.length}/${checks.length} checks passed ===`);
  if (agentOutcome.result) {
    const r = agentOutcome.result;
    if (r.subtype === "success") {
      console.log(`agent: ${r.num_turns} turns, $${r.total_cost_usd.toFixed(4)}, ${r.duration_ms}ms`);
      console.log(`usage: in=${r.usage.input_tokens} out=${r.usage.output_tokens} cache_read=${r.usage.cache_read_input_tokens ?? 0}`);
    }
  } else if (!NO_LLM) {
    console.log(`agent: did not execute — ${agentOutcome.reason}`);
    console.log(`hint: set ANTHROPIC_API_KEY in env, or run "claude /login" to authenticate`);
  }

  return failed.length === 0 ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((e) => {
    console.error("harness crashed:", e);
    process.exit(2);
  });
