# turbodb agent retention test

End-to-end retention test for [turbodb](../). A Haiku-powered Claude
Agent SDK session drives the database over HTTP, then the harness
independently verifies the data, kills turbodb, restarts it against the
same data directory, and verifies again — proving WAL durability.

## What it tests

1. Agent does three POST inserts (alpha, beta, gamma; each `n: 1`).
2. Agent does one atomic `txn` upsert that flips all three to `n: 2`.
3. Harness re-reads via `GET /db/agent_test/:id` and checks: 3 docs,
   all `n == 2`, all have a `ts > 0`, ts values distinct.
4. Harness kills turbodb, restarts it on the same `--data` dir, repeats
   the verification. Same docs + same values means WAL replay works.

## Running

```bash
# requires the host turbodb binary at ../zig-out/bin/turbodb
bun run harness.ts            # full flow (LLM agent + verification)
bun run harness.ts --no-llm   # skip the agent, seed via direct curl
```

Ports used: 27117 (wire) and 27118 (HTTP). Data lives in
`./agent-test-data/` (deleted at the start of every full run). turbodb
stdout/stderr is appended to `./turbodb.log`.

## Auth

The harness uses `pathToClaudeCodeExecutable` to point at the user's
installed `claude` binary so it inherits keychain credentials. If the
agent fails to start with an auth error, either:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
# or
claude /login
```

Even without auth, `--no-llm` exercises the boot, seed, restart, and
retention check — useful as a smoke test of the host binary.

## Expected output (success)

```
[PASS] agent ran to completion  turns=N
[PASS] round-1 (live): 3 docs present
[PASS] round-1 (live): n=2 for all (atomic txn applied)
[PASS] round-1 (live): ts field present and > 0
[PASS] round-1 (live): ts values distinct/monotonic
[PASS] round-2 (after restart): 3 docs present
[PASS] round-2 (after restart): n=2 for all (atomic txn applied)
[PASS] round-2 (after restart): ts=0 post-restart (expected per AGENTS.md §7)
=== summary: 8/8 checks passed ===
```

---

## Swarm test (`swarm.ts`)

A second harness that uses turbodb as a **CAS-style coordination
primitive** for an LLM agent swarm. Three Haiku agents race in parallel
to atomically claim 6 work items via the `/db/:col/txn` endpoint. The
test passes only if every task has exactly one claim doc, every claim
has a matching result doc, and `claim.by == result.by` — i.e. no
double-claims and no torn writes under concurrent LLM-driven traffic.

### What it tests

- The atomic `txn` endpoint as a real coordination primitive.
- Duplicate-`_id` detection (HTTP 409) as the conflict signal that
  drives agent retry.
- Stripe-lock + 2-pass validate-then-apply correctness under racing
  insert conflicts (the txn pairs `claim:<id>` + `result:<id>` so a
  lost claim would also lose its result).

The harness exposes four in-process MCP tools to each agent:

| Tool | Endpoint |
|---|---|
| `mcp__turbodb__turbodb_scan` | `GET /db/:col?limit=N` |
| `mcp__turbodb__turbodb_get` | `GET /db/:col/:key` |
| `mcp__turbodb__turbodb_insert` | `POST /db/:col` |
| `mcp__turbodb__turbodb_txn` | `POST /db/:col/txn` |

Agents see no other tools — turbodb is the only way to act.

### Running

```bash
bun run swarm                # full flow (3 Haiku agents racing in parallel)
bun run swarm:no-llm         # seed + verify without LLMs (sanity check)
```

Ports 27117 (wire) and 27118 (HTTP). Data lives in
`./agent-test-data-swarm/` (separate from the single-agent test, deleted
each run). turbodb output is appended to `./turbodb-swarm.log`.

### Expected output (success)

```
[SWARM] 3 agents launched in parallel
[PASS] 6 task seed docs present
[PASS] all 6 tasks have a claim doc
[PASS] all 6 claims have a matching result doc
[PASS] no double-claims (atomicity proof)
[PASS] claim.by == result.by for all tasks
[INFO] agent-A: turns=N, cost=$X.XX, claimed=[t1, t2], txn_ok=2, txn_conflict=K
[INFO] agent-B: turns=N, cost=$X.XX, claimed=[t3, t5], txn_ok=2, txn_conflict=K
[INFO] agent-C: turns=N, cost=$X.XX, claimed=[t4, t6], txn_ok=2, txn_conflict=K
[INFO] total cost: $0.10–0.20, total tokens: in=X out=Y cache_read=Z
[INFO] contention observed: yes
=== summary: 5/5 checks passed ===
```

If `contention observed: no`, the test still passed but the agents
happened to pick disjoint tasks; atomicity was not exercised. Re-run
or shrink the task pool to force collisions.

---

## Chat-coord test (`chat.ts`)

A third harness that uses turbodb as a **shared chat layer + file store**
for an LLM agent swarm doing a *real* task: collaboratively building a
small Python FastAPI service. Three Haiku agents with distinct roles
(`architect`, `implementer`, `reviewer`) run in **sequential phases** and
can only communicate via turbodb — no direct channel.

### What it tests

- turbodb as a coordination substrate beyond CAS/claim semantics —
  unstructured chat (append-only `chat` collection) plus a key/value
  file store (upsert in `files` collection).
- Whether the agents *actually reference* each other's prior messages
  (real coord) vs writing in monologue (DB-as-write-only-log). The
  harness scores this with a keyword-overlap heuristic at the end.
- An end-to-end artifact: `main.py` is extracted from the `files`
  collection and run through `python3 -m py_compile` for a hard pass/fail.

The harness exposes these in-process MCP tools, with `from` bound to the
calling agent's id (an agent cannot post as another agent):

| Tool | Backing |
|---|---|
| `mcp__chat__chat_post` | `POST /db/chat`  (insert, key auto-assigned `msg:NNNN`) |
| `mcp__chat__chat_read` | `GET /db/chat?limit=500`, sorted, optional `since` filter |
| `mcp__chat__file_write` | `POST /db/files/txn` upsert (re-writes overwrite) |
| `mcp__chat__file_read` | `GET /db/files/:path` |
| `mcp__chat__file_list` | `GET /db/files?limit=500`, paths + authors only |

### Phases (sequential)

1. **Architect** (5 turns) — posts a spec to `"all"`. No code.
2. **Implementer** (8 turns) — reads chat, writes `main.py`, posts a summary.
3. **Reviewer** (6 turns) — reads chat + files, posts a review citing filenames.
4. **Implementer round 2** (5 turns) — fixes if requested, else posts "no changes needed".
5. **Reviewer round 2** (3 turns) — final approval or one more comment.

### Running

```bash
bun run chat                # full flow (5 phases, 3 Haiku agents)
bun run chat:no-llm         # seed fake chat + main.py, run verification only
```

Ports 27117 (wire) and 27118 (HTTP). Data lives in
`./agent-test-data-chat/` (deleted each run). turbodb output goes to
`./turbodb-chat.log`.

### Verification

- `main.py` exists in the `files` collection
- `main.py` is syntactically valid Python (`python3 -m py_compile`)
- All 3 agents posted to chat (`from` field is harness-stamped)
- Chat log has at least 6 messages
- Coordination signal: `K/N messages reference prior context` (heuristic)
- Full chat log is printed (truncated 300 chars/msg) plus full `main.py`

The coord signal is heuristic — high signal value but not definitive.
Eyeball the chat log to judge real coordination vs monologue.

---

## Agent Slack test (`codex-chat-tools.ts`)

Agent Slack is the Codex-native variant of the chat-coord test: a tiny,
turbodb-backed coordination bus for agents. Instead of the Claude Agent
SDK's in-process MCP server, it runs five `codex exec` phases and
configures a local stdio MCP server (`mcp-chat.ts`) for each phase.

The MCP server exposes real tools:

| Tool | Backing |
|---|---|
| `mcp__agent_slack__health` | `GET /health` |
| `mcp__agent_slack__chat_post` | `POST /db/chat`, with `from` stamped by `AGENT_ID` |
| `mcp__agent_slack__chat_read` | `GET /db/chat?limit=500` |
| `mcp__agent_slack__file_write` | `POST /db/files/txn` upsert |
| `mcp__agent_slack__file_read` | `GET /db/files/:path` |
| `mcp__agent_slack__file_list` | `GET /db/files?limit=500` |

Every tool call is appended to `agent-slack.audit.ndjson`, so the
harness can verify the agents used the tools rather than only leaving
database state behind.

```bash
bun run chat:codex-tools
bun run agent-slack
```

Ports 27317 (wire) and 27318 (HTTP). Data lives in
`./agent-test-data-agent-slack/`. Codex JSONL output is written to
`./agent-slack.log`, and turbodb output goes to
`./turbodb-agent-slack.log`.

The harness prints effectiveness counters at the end: phase count,
elapsed agent seconds, message count, file count, tool-call count,
coordination signal, and token totals from Codex JSONL. These numbers
do not prove efficiency by themselves; they give us an A/B surface for
comparing Agent Slack against no-chat or direct-HTTP variants.

The harness runs nested `codex exec` with
`--dangerously-bypass-approvals-and-sandbox`. Without that, non-interactive
Codex sees the MCP tools but cancels each tool call before execution because
there is no approval UI to answer the tool prompt.

### Compare coordination styles

`agent-slack-compare.ts` runs the same five-phase task in three modes:

| Mode | Meaning |
|---|---|
| `no-bus` | Negative control: isolated agents, no shared state or artifact channel. |
| `direct-http` | Agents coordinate by manually calling turbodb HTTP endpoints. |
| `agent-slack` | Agents coordinate through the Agent Slack MCP tools. |

```bash
bun run agent-slack:compare
bun run agent-slack:compare -- --modes=direct-http,agent-slack
```

The comparison table reports phase success, artifact presence,
`py_compile`, chat/message counts, artifact handoff, tool events, elapsed
agent seconds, and token totals.
