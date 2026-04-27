#!/usr/bin/env bun
import { appendFile } from "node:fs/promises";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import * as z from "zod/v4";

const BASE = process.env.BASE ?? "http://localhost:27118";
const AGENT_ID = process.env.AGENT_ID ?? "unknown";
const AUDIT_FILE = process.env.AUDIT_FILE;
const CHAT_COL = process.env.CHAT_COL ?? "chat";
const FILES_COL = process.env.FILES_COL ?? "files";

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

function jsonResult(payload: unknown) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(payload) }],
  };
}

async function audit(toolName: string, input: unknown, output: unknown) {
  if (!AUDIT_FILE) return;
  const event = {
    at: new Date().toISOString(),
    agent_id: AGENT_ID,
    tool: toolName,
    input,
    output,
  };
  await appendFile(AUDIT_FILE, `${JSON.stringify(event)}\n`);
}

async function scanCol(col: string, limit = 500): Promise<ScanResp> {
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}?limit=${limit}`);
  if (r.status === 404) {
    return { tenant: "default", collection: col, count: 0, docs: [] };
  }
  if (!r.ok) throw new Error(`scan ${col} -> ${r.status} ${await r.text()}`);
  return (await r.json()) as ScanResp;
}

async function getDoc(col: string, key: string): Promise<ScanDoc | null> {
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}/${encodeURIComponent(key)}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`get ${col}/${key} -> ${r.status} ${await r.text()}`);
  return (await r.json()) as ScanDoc;
}

async function insertDoc(col: string, key: string, value: Record<string, unknown>) {
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ _id: key, ...value }),
  });
  if (!r.ok) return { ok: false, status: r.status, body: await r.text() };
  await r.text();
  return { ok: true, status: r.status };
}

async function upsertDoc(col: string, key: string, value: Record<string, unknown>) {
  const r = await fetch(`${BASE}/db/${encodeURIComponent(col)}/txn`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ops: [{ op: "upsert", key, value }] }),
  });
  if (!r.ok) return { ok: false, status: r.status, body: await r.text() };
  await r.text();
  return { ok: true, status: r.status };
}

async function nextChatKey(): Promise<string> {
  const scan = await scanCol(CHAT_COL, 500);
  let max = 0;
  for (const d of scan.docs) {
    const m = /^msg:(\d+)$/.exec(d.key);
    if (!m) continue;
    max = Math.max(max, Number(m[1]));
  }
  return `msg:${String(max + 1).padStart(4, "0")}`;
}

function cleanPath(path: string): string {
  const cleaned = path.replace(/^\.?\/+/, "");
  if (cleaned.length === 0 || cleaned.includes("/")) {
    throw new Error("path must be a bare filename (no slashes)");
  }
  return cleaned;
}

const server = new McpServer({
  name: "agent-slack",
  version: "0.1.0",
});

server.registerTool(
  "health",
  {
    description: "Check whether the turbodb HTTP server is reachable.",
    inputSchema: {},
  },
  async () => {
    try {
      const r = await fetch(`${BASE}/health`);
      const text = await r.text();
      const payload = { ok: r.ok, status: r.status, body: text };
      await audit("health", {}, payload);
      return jsonResult(payload);
    } catch (e) {
      const payload = { ok: false, error: (e as Error).message };
      await audit("health", {}, payload);
      return jsonResult(payload);
    }
  },
);

server.registerTool(
  "chat_post",
  {
    description: `Post a chat message as ${AGENT_ID}. The from field is set by the MCP server.`,
    inputSchema: {
      to: z.string(),
      content: z.string(),
    },
  },
  async ({ to, content }) => {
    const input = { to, content };
    try {
      for (let attempt = 0; attempt < 10; attempt++) {
        const key = await nextChatKey();
        const value = { from: AGENT_ID, to, content, ts: Date.now() };
        const r = await insertDoc(CHAT_COL, key, value);
        if (r.ok) {
          const payload = { ok: true, key, from: AGENT_ID };
          await audit("chat_post", input, payload);
          return jsonResult(payload);
        }
        if (r.status !== 409) {
          const payload = { ok: false, status: r.status, error: r.body };
          await audit("chat_post", input, payload);
          return jsonResult(payload);
        }
      }
      const payload = { ok: false, error: "could not allocate unique msg key after retries" };
      await audit("chat_post", input, payload);
      return jsonResult(payload);
    } catch (e) {
      const payload = { ok: false, error: (e as Error).message };
      await audit("chat_post", input, payload);
      return jsonResult(payload);
    }
  },
);

server.registerTool(
  "chat_read",
  {
    description: "Read chat messages sorted by msg key. Optional since filters to keys greater than that value.",
    inputSchema: {
      since: z.string().optional(),
    },
  },
  async ({ since }) => {
    const input = { since };
    try {
      const scan = await scanCol(CHAT_COL, 500);
      let messages = scan.docs
        .filter((d) => d.key.startsWith("msg:"))
        .map((d) => ({
          key: d.key,
          from: String((d.value as { from?: unknown }).from ?? ""),
          to: String((d.value as { to?: unknown }).to ?? ""),
          content: String((d.value as { content?: unknown }).content ?? ""),
        }))
        .sort((a, b) => a.key.localeCompare(b.key));
      if (since) messages = messages.filter((m) => m.key > since);
      if (messages.length > 50) messages = messages.slice(-50);
      const payload = { ok: true, count: messages.length, messages };
      await audit("chat_read", input, payload);
      return jsonResult(payload);
    } catch (e) {
      const payload = { ok: false, error: (e as Error).message };
      await audit("chat_read", input, payload);
      return jsonResult(payload);
    }
  },
);

server.registerTool(
  "file_write",
  {
    description: "Write or replace one bare-filename file in the turbodb files collection.",
    inputSchema: {
      path: z.string(),
      content: z.string(),
    },
  },
  async ({ path, content }) => {
    const input = { path, content_length: content.length };
    try {
      const cleaned = cleanPath(path);
      const r = await upsertDoc(FILES_COL, cleaned, { content, by: AGENT_ID, ts: Date.now() });
      const payload = r.ok
        ? { ok: true, path: cleaned, by: AGENT_ID }
        : { ok: false, status: r.status, error: r.body };
      await audit("file_write", input, payload);
      return jsonResult(payload);
    } catch (e) {
      const payload = { ok: false, error: (e as Error).message };
      await audit("file_write", input, payload);
      return jsonResult(payload);
    }
  },
);

server.registerTool(
  "file_read",
  {
    description: "Read one file from the turbodb files collection.",
    inputSchema: {
      path: z.string(),
    },
  },
  async ({ path }) => {
    const input = { path };
    try {
      const cleaned = cleanPath(path);
      const d = await getDoc(FILES_COL, cleaned);
      if (!d) {
        const payload = { ok: false, not_found: true };
        await audit("file_read", input, payload);
        return jsonResult(payload);
      }
      const v = d.value as { content?: unknown; by?: unknown; ts?: unknown };
      const payload = {
        ok: true,
        path: cleaned,
        content: String(v.content ?? ""),
        by: String(v.by ?? ""),
        ts: typeof v.ts === "number" ? v.ts : 0,
      };
      await audit("file_read", input, payload);
      return jsonResult(payload);
    } catch (e) {
      const payload = { ok: false, error: (e as Error).message };
      await audit("file_read", input, payload);
      return jsonResult(payload);
    }
  },
);

server.registerTool(
  "file_list",
  {
    description: "List filenames and authors from the turbodb files collection.",
    inputSchema: {},
  },
  async () => {
      try {
        const scan = await scanCol(FILES_COL, 500);
        const byPath = new Map<string, { path: string; by: string; doc_id: number }>();
        for (const d of scan.docs) {
          const prev = byPath.get(d.key);
          if (prev && prev.doc_id > d.doc_id) continue;
          byPath.set(d.key, {
            path: d.key,
            by: String((d.value as { by?: unknown }).by ?? ""),
            doc_id: d.doc_id,
          });
        }
        const files = [...byPath.values()].map(({ path, by }) => ({ path, by }));
        const payload = { ok: true, count: files.length, files };
      await audit("file_list", {}, payload);
      return jsonResult(payload);
    } catch (e) {
      const payload = { ok: false, error: (e as Error).message };
      await audit("file_list", {}, payload);
      return jsonResult(payload);
    }
  },
);

const transport = new StdioServerTransport();
await server.connect(transport);
