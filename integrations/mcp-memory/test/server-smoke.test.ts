/**
 * Smoke-tests the actual stdio server process — not just the tool
 * functions it wraps. Spawns `src/server.ts`, speaks one real
 * `initialize` + `tools/list` exchange over stdin/stdout using the wire
 * protocol itself (newline-delimited JSON-RPC), and asserts the three
 * memory tools are listed. This is the difference between "it compiles"
 * and "it is an MCP server".
 */
import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterAll, describe, expect, it } from "vitest";

const PACKAGE_ROOT = fileURLToPath(new URL("..", import.meta.url));
// Verified against the installed @modelcontextprotocol/core@2.0.0 build's
// own `LATEST_PROTOCOL_VERSION` constant, not assumed from documentation.
const PROTOCOL_VERSION = "2025-11-25";

type JsonRpcMessage = Readonly<{
  jsonrpc: "2.0";
  id?: number;
  method?: string;
  params?: unknown;
  result?: unknown;
  error?: unknown;
}>;

type InitializeResult = Readonly<{ serverInfo: Readonly<{ name: string; version: string }> }>;
type ToolsListResult = Readonly<{ tools: readonly Readonly<{ name: string }>[] }>;

/** Feeds raw stdout chunks into a line-buffered JSON-RPC message parser —
 * the stdio transport frames one JSON-RPC message per line. */
function lineDelimitedJsonRpc(onMessage: (message: JsonRpcMessage) => void): (chunk: Buffer) => void {
  let buffered = "";
  return (chunk) => {
    buffered += chunk.toString("utf8");
    let newlineAt = buffered.indexOf("\n");
    while (newlineAt !== -1) {
      const line = buffered.slice(0, newlineAt).trim();
      buffered = buffered.slice(newlineAt + 1);
      if (line.length > 0) onMessage(JSON.parse(line) as JsonRpcMessage);
      newlineAt = buffered.indexOf("\n");
    }
  };
}

describe("stdio server smoke test", () => {
  const dataDir = mkdtempSync(join(tmpdir(), "asg-mcp-memory-smoke-"));
  let child: ChildProcessWithoutNullStreams | undefined;

  afterAll(() => {
    child?.kill();
    rmSync(dataDir, { recursive: true, force: true });
  });

  it("initializes and lists the three memory tools over real stdio JSON-RPC", async () => {
    child = spawn("npx", ["tsx", "src/server.ts"], {
      cwd: PACKAGE_ROOT,
      env: { ...process.env, ASG_MCP_MEMORY_DATA_DIR: dataDir },
    });
    const server = child;

    const messages: JsonRpcMessage[] = [];
    let stderr = "";
    server.stdout.on("data", lineDelimitedJsonRpc((message) => messages.push(message)));
    server.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");
    });

    const send = (message: JsonRpcMessage): void => {
      server.stdin.write(JSON.stringify(message) + "\n");
    };
    const waitForResponse = async (id: number, timeoutMs = 15_000): Promise<JsonRpcMessage> => {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const found = messages.find((m) => m.id === id);
        if (found !== undefined) return found;
        if (Date.now() > deadline) {
          throw new Error(`timed out waiting for response id=${id}. stderr so far:\n${stderr}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    };

    send({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: PROTOCOL_VERSION, capabilities: {}, clientInfo: { name: "smoke-test", version: "0.0.0" } },
    });
    const initResponse = await waitForResponse(1);
    expect(initResponse.error).toBeUndefined();
    const initResult = initResponse.result as InitializeResult;
    expect(initResult.serverInfo.name).toBe("asg-mcp-memory");

    send({ jsonrpc: "2.0", method: "notifications/initialized" });

    send({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} });
    const toolsResponse = await waitForResponse(2);
    expect(toolsResponse.error).toBeUndefined();
    const toolsResult = toolsResponse.result as ToolsListResult;
    const names = toolsResult.tools.map((t) => t.name).sort();
    expect(names).toEqual(["believedAt", "recall", "whySoFar"]);
  }, 20_000);
});
