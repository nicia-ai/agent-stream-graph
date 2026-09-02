#!/usr/bin/env node
/**
 * MCP server — exposes the entity-resolved, time-travelable, source-justified
 * belief graph in `graph.ts` as three tools: `recall`, `believedAt`, and
 * `whySoFar`. Every tool handler below is a thin wrapper over `tools.ts`;
 * `demo.ts` calls the exact same functions without an MCP client in the way.
 *
 * Run directly (`pnpm start`) or via an MCP client — see README.md for the
 * config block.
 */
import { McpServer } from "@modelcontextprotocol/server";
import { StdioServerTransport } from "@modelcontextprotocol/server/stdio";
import { z } from "zod";

import { openMemoryStore } from "./store.js";
import { believedAt, recall, whySoFar } from "./tools.js";

/** Every tool handler below returns its `tools.ts` result the same way —
 * pretty-printed JSON as the one text block MCP's `content` array expects. */
function jsonResult(value: unknown): { content: [{ type: "text"; text: string }] } {
  return { content: [{ type: "text", text: JSON.stringify(value, null, 2) }] };
}

async function main(): Promise<void> {
  // Overridable so a smoke test (or a second server instance) can point at
  // an isolated, disposable directory instead of the shared default under
  // `.data/server`.
  const dataDir = process.env.ASG_MCP_MEMORY_DATA_DIR;
  const memory = await openMemoryStore(dataDir === undefined ? {} : { dataDir });

  const server = new McpServer({ name: "asg-mcp-memory", version: "1.0.0" });

  server.registerTool(
    "recall",
    {
      title: "Recall an entity",
      description:
        "Resolve any name, alias, email, domain, or id to the canonical entity and what is currently believed about it — including every alias that has collapsed into it.",
      inputSchema: z.object({
        entity: z.string().describe('A name, alias, email, domain, or id to resolve, e.g. "J. Doe" or "jane.doe@acme.example".'),
      }),
    },
    async ({ entity }) => jsonResult(await recall(memory.store, entity)),
  );

  server.registerTool(
    "believedAt",
    {
      title: "What was believed at a stream offset",
      description:
        "Time travel: reconstructs what this memory believed once a given observation stream (agent) had reached a given offset — even if the current belief has since been corrected.",
      inputSchema: z.object({
        agent: z.string().describe('The observation stream name, e.g. "linkedin-scrape" or "crm-import".'),
        offset: z.string().describe("The stream offset to reconstruct belief as of."),
      }),
    },
    async ({ agent, offset }) => jsonResult(await believedAt(memory.store, memory.book, agent, offset)),
  );

  server.registerTool(
    "whySoFar",
    {
      title: "Why is this fact believed",
      description:
        "Provenance: walks the justification graph behind a fact (an entity plus a predicate, e.g. { entity: \"jane.doe@acme.example\", predicate: \"verified\" }) and reports every source that supports it and whether that support has been retracted.",
      inputSchema: z.object({
        entity: z.string().describe("A name, alias, email, or id identifying the entity the fact is about."),
        predicate: z.string().describe('The fact\'s predicate, e.g. "verified".'),
      }),
    },
    async ({ entity, predicate }) => jsonResult(await whySoFar(memory.store, entity, predicate)),
  );

  const transport = new StdioServerTransport();
  await server.connect(transport);

  const shutdown = (): void => {
    void memory.close().finally(() => process.exit(0));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
