/**
 * Worker entry point — accepts agent events over HTTP and routes them, by
 * agent id, to that agent's own `AgentMaterializer` Durable Object.
 *
 *   POST /agents/:agentId/events   { seq: number, events: GraphEvent[] }
 *     -> 200 { processed, fromOffset, lastOffset }   (see `ConsumeResult`)
 *     -> 400 on a malformed body
 *
 *   GET  /agents/:agentId
 *     -> 200 { agentId, entityCount }
 *
 * The routing IS half the argument for this deployment shape (the other half
 * is in `materializer.ts` and the README): `idFromName(agentId)` pins every
 * request for one agent onto the same Durable Object instance, so this
 * library's own documented limitation — "one consumer per belief store" — is
 * structural here, not an operational rule someone has to remember.
 */
import { eventBatchSchema, toGraphEvent } from "./graph.js";
import type { Env } from "./materializer.js";

export { AgentMaterializer } from "./materializer.js";

const AGENT_EVENTS_PATTERN = /^\/agents\/([^/]+)\/events$/;
const AGENT_PATTERN = /^\/agents\/([^/]+)$/;

/** Every request for one agent id routes to the same DO instance — see the module doc above. */
function materializerStub(agentId: string, env: Env) {
  return env.MATERIALIZER.get(env.MATERIALIZER.idFromName(agentId));
}

/** Extracts the `:agentId` path param, or `undefined` if `pathname` doesn't match `pattern`. */
function matchAgentId(pattern: RegExp, pathname: string): string | undefined {
  return pattern.exec(pathname)?.[1];
}

async function handleIngest(agentId: string, request: Request, env: Env): Promise<Response> {
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "request body must be JSON" }, { status: 400 });
  }
  const parsed = eventBatchSchema.safeParse(body);
  if (!parsed.success) {
    return Response.json({ error: "request body does not match { seq, events }", issues: parsed.error.issues }, { status: 400 });
  }
  const result = await materializerStub(agentId, env).ingest({
    seq: parsed.data.seq,
    events: parsed.data.events.map(toGraphEvent),
  });
  return Response.json(result);
}

async function handleSnapshot(agentId: string, env: Env): Promise<Response> {
  return Response.json(await materializerStub(agentId, env).snapshot());
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    const eventsAgentId = matchAgentId(AGENT_EVENTS_PATTERN, url.pathname);
    if (eventsAgentId !== undefined) {
      if (request.method !== "POST") return new Response("method not allowed", { status: 405 });
      return handleIngest(eventsAgentId, request, env);
    }

    const agentId = matchAgentId(AGENT_PATTERN, url.pathname);
    if (agentId !== undefined) {
      if (request.method !== "GET") return new Response("method not allowed", { status: 405 });
      return handleSnapshot(agentId, env);
    }

    return new Response("not found", { status: 404 });
  },
} satisfies ExportedHandler<Env>;
