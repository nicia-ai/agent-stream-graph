import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

/**
 * An in-memory Durable Streams server covering the slice this package speaks:
 * JSON stream reads with `Stream-Next-Offset` / `Stream-Up-To-Date`, forking via
 * `Stream-Forked-From`, retention (`410 Gone`), and the pull-wake subscription
 * endpoints.
 *
 * Offsets are the count of messages before a position, zero-padded so they sort
 * lexicographically — the protocol requires opaque, lexicographically sortable,
 * strictly increasing tokens, and nothing here depends on the padding.
 *
 * Appends are recorded as CHUNKS, and one GET returns at most one chunk. That
 * makes batch boundaries deterministic, which is what lets the tests pin down
 * behaviour that must NOT depend on them ({@link DurableStreamsServer.coalesceChunks}).
 */

const OFFSET_WIDTH = 16;
const STREAM_START = "-1";
const CONTENT_TYPE_JSON = "application/json";
const HEADER_NEXT_OFFSET = "Stream-Next-Offset";
const HEADER_UP_TO_DATE = "Stream-Up-To-Date";
const HEADER_FORKED_FROM = "stream-forked-from";
const HEADER_FORK_OFFSET = "stream-fork-offset";
const HEADER_FORK_SUB_OFFSET = "stream-fork-sub-offset";
const SUBSCRIPTIONS_PREFIX = "/__ds/subscriptions/";
const DEFAULT_LEASE_TTL_MS = 30_000;

const STATUS_OK = 200;
const STATUS_CREATED = 201;
const STATUS_NO_CONTENT = 204;
const STATUS_BAD_REQUEST = 400;
const STATUS_NOT_FOUND = 404;
const STATUS_CONFLICT = 409;
const STATUS_GONE = 410;

function encodeOffset(count: number): string {
  return String(count).padStart(OFFSET_WIDTH, "0");
}

function decodeOffset(offset: string | undefined): number {
  if (offset === undefined || offset === STREAM_START) return 0;
  return Number.parseInt(offset, 10);
}

/**
 * Compile a subscription pattern, per the protocol's glob: `*` matches exactly
 * one path segment, `**` matches zero or more.
 *
 * Written out rather than pulled from a dependency because the whole point of
 * this stand-in is that it implements the protocol slice this package speaks
 * with nothing behind it to disagree with.
 */
function globToRegExp(pattern: string): RegExp {
  // Split on the glob tokens, keeping them (capturing group), then translate
  // each piece. `**` is listed before `*` so the alternation prefers the
  // longer token wherever both could start a match.
  const source = pattern
    .split(/(\*\*|\*)/)
    .map((piece) => (piece === "**" ? ".*" : piece === "*" ? "[^/]*" : piece.replace(/[.+?^${}()|[\]\\]/g, "\\$&")))
    .join("");
  return new RegExp(`^${source}$`);
}

type StreamState = {
  messages: unknown[];
  /** End index of each append; one GET never crosses one of these. */
  boundaries: number[];
  /** Messages before this index have been dropped by retention. */
  retainedFrom: number;
  forkedFrom?: { path: string; inherited: number };
};

type SubscriptionState = {
  /** Explicitly linked paths. */
  streams: string[];
  /** Glob over stream paths, matched live so streams created later still link. */
  pattern?: string;
  acked: Map<string, number>;
  leaseTtlMs: number;
  generation: number;
  holder?: { worker: string; wakeId: string; token: string };
};

export type DurableStreamsServer = Readonly<{
  /** Root URL, e.g. `http://127.0.0.1:54321`. */
  url: string;
  /** Full URL for a stream path. */
  streamUrl: (path: string) => string;
  createStream: (path: string) => void;
  /** Append one chunk of messages. One GET returns at most one chunk. */
  append: (path: string, items: readonly unknown[]) => void;
  /** Collapse every chunk boundary, so re-reads return everything in one batch. */
  coalesceChunks: (path: string) => void;
  /** Drop messages before `index`, so reads below it answer `410 Gone`. */
  retainFrom: (path: string, index: number) => void;
  createSubscription: (id: string, streams: readonly string[]) => void;
  ackedOffsetOf: (id: string, path: string) => string | undefined;
  close: () => Promise<void>;
}>;

export async function startDurableStreamsServer(): Promise<DurableStreamsServer> {
  const streams = new Map<string, StreamState>();
  const subscriptions = new Map<string, SubscriptionState>();
  let wakeCounter = 0;

  /**
   * The paths a subscription currently covers: explicit links first, then any
   * stream matching its pattern.
   *
   * Evaluated on every claim rather than frozen at creation, because that is
   * the whole point of a glob subscription — a stream created after the
   * subscription still has to wake it.
   */
  const linkedStreams = (state: SubscriptionState): readonly { path: string; linkType: string }[] => {
    const linked = new Map<string, string>();
    for (const path of state.streams) linked.set(path, "explicit");
    if (state.pattern !== undefined) {
      const matcher = globToRegExp(state.pattern);
      for (const path of streams.keys()) {
        if (matcher.test(path) && !linked.has(path)) linked.set(path, "pattern");
      }
    }
    return [...linked].map(([path, linkType]) => ({ path, linkType }));
  };

  const stateOf = (path: string): StreamState => {
    const state = streams.get(path);
    if (state === undefined) throw new Error(`fake durable streams: unknown stream "${path}"`);
    return state;
  };

  const server = createServer((request, response) => {
    void handle(request, response).catch((error: unknown) => {
      response.writeHead(500, { "Content-Type": "text/plain" });
      response.end(String(error));
    });
  });

  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const url = new URL(request.url ?? "/", "http://localhost");
    if (url.pathname.startsWith(SUBSCRIPTIONS_PREFIX)) {
      await handleSubscription(request, response, url);
      return;
    }
    const path = decodeURIComponent(url.pathname.replace(/^\//, ""));
    switch (request.method) {
      case "PUT":
        handlePut(request, response, path);
        return;
      case "GET":
        handleGet(response, path, url.searchParams.get("offset") ?? undefined);
        return;
      default:
        response.writeHead(STATUS_BAD_REQUEST).end();
    }
  }

  function handlePut(request: IncomingMessage, response: ServerResponse, path: string): void {
    const forkedFrom = request.headers[HEADER_FORKED_FROM];
    if (typeof forkedFrom !== "string") {
      if (streams.has(path)) {
        response.writeHead(STATUS_OK).end();
        return;
      }
      streams.set(path, { messages: [], boundaries: [], retainedFrom: 0 });
      response.writeHead(STATUS_CREATED).end();
      return;
    }
    const source = streams.get(forkedFrom);
    if (source === undefined) {
      response.writeHead(STATUS_NOT_FOUND).end(`no such source stream "${forkedFrom}"`);
      return;
    }
    const anchor = decodeOffset(readHeader(request, HEADER_FORK_OFFSET));
    const sub = Number.parseInt(readHeader(request, HEADER_FORK_SUB_OFFSET) ?? "0", 10);
    const inherited = anchor + sub;
    if (inherited > source.messages.length) {
      response
        .writeHead(STATUS_BAD_REQUEST)
        .end(`fork point ${inherited} is beyond the source tail ${source.messages.length}`);
      return;
    }
    const existing = streams.get(path);
    if (existing !== undefined) {
      const same = existing.forkedFrom?.path === forkedFrom && existing.forkedFrom.inherited === inherited;
      response.writeHead(same ? STATUS_OK : STATUS_CONFLICT).end();
      return;
    }
    // Copy-on-write is invisible to a reader: the fork simply reads the source's
    // prefix followed by its own appends, so materializing the prefix is a
    // faithful stand-in. Boundaries are inherited too, so a fork chunks exactly
    // as its source did up to the anchor.
    const inheritedBoundaries = source.boundaries.filter((boundary) => boundary <= inherited);
    if (inherited > 0 && inheritedBoundaries.at(-1) !== inherited) inheritedBoundaries.push(inherited);
    streams.set(path, {
      messages: source.messages.slice(0, inherited),
      boundaries: inheritedBoundaries,
      retainedFrom: 0,
      forkedFrom: { path: forkedFrom, inherited },
    });
    response.writeHead(STATUS_CREATED).end();
  }

  function handleGet(response: ServerResponse, path: string, offset: string | undefined): void {
    const state = streams.get(path);
    if (state === undefined) {
      response.writeHead(STATUS_NOT_FOUND).end();
      return;
    }
    // The start sentinel asks for "everything you still have", so retention
    // clamps it rather than killing it — that is what makes the documented
    // recovery from a `410` (drop the cursor, re-read from the start) possible
    // at all. Only an EXPLICIT offset below the retained window is `410 Gone`.
    const requestedStart = offset === undefined || offset === STREAM_START;
    const from = requestedStart ? state.retainedFrom : decodeOffset(offset);
    if (from < state.retainedFrom) {
      response.writeHead(STATUS_GONE).end(`offset ${offset} is before the earliest retained position`);
      return;
    }
    const until = state.boundaries.find((boundary) => boundary > from) ?? state.messages.length;
    const items = state.messages.slice(from, until);
    const upToDate = until >= state.messages.length;
    const headers: Record<string, string> = {
      "Content-Type": CONTENT_TYPE_JSON,
      [HEADER_NEXT_OFFSET]: encodeOffset(until),
    };
    if (upToDate) headers[HEADER_UP_TO_DATE] = "true";
    response.writeHead(STATUS_OK, headers).end(JSON.stringify(items));
  }

  async function handleSubscription(
    request: IncomingMessage,
    response: ServerResponse,
    url: URL,
  ): Promise<void> {
    const rest = url.pathname.slice(SUBSCRIPTIONS_PREFIX.length);
    const [rawId, action] = rest.split("/");
    const id = decodeURIComponent(rawId ?? "");
    const state = subscriptions.get(id);

    if (request.method === "PUT") {
      const body = await readJson(request);
      const linked = Array.isArray(body["streams"]) ? (body["streams"] as string[]) : [];
      if (state !== undefined) {
        response.writeHead(STATUS_OK).end();
        return;
      }
      const pattern = typeof body["pattern"] === "string" ? body["pattern"] : undefined;
      subscriptions.set(id, {
        streams: linked,
        ...(pattern === undefined ? {} : { pattern }),
        acked: new Map(),
        leaseTtlMs: typeof body["lease_ttl_ms"] === "number" ? body["lease_ttl_ms"] : DEFAULT_LEASE_TTL_MS,
        generation: 0,
      });
      response.writeHead(STATUS_CREATED).end();
      return;
    }
    if (state === undefined) {
      response.writeHead(STATUS_NOT_FOUND).end();
      return;
    }
    if (request.method === "DELETE") {
      subscriptions.delete(id);
      response.writeHead(STATUS_NO_CONTENT).end();
      return;
    }
    const body = await readJson(request);
    switch (action) {
      case "claim":
        handleClaim(response, id, state, String(body["worker"] ?? "unknown"));
        return;
      case "ack":
        handleAck(response, state, body);
        return;
      case "release":
        if (!isCurrentWake(state, body)) {
          response.writeHead(STATUS_CONFLICT).end(JSON.stringify({ error: { code: "FENCED" } }));
          return;
        }
        delete state.holder;
        response.writeHead(STATUS_NO_CONTENT).end();
        return;
      default:
        response.writeHead(STATUS_BAD_REQUEST).end();
    }
  }

  function handleClaim(
    response: ServerResponse,
    id: string,
    state: SubscriptionState,
    worker: string,
  ): void {
    if (state.holder !== undefined) {
      response.writeHead(STATUS_CONFLICT, { "Content-Type": CONTENT_TYPE_JSON }).end(
        JSON.stringify({
          error: { code: "ALREADY_CLAIMED", current_holder: state.holder.worker, generation: state.generation },
        }),
      );
      return;
    }
    wakeCounter += 1;
    state.generation += 1;
    const wakeId = `w_${wakeCounter}`;
    state.holder = { worker, wakeId, token: `token-${id}-${state.generation}` };
    response.writeHead(STATUS_OK, { "Content-Type": CONTENT_TYPE_JSON }).end(
      JSON.stringify({
        wake_id: wakeId,
        generation: state.generation,
        token: state.holder.token,
        lease_ttl_ms: state.leaseTtlMs,
        streams: linkedStreams(state).map(({ path, linkType }) => {
          const acked = state.acked.get(path) ?? 0;
          const tail = streams.get(path)?.messages.length ?? 0;
          return {
            path,
            link_type: linkType,
            acked_offset: encodeOffset(acked),
            tail_offset: encodeOffset(tail),
            has_pending: acked < tail,
          };
        }),
      }),
    );
  }

  function handleAck(response: ServerResponse, state: SubscriptionState, body: Record<string, unknown>): void {
    if (!isCurrentWake(state, body)) {
      response.writeHead(STATUS_CONFLICT, { "Content-Type": CONTENT_TYPE_JSON }).end(
        JSON.stringify({ error: { code: "FENCED" } }),
      );
      return;
    }
    const acks = Array.isArray(body["acks"]) ? (body["acks"] as Record<string, unknown>[]) : [];
    for (const ack of acks) {
      state.acked.set(String(ack["stream"]), decodeOffset(String(ack["offset"])));
    }
    if (body["done"] === true) delete state.holder;
    const nextWake = linkedStreams(state).some(
      ({ path }) => (state.acked.get(path) ?? 0) < (streams.get(path)?.messages.length ?? 0),
    );
    response
      .writeHead(STATUS_OK, { "Content-Type": CONTENT_TYPE_JSON })
      .end(JSON.stringify({ ok: true, next_wake: nextWake }));
  }

  function isCurrentWake(state: SubscriptionState, body: Record<string, unknown>): boolean {
    return (
      state.holder !== undefined &&
      state.holder.wakeId === body["wake_id"] &&
      state.generation === body["generation"]
    );
  }

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  return {
    url,
    streamUrl: (path) => `${url}/${path}`,
    createStream(path) {
      streams.set(path, { messages: [], boundaries: [], retainedFrom: 0 });
    },
    append(path, items) {
      const state = stateOf(path);
      state.messages.push(...items);
      state.boundaries.push(state.messages.length);
    },
    coalesceChunks(path) {
      const state = stateOf(path);
      state.boundaries = state.messages.length === 0 ? [] : [state.messages.length];
    },
    retainFrom(path, index) {
      stateOf(path).retainedFrom = index;
    },
    createSubscription(id, linked) {
      subscriptions.set(id, {
        streams: [...linked],
        acked: new Map(),
        leaseTtlMs: DEFAULT_LEASE_TTL_MS,
        generation: 0,
      });
    },
    ackedOffsetOf(id, path) {
      const acked = subscriptions.get(id)?.acked.get(path);
      return acked === undefined ? undefined : encodeOffset(acked);
    },
    close: () => closeServer(server),
  };
}

function readHeader(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return typeof value === "string" ? value : undefined;
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  if (chunks.length === 0) return {};
  const text = Buffer.concat(chunks).toString("utf8");
  return text === "" ? {} : (JSON.parse(text) as Record<string, unknown>);
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.closeAllConnections();
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });
}
