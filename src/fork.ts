import {
  bodyText,
  HEADER_CONTENT_TYPE,
  type HttpEndpoint,
  JSON_CONTENT_TYPE,
  STATUS_CREATED,
  STATUS_OK,
} from "./http.js";
import { parseCompositeOffset } from "./offset.js";

// --- Stream forking ---------------------------------------------------------
//
// A fork is a copy-on-write branch of a durable stream: reads on the fork return
// the source's data up to the anchor, then the fork's own appends. That makes it
// the log-side half of fork/merge — two agents can diverge from one shared
// prefix — and TypeGraph's `mergeIncremental` is the half that reconciles the
// beliefs they build.
//
// `@durable-streams/client@0.2.6` exposes no fork API, so this speaks the
// protocol directly over `fetch`. That also keeps forking available when the
// optional client is not installed.

/** Where a fork branches from its source. */
export type ForkPoint = Readonly<{
  /** A server-issued base offset — the anchor the fork inherits up to. */
  offset: string;
  /** Additional flattened JSON messages past the anchor to inherit. */
  subOffset: number;
}>;

const HEADER_FORKED_FROM = "Stream-Forked-From";
const HEADER_FORK_OFFSET = "Stream-Fork-Offset";
const HEADER_FORK_SUB_OFFSET = "Stream-Fork-Sub-Offset";

/** The server refused a fork. `status` carries the protocol's meaning. */
export class StreamForkError extends Error {
  readonly status: number;
  readonly sourcePath: string;
  readonly targetUrl: string;

  constructor(status: number, sourcePath: string, targetUrl: string, body: string) {
    super(
      `Forking "${sourcePath}" into "${targetUrl}" failed with HTTP ${status}: ${describeForkStatus(status)}` +
        (body === "" ? "" : ` Server said: ${body}`),
    );
    this.name = "StreamForkError";
    this.status = status;
    this.sourcePath = sourcePath;
    this.targetUrl = targetUrl;
  }
}

function describeForkStatus(status: number): string {
  switch (status) {
    case 404:
      return "the source stream was not found.";
    case 400:
      return "the fork offset is beyond the source's tail, or an offset was malformed.";
    case 409:
      return "a stream already exists at the target with a different configuration, or the source is soft-deleted.";
    default:
      return "unexpected status.";
  }
}

/**
 * Convert a checkpoint cursor into the point to fork from.
 *
 * Cursors written by `durableStreamSource` are the protocol's own
 * `(offset, sub-offset)` pair, so the stream can be branched at exactly the
 * position a belief graph was last anchored to — no scanning, no approximation.
 */
export function forkPointFor(cursor: string): ForkPoint {
  const composite = parseCompositeOffset(cursor);
  if (composite === undefined) {
    throw new Error(
      `forkPointFor(): cursor "${cursor}" is not a composite offset, so it does not name a message-level ` +
        `position to branch at. Only cursors produced by durableStreamSource carry a sub-offset; an ` +
        `Electric shape cursor addresses a whole batch and cannot anchor a fork precisely.`,
    );
  }
  return { offset: composite.base, subOffset: composite.consumed };
}

export type ForkStreamArgs = HttpEndpoint &
  Readonly<{
    /** Full URL of the fork to create, e.g. `"http://localhost:8791/agents/crm-agent-what-if"`. */
    url: string;
    /**
     * The source stream's path RELATIVE TO THE STREAM ROOT (the protocol's
     * `Stream-Forked-From`), e.g. `"agents/crm-agent"` — not a full URL.
     */
    sourcePath: string;
    /** Where to branch — pass `forkPointFor(cursor)` to branch at a checkpoint. */
    at: ForkPoint;
  }>;

export type ForkStreamResult = Readonly<{
  url: string;
  /** `false` when an identical fork already existed — the call is idempotent. */
  created: boolean;
}>;

/**
 * Create a fork of a durable stream at {@link ForkStreamArgs.at}.
 *
 * Idempotent: re-forking with the same configuration returns `created: false`
 * rather than failing, so a crashed-and-restarted branch job converges. A fork
 * does NOT inherit the source's idempotent-producer state — it is a new stream
 * from a writer's point of view.
 */
export async function forkStream(args: ForkStreamArgs): Promise<ForkStreamResult> {
  const fetchClient = args.fetchClient ?? fetch;
  const response = await fetchClient(args.url, {
    method: "PUT",
    headers: {
      ...args.headers,
      [HEADER_FORKED_FROM]: args.sourcePath,
      [HEADER_FORK_OFFSET]: args.at.offset,
      [HEADER_FORK_SUB_OFFSET]: String(args.at.subOffset),
      [HEADER_CONTENT_TYPE]: JSON_CONTENT_TYPE,
    },
  });
  if (response.status !== STATUS_CREATED && response.status !== STATUS_OK) {
    throw new StreamForkError(response.status, args.sourcePath, args.url, await bodyText(response));
  }
  return { url: args.url, created: response.status === STATUS_CREATED };
}
