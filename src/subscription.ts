import type { GraphDef, Store } from "@nicia-ai/typegraph";

import type { CheckpointBook } from "./checkpoint.js";
import { consume, type Projector } from "./consumer.js";
import {
  bodyText,
  HEADER_CONTENT_TYPE,
  type HttpEndpoint,
  JSON_CONTENT_TYPE,
  STATUS_CONFLICT,
  STATUS_CREATED,
  STATUS_NO_CONTENT,
  STATUS_OK,
} from "./http.js";
import { parseCompositeOffset } from "./offset.js";
import type { ShapeSource } from "./shape-source.js";

// --- Pull-wake subscriptions ------------------------------------------------
//
// A subscription lets the server tell us WHICH streams have pending work instead
// of polling every stream ourselves. A worker claims the subscription (taking a
// lease), drains the streams the server reports as pending, then acks and
// releases.
//
// The server-side `acked_offset` is NOT our resume cursor. It addresses whole
// appends, while our checkpoints address individual messages, and it is written
// after processing rather than atomically with it — so it can only ever be a
// wake-scheduling hint. The CheckpointBook stays authoritative for resumption;
// acking merely tells the server it need not wake us again for work we have
// already done. Understating progress therefore costs at most a redundant wake,
// which is why the ack carries the BASE of the last cursor (see `ackableOffset`).

const SUBSCRIPTIONS_PATH = "__ds/subscriptions";
const DEFAULT_LEASE_TTL_MS = 30_000;

/** Everything needed to address one subscription. */
export type SubscriptionRef = HttpEndpoint &
  Readonly<{
    /** The stream root URL that hosts the `__ds` endpoints, e.g. `"http://localhost:8791"`. */
    rootUrl: string;
    /** Subscription id — stable across restarts; re-confirming is idempotent. */
    id: string;
  }>;

/** One stream linked to a subscription, as the server reports it. */
export type PendingStream = Readonly<{
  /** Stream-root-relative path, e.g. `"agents/crm-agent"`. */
  path: string;
  linkType: "glob" | "explicit";
  ackedOffset: string | undefined;
  tailOffset: string | undefined;
  hasPending: boolean;
}>;

/** A held lease on a subscription. Every ack and release must echo it back. */
export type SubscriptionClaim = Readonly<{
  wakeId: string;
  generation: number;
  token: string;
  streams: readonly PendingStream[];
  leaseTtlMs: number;
}>;

/** The subscription was already claimed by another worker; `holder` names it when reported. */
export class SubscriptionClaimedError extends Error {
  readonly holder: string | undefined;
  readonly generation: number | undefined;

  constructor(id: string, holder: string | undefined, generation: number | undefined) {
    super(
      `Subscription "${id}" is already claimed${holder === undefined ? "" : ` by "${holder}"`}` +
        `${generation === undefined ? "" : ` at generation ${generation}`}. Another worker holds the lease; ` +
        `wait for the next wake rather than retrying immediately.`,
    );
    this.name = "SubscriptionClaimedError";
    this.holder = holder;
    this.generation = generation;
  }
}

/** The lease moved on (expired, or another worker took over) — the claim is void. */
export class SubscriptionFencedError extends Error {
  readonly wakeId: string;
  readonly generation: number;

  constructor(id: string, wakeId: string, generation: number, operation: string) {
    super(
      `Subscription "${id}" fenced the ${operation}: wake "${wakeId}" at generation ${generation} is no longer ` +
        `current. The lease expired or another worker claimed it, so this worker's progress was not recorded. ` +
        `Nothing is lost — the durable checkpoint still governs resumption — but stop writing under this claim.`,
    );
    this.name = "SubscriptionFencedError";
    this.wakeId = wakeId;
    this.generation = generation;
  }
}

/** An unexpected status from a subscription endpoint. */
export class SubscriptionRequestError extends Error {
  readonly status: number;

  constructor(operation: string, id: string, status: number, body: string) {
    super(
      `Subscription ${operation} for "${id}" failed with HTTP ${status}.` +
        (body === "" ? "" : ` Server said: ${body}`),
    );
    this.name = "SubscriptionRequestError";
    this.status = status;
  }
}

function subscriptionUrl(ref: SubscriptionRef, suffix = ""): string {
  const root = ref.rootUrl.endsWith("/") ? ref.rootUrl.slice(0, -1) : ref.rootUrl;
  return `${root}/${SUBSCRIPTIONS_PATH}/${encodeURIComponent(ref.id)}${suffix}`;
}

async function request(
  ref: SubscriptionRef,
  suffix: string,
  init: Readonly<{ method: string; body?: unknown; bearer?: string }>,
): Promise<Response> {
  const fetchClient = ref.fetchClient ?? fetch;
  const headers: Record<string, string> = { ...ref.headers };
  if (init.body !== undefined) headers[HEADER_CONTENT_TYPE] = JSON_CONTENT_TYPE;
  if (init.bearer !== undefined) headers["Authorization"] = `Bearer ${init.bearer}`;
  return await fetchClient(subscriptionUrl(ref, suffix), {
    method: init.method,
    headers,
    ...(init.body === undefined ? {} : { body: JSON.stringify(init.body) }),
  });
}

function readStreams(payload: unknown): readonly PendingStream[] {
  if (typeof payload !== "object" || payload === null || !("streams" in payload)) return [];
  const streams = (payload as { streams: unknown }).streams;
  if (!Array.isArray(streams)) return [];
  return streams.map((entry: Record<string, unknown>) => ({
    path: String(entry["path"]),
    linkType: entry["link_type"] === "explicit" ? "explicit" : "glob",
    ackedOffset: typeof entry["acked_offset"] === "string" ? entry["acked_offset"] : undefined,
    tailOffset: typeof entry["tail_offset"] === "string" ? entry["tail_offset"] : undefined,
    hasPending: entry["has_pending"] === true,
  }));
}

export type EnsureSubscriptionArgs = SubscriptionRef &
  Readonly<{
    /** Glob over stream paths — `*` matches one segment, `**` matches zero or more. */
    pattern?: string;
    /** Explicitly linked stream paths. At least one of `pattern` or `streams` is required. */
    streams?: readonly string[];
    /** Stream that wake events are written to, for the pull-wake flow. */
    wakeStream?: string;
    /** How long a claim holds the lease without a heartbeat. Defaults to 30s. */
    leaseTtlMs?: number;
    description?: string;
  }>;

/**
 * Create the subscription, or confirm an identical one already exists.
 *
 * Servers hash the normalized configuration, so re-running this with the same
 * arguments is idempotent; changing the pattern, streams, or lease without
 * changing the id is a `409` rather than a silent reconfiguration.
 */
export async function ensureSubscription(args: EnsureSubscriptionArgs): Promise<Readonly<{ created: boolean }>> {
  if (args.pattern === undefined && (args.streams === undefined || args.streams.length === 0)) {
    throw new Error(
      `ensureSubscription("${args.id}"): give at least one of \`pattern\` or \`streams\` — a subscription with ` +
        "neither matches no streams and would never wake.",
    );
  }
  const response = await request(args, "", {
    method: "PUT",
    body: {
      type: "pull",
      ...(args.pattern === undefined ? {} : { pattern: args.pattern }),
      ...(args.streams === undefined ? {} : { streams: [...args.streams] }),
      ...(args.wakeStream === undefined ? {} : { wake_stream: args.wakeStream }),
      lease_ttl_ms: args.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS,
      ...(args.description === undefined ? {} : { description: args.description }),
    },
  });
  if (response.status !== STATUS_CREATED && response.status !== STATUS_OK) {
    throw new SubscriptionRequestError("create", args.id, response.status, await bodyText(response));
  }
  return { created: response.status === STATUS_CREATED };
}

/**
 * Take the subscription's lease and learn which streams have pending work.
 *
 * Throws {@link SubscriptionClaimedError} when another worker holds it — that is
 * the normal outcome of a race between workers, not a failure of this one.
 */
export async function claimSubscription(
  ref: SubscriptionRef,
  worker: string,
): Promise<SubscriptionClaim> {
  const response = await request(ref, "/claim", { method: "POST", body: { worker } });
  if (response.status === STATUS_CONFLICT) {
    const body: unknown = await response.json().catch(() => undefined);
    const error = readErrorObject(body);
    throw new SubscriptionClaimedError(
      ref.id,
      typeof error?.["current_holder"] === "string" ? error["current_holder"] : undefined,
      typeof error?.["generation"] === "number" ? error["generation"] : undefined,
    );
  }
  if (response.status !== STATUS_OK) {
    throw new SubscriptionRequestError("claim", ref.id, response.status, await bodyText(response));
  }
  const payload = (await response.json()) as Record<string, unknown>;
  return {
    wakeId: String(payload["wake_id"]),
    generation: Number(payload["generation"]),
    token: String(payload["token"]),
    streams: readStreams(payload),
    leaseTtlMs: typeof payload["lease_ttl_ms"] === "number" ? payload["lease_ttl_ms"] : DEFAULT_LEASE_TTL_MS,
  };
}

function readErrorObject(body: unknown): Record<string, unknown> | undefined {
  if (typeof body !== "object" || body === null || !("error" in body)) return undefined;
  const error = (body as { error: unknown }).error;
  return typeof error === "object" && error !== null ? (error as Record<string, unknown>) : undefined;
}

/** One stream's progress, as reported to the server. */
export type StreamAck = Readonly<{ stream: string; offset: string }>;

/**
 * Record progress under a held claim.
 *
 * `done: false` extends the lease without ending the claim — the heartbeat for
 * work that outlives `leaseTtlMs`. `done: true` ends it; `nextWake` then tells
 * you whether the server already has more work queued.
 */
export async function ackSubscription(
  ref: SubscriptionRef,
  claim: SubscriptionClaim,
  acks: readonly StreamAck[],
  done: boolean,
): Promise<Readonly<{ nextWake: boolean }>> {
  const response = await request(ref, "/ack", {
    method: "POST",
    bearer: claim.token,
    body: { wake_id: claim.wakeId, generation: claim.generation, acks: [...acks], done },
  });
  if (response.status === STATUS_CONFLICT) {
    throw new SubscriptionFencedError(ref.id, claim.wakeId, claim.generation, "ack");
  }
  if (response.status !== STATUS_OK) {
    throw new SubscriptionRequestError("ack", ref.id, response.status, await bodyText(response));
  }
  const payload = (await response.json()) as Record<string, unknown>;
  return { nextWake: payload["next_wake"] === true };
}

/** Give the lease back without acking — the server re-wakes if work remains. */
export async function releaseSubscription(ref: SubscriptionRef, claim: SubscriptionClaim): Promise<void> {
  const response = await request(ref, "/release", {
    method: "POST",
    bearer: claim.token,
    body: { wake_id: claim.wakeId, generation: claim.generation },
  });
  if (response.status === STATUS_CONFLICT) {
    throw new SubscriptionFencedError(ref.id, claim.wakeId, claim.generation, "release");
  }
  if (response.status !== STATUS_NO_CONTENT && response.status !== STATUS_OK) {
    throw new SubscriptionRequestError("release", ref.id, response.status, await bodyText(response));
  }
}

/** Delete the subscription. In-flight acks against it then fail without advancing any cursor. */
export async function deleteSubscription(ref: SubscriptionRef): Promise<void> {
  const response = await request(ref, "", { method: "DELETE" });
  if (response.status !== STATUS_NO_CONTENT && response.status !== STATUS_OK) {
    throw new SubscriptionRequestError("delete", ref.id, response.status, await bodyText(response));
  }
}

/**
 * The whole-append offset to report for a checkpoint cursor.
 *
 * Our cursors address a message inside an append; the server's `acked_offset`
 * addresses the append. Reporting the base UNDERSTATES progress by at most the
 * messages consumed inside that append, which costs a redundant wake and never
 * a lost one. Rounding up would claim messages we may not have applied.
 */
function ackableOffset(cursor: string): string {
  return parseCompositeOffset(cursor)?.base ?? cursor;
}

export type ConsumeSubscribedArgs<G extends GraphDef, V = Record<string, unknown>> = Readonly<{
  subscription: SubscriptionRef;
  /** Identifies this worker in claim conflicts. */
  worker: string;
  /** Build the source for one stream the server reports as pending. */
  sourceFor: (stream: PendingStream) => ShapeSource<V>;
  /** Belief store to project into. MUST be created with `{ history: true }`. */
  store: Store<G>;
  checkpoints: CheckpointBook;
  project: Projector<G, V>;
  /** Transaction-size bound, forwarded to {@link consume}. */
  maxBatchSize?: number;
}>;

export type ConsumeSubscribedResult = Readonly<{
  streams: readonly Readonly<{ path: string; processed: number; lastOffset: string | undefined }>[];
  /** Whether the server has more work queued for the next wake. */
  nextWake: boolean;
}>;

/**
 * Claim the subscription, drain every stream it reports as pending, then ack and
 * end the claim.
 *
 * This replaces polling each stream on a timer: the server decides who has work.
 * It does not replace the checkpoint — each stream is still consumed through
 * {@link consume}, so a crash anywhere in here resumes from the durable cursor
 * and the lease simply expires.
 *
 * The lease is always given back: if consuming throws, the claim is released
 * (rather than acked) so another worker can pick the work up immediately instead
 * of waiting out `leaseTtlMs`.
 */
export async function consumeSubscribed<G extends GraphDef, V = Record<string, unknown>>(
  args: ConsumeSubscribedArgs<G, V>,
): Promise<ConsumeSubscribedResult> {
  const claim = await claimSubscription(args.subscription, args.worker);
  const pending = claim.streams.filter((stream) => stream.hasPending);
  const drained: { path: string; processed: number; lastOffset: string | undefined }[] = [];
  const acks: StreamAck[] = [];

  try {
    for (const stream of pending) {
      const result = await consume({
        source: args.sourceFor(stream),
        store: args.store,
        checkpoints: args.checkpoints,
        project: args.project,
        ...(args.maxBatchSize === undefined ? {} : { maxBatchSize: args.maxBatchSize }),
      });
      drained.push({ path: stream.path, processed: result.processed, lastOffset: result.lastOffset });
      if (result.lastOffset !== undefined) {
        acks.push({ stream: stream.path, offset: ackableOffset(result.lastOffset) });
      }
    }
  } catch (error) {
    // Hand the lease back rather than sitting on it until it expires. A release
    // that itself fails must not mask the real failure, so it is swallowed.
    await releaseSubscription(args.subscription, claim).catch(() => undefined);
    throw error;
  }

  const { nextWake } = await ackSubscription(args.subscription, claim, acks, true);
  return { streams: drained, nextWake };
}
