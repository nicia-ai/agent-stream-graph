/**
 * The service-dependency graph this package materializes from OpenTelemetry
 * spans: `Service` nodes (entity-resolved across whatever spelling a span's
 * `service.name` resource attribute happened to carry), `Operation` nodes
 * (an operation as observed on a service), and the `calls` edge between two
 * services — the actual topology this package exists to answer questions
 * about.
 */
import { defineEdge, defineGraph, defineNode } from "@nicia-ai/typegraph";
import { z } from "zod";

export const Service = defineNode("Service", {
  schema: z.object({ name: z.string() }),
});

/**
 * One raw spelling of a service's identity as it was actually observed on a
 * span — `checkout-svc`, `checkout`, `checkout.prod` are three `ServiceAlias`
 * rows that all resolve to the same canonical `Service`. Keeping every
 * observed spelling as its own row (rather than an array field on `Service`)
 * is what lets `aliasOf` retain all of them without a read-modify-write:
 * each span, decoded entirely on its own, upserts exactly the one alias row
 * it saw — `decode.ts` never needs to know what other spellings came before.
 */
export const ServiceAlias = defineNode("ServiceAlias", {
  schema: z.object({ observedName: z.string() }),
});

/** An operation as observed on a service — a span's own `name`, scoped by service. */
export const Operation = defineNode("Operation", {
  schema: z.object({ name: z.string() }),
});

const aliasOf = defineEdge("aliasOf", { schema: z.object({}) });
const performs = defineEdge("performs", { schema: z.object({}) });
const calls = defineEdge("calls", { schema: z.object({}) });

export const serviceGraph = defineGraph({
  id: "otel_service_graph",
  nodes: {
    Service: { type: Service },
    ServiceAlias: { type: ServiceAlias },
    Operation: { type: Operation },
  },
  edges: {
    aliasOf: { type: aliasOf, from: [ServiceAlias], to: [Service] },
    performs: { type: performs, from: [Service], to: [Operation] },
    calls: { type: calls, from: [Service], to: [Service] },
  },
});

/** `Operation` node id: an operation only makes sense scoped to the service that performs it. */
export function operationNodeId(canonicalServiceId: string, operationName: string): string {
  return `${canonicalServiceId}::${operationName}`;
}

// ---- entity resolution on the service identity ----

const ENVIRONMENT_SUFFIXES = [".prod", ".production", ".staging", ".stage", ".dev", ".development", ".local", ".internal"] as const;
const DEPLOYMENT_SUFFIXES = ["-svc", "-service", "-srv"] as const;
const STRIPPABLE_SUFFIXES: readonly string[] = [...ENVIRONMENT_SUFFIXES, ...DEPLOYMENT_SUFFIXES];

/**
 * Collapses the messy spellings real telemetry actually emits for one
 * logical service — environment suffixes (`checkout.prod`) and deployment
 * suffixes (`checkout-svc`) — down to one canonical id. Naive on purpose: a
 * fixed suffix list, not fuzzy matching, so it stays a pure, deterministic
 * function of the raw name alone — no store lookup, no similarity threshold
 * to tune, safe to call from a `Decoder` that sees one span at a time.
 *
 * The trade-off is real, not hidden: it will over-merge two genuinely
 * distinct services that happen to share a base name (an unrelated
 * `payments-svc` and a `payments.internal` dashboard would collapse into one
 * `payments` node). See the README's Limitations section.
 */
export function normalizeServiceIdentity(rawName: string): string {
  let normalized = rawName.trim().toLowerCase();
  let strippedSomething = true;
  while (strippedSomething) {
    strippedSomething = false;
    for (const suffix of STRIPPABLE_SUFFIXES) {
      if (normalized.length > suffix.length && normalized.endsWith(suffix)) {
        normalized = normalized.slice(0, -suffix.length);
        strippedSomething = true;
      }
    }
  }
  return normalized;
}
