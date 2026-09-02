/**
 * The scenario `src/demo.ts` drives through the real OTel SDK to *generate*
 * spans — this file describes traffic, not spans. Nothing here is span JSON;
 * `demo.ts` turns each `Request` into real `startSpan`/`.end()` calls via
 * `NodeTracerProvider`, so the `SpanRecord`s the exporter buffers are
 * genuinely the SDK's own shapes, not hand-written fixtures wearing a
 * disguise.
 *
 * Deliberately plain data (no OTel imports) so it reads as a story on its
 * own: a service topology, told as "who called whom, spelled how."
 */

/** Which "who did we call" span attribute a hop's CLIENT span carries — see exporter.ts. */
export type PeerAttribute = "service.peer.name" | "peer.service";

export type CallStep = Readonly<{
  /** Raw `service.name` of the service this hop calls — deliberately messy across hops. */
  toRaw: string;
  operation: string;
  peerAttribute: PeerAttribute;
}>;

/** One inbound request: an entry service, and a chain of calls it makes (each hop calling the last). */
export type Request = Readonly<{
  entryRaw: string;
  entryOperation: string;
  calls: readonly CallStep[];
}>;

// ---- before the incident: normal traffic, told under two different spellings ----
// `checkout-svc` is the original deployment name; `checkout` is what it reads as
// after a routine rename. Both are the same logical service — the payoff is
// TypeGraph collapsing them into one `Service` node without being told to.

export const BEFORE_INCIDENT_REQUESTS: readonly Request[] = [
  // checkout (as checkout-svc) charges a card
  {
    entryRaw: "web-gateway",
    entryOperation: "POST /checkout",
    calls: [
      { toRaw: "checkout-svc", operation: "POST /checkout", peerAttribute: "service.peer.name" },
      { toRaw: "payments", operation: "ChargeCard", peerAttribute: "service.peer.name" },
    ],
  },
  // checkout (as checkout-svc) reserves stock
  {
    entryRaw: "web-gateway",
    entryOperation: "POST /checkout",
    calls: [
      { toRaw: "checkout-svc", operation: "POST /checkout", peerAttribute: "service.peer.name" },
      { toRaw: "inventory-svc", operation: "ReserveStock", peerAttribute: "peer.service" },
    ],
  },
  // checkout (renamed to checkout) charges a card
  {
    entryRaw: "web-gateway",
    entryOperation: "POST /checkout",
    calls: [
      { toRaw: "checkout", operation: "POST /checkout", peerAttribute: "service.peer.name" },
      { toRaw: "payments", operation: "ChargeCard", peerAttribute: "service.peer.name" },
    ],
  },
  // checkout (renamed to checkout) reserves stock
  {
    entryRaw: "web-gateway",
    entryOperation: "POST /checkout",
    calls: [
      { toRaw: "checkout", operation: "POST /checkout", peerAttribute: "service.peer.name" },
      { toRaw: "inventory-svc", operation: "ReserveStock", peerAttribute: "peer.service" },
    ],
  },
];

// ---- the incident window: a redeploy to `checkout.prod` starts a NEW
// dependency on `fraud-detection` that never existed before. This is the
// topology change the demo's `asOfRecorded` payoff has to catch.

export const INCIDENT_REQUESTS: readonly Request[] = [
  // checkout (now checkout.prod) starts scoring fraud risk — NEW dependency
  {
    entryRaw: "web-gateway",
    entryOperation: "POST /checkout",
    calls: [
      { toRaw: "checkout.prod", operation: "POST /checkout", peerAttribute: "service.peer.name" },
      { toRaw: "fraud-detection", operation: "ScoreRisk", peerAttribute: "service.peer.name" },
    ],
  },
  // checkout (checkout.prod) still charges a card
  {
    entryRaw: "web-gateway",
    entryOperation: "POST /checkout",
    calls: [
      { toRaw: "checkout.prod", operation: "POST /checkout", peerAttribute: "service.peer.name" },
      { toRaw: "payments", operation: "ChargeCard", peerAttribute: "service.peer.name" },
    ],
  },
];
