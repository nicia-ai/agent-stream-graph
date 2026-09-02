/**
 * Pure `Decoder`: one `SpanRecord` in, graph events out. No store, no I/O —
 * every case here is a deterministic function of the one span it is handed,
 * which is what makes it unit-testable (see `test/decode.test.ts`) with
 * nothing running — no store, no OTel SDK, not even a graph backend.
 */
import type { Decoder, GraphEvent } from "@nicia-ai/agent-stream-graph";

import { normalizeServiceIdentity, operationNodeId, serviceGraph } from "./graph.js";
import type { SpanRecord } from "./exporter.js";

export const decode: Decoder<typeof serviceGraph, SpanRecord> = (change, g) => {
  if (change.operation === "delete") {
    // A span is an immutable historical fact once it has ended — "deleting"
    // one from the source stream (say, an upstream de-duplication pass) has
    // no sensible graph meaning: the topology it evidenced doesn't un-happen.
    // A deliberate no-op, not a node/edge removal.
    return [];
  }

  const span = change.value;
  // Span timing IS valid time: `validFrom` on every event (not just closing
  // ones) is what lets the incident timeline in the demo work — see README.
  const validFrom = span.startTime;
  const canonicalId = normalizeServiceIdentity(span.serviceName);
  const opId = operationNodeId(canonicalId, span.name);

  const events: GraphEvent<typeof serviceGraph>[] = [
    g.nodes.Service.upsert(canonicalId, { name: canonicalId }, { validFrom }),
    g.nodes.ServiceAlias.upsert(span.serviceName, { observedName: span.serviceName }, { validFrom }),
    g.edges.aliasOf.upsert({ kind: "ServiceAlias", id: span.serviceName }, { kind: "Service", id: canonicalId }, undefined, { validFrom }),
    g.nodes.Operation.upsert(opId, { name: span.name }, { validFrom }),
    g.edges.performs.upsert({ kind: "Service", id: canonicalId }, { kind: "Operation", id: opId }, undefined, { validFrom }),
  ];

  // The call edge: only a CLIENT span names who it called, via whichever
  // "who did we call" attribute it actually carries (see exporter.ts for the
  // peer.service / service.peer.name ambiguity). SERVER/INTERNAL/PRODUCER/
  // CONSUMER spans don't name a callee the way a CLIENT span does, so they
  // contribute no `calls` edge.
  const calleeRawName = span.kind === "CLIENT" ? (span.peerServiceName ?? span.serverAddress) : undefined;
  if (calleeRawName !== undefined) {
    const calleeId = normalizeServiceIdentity(calleeRawName);
    events.push(
      g.nodes.Service.upsert(calleeId, { name: calleeId }, { validFrom }),
      g.edges.calls.upsert({ kind: "Service", id: canonicalId }, { kind: "Service", id: calleeId }, undefined, { validFrom }),
    );
  }

  return events;
};
