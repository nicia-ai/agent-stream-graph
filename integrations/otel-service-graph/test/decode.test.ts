/**
 * `decode` is a pure function of one `SpanRecord` — no store, no OTel SDK,
 * no I/O anywhere in this file. Every case constructs a plain `SpanRecord`
 * literal directly (never a real `ReadableSpan`) and inspects the
 * `GraphEvent[]` it produces.
 */
import { graphEmitter, OP_EDGE_UPSERT, OP_NODE_UPSERT, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { describe, expect, it } from "vitest";

import { decode } from "../src/decode.js";
import type { SpanRecord } from "../src/exporter.js";
import { serviceGraph } from "../src/graph.js";

const emit = graphEmitter(serviceGraph);

function spanChange(offset: string, span: Partial<SpanRecord> & Pick<SpanRecord, "spanId" | "serviceName" | "name" | "kind">): ShapeChange<SpanRecord> {
  const value: SpanRecord = {
    traceId: "trace-1",
    parentSpanId: undefined,
    peerServiceName: undefined,
    serverAddress: undefined,
    startTime: "2026-01-01T00:00:00.000Z",
    endTime: "2026-01-01T00:00:01.000Z",
    ...span,
  };
  return { offset, shape: "span", key: span.spanId, operation: "insert", value };
}

describe("decode", () => {
  it("upserts a Service, ServiceAlias, aliasOf edge, Operation, and performs edge for an INTERNAL span", () => {
    const change = spanChange("001", { spanId: "s1", serviceName: "checkout-svc", name: "process", kind: "INTERNAL" });
    const events = decode(change, emit);

    expect(events).toHaveLength(5);
    expect(events).toContainEqual(emit.nodes.Service.upsert("checkout", { name: "checkout" }, { validFrom: change.value.startTime }));
    expect(events).toContainEqual(
      emit.nodes.ServiceAlias.upsert("checkout-svc", { observedName: "checkout-svc" }, { validFrom: change.value.startTime }),
    );
    expect(events).toContainEqual(
      emit.edges.aliasOf.upsert(
        { kind: "ServiceAlias", id: "checkout-svc" },
        { kind: "Service", id: "checkout" },
        undefined,
        { validFrom: change.value.startTime },
      ),
    );
    expect(events).toContainEqual(emit.nodes.Operation.upsert("checkout::process", { name: "process" }, { validFrom: change.value.startTime }));
    // No CLIENT span, no callee named -> no `calls` edge.
    expect(events.some((event) => event.op === OP_EDGE_UPSERT && event.kind === "calls")).toBe(false);
  });

  it("normalizes environment and deployment suffixes to the same canonical Service id", () => {
    const spellings = ["checkout-svc", "checkout", "checkout.prod"];
    const canonicalIds = spellings.map((serviceName, i) => {
      const change = spanChange(String(i + 1).padStart(3, "0"), { spanId: `s${i}`, serviceName, name: "op", kind: "INTERNAL" });
      const upsert = decode(change, emit).find((event) => event.op === OP_NODE_UPSERT && event.kind === "Service");
      if (upsert === undefined || upsert.op !== OP_NODE_UPSERT) throw new Error("expected a Service upsert event");
      return upsert.id;
    });
    expect(new Set(canonicalIds)).toEqual(new Set(["checkout"]));
  });

  it("emits a calls edge from a CLIENT span carrying service.peer.name, using the normalized callee id", () => {
    const change = spanChange("001", {
      spanId: "s1",
      serviceName: "checkout.prod",
      name: "ChargeCard",
      kind: "CLIENT",
      peerServiceName: "payments-svc",
    });
    const events = decode(change, emit);
    expect(events).toContainEqual(
      emit.edges.calls.upsert({ kind: "Service", id: "checkout" }, { kind: "Service", id: "payments" }, undefined, { validFrom: change.value.startTime }),
    );
  });

  it("falls back to server.address when a CLIENT span has no peer service name", () => {
    const change = spanChange("001", {
      spanId: "s1",
      serviceName: "checkout",
      name: "ReserveStock",
      kind: "CLIENT",
      serverAddress: "inventory-svc",
    });
    const events = decode(change, emit);
    expect(events).toContainEqual(
      emit.edges.calls.upsert({ kind: "Service", id: "checkout" }, { kind: "Service", id: "inventory" }, undefined, { validFrom: change.value.startTime }),
    );
  });

  it("emits no calls edge from a CLIENT span with neither peer service name nor server address", () => {
    const change = spanChange("001", { spanId: "s1", serviceName: "checkout", name: "Mystery", kind: "CLIENT" });
    const events = decode(change, emit);
    expect(events.some((event) => event.op === OP_EDGE_UPSERT && event.kind === "calls")).toBe(false);
  });

  it("carries the span's own start time into validFrom, not ingest time", () => {
    const change = spanChange("001", {
      spanId: "s1",
      serviceName: "checkout",
      name: "op",
      kind: "INTERNAL",
      startTime: "2020-06-15T12:00:00.000Z",
    });
    const events = decode(change, emit);
    const upsert = events.find((event) => event.op === OP_NODE_UPSERT && event.kind === "Service");
    if (upsert === undefined || upsert.op !== OP_NODE_UPSERT) throw new Error("expected a Service upsert event");
    expect(upsert.validFrom).toBe("2020-06-15T12:00:00.000Z");
  });

  it("treats a delete change as a no-op — a span is an immutable historical fact", () => {
    const change: ShapeChange<SpanRecord> = { ...spanChange("001", { spanId: "s1", serviceName: "checkout", name: "op", kind: "INTERNAL" }), operation: "delete" };
    expect(decode(change, emit)).toEqual([]);
  });
});
