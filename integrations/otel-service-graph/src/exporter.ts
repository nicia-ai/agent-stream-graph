/**
 * The seam between the OpenTelemetry SDK and this library: a `SpanExporter`
 * that turns finished spans into a plain-JSON `SpanRecord`, plus a converter
 * from a buffer of those into the `ShapeChange` log `consume()` (via
 * `mockShapeSource` here, or a real durable stream in production) expects.
 *
 * Everything past this file — `decode.ts`, the graph, the tests — never
 * touches an OTel SDK type again. That is deliberate: a `SpanRecord` is a
 * plain object, so `decode.ts` stays unit-testable with nothing running,
 * exactly like the rest of this library's decoders.
 */
import type { Attributes, AttributeValue } from "@opentelemetry/api";
import { SpanKind } from "@opentelemetry/api";
import { type ExportResult, ExportResultCode } from "@opentelemetry/core";
import type { ReadableSpan, SpanExporter } from "@opentelemetry/sdk-trace-base";
import { ATTR_SERVER_ADDRESS, ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
// Both `peer.service` and its documented replacement `service.peer.name` live
// only under the `/incubating` subpath in the installed
// `@opentelemetry/semantic-conventions@1.43.0` — neither is exported from the
// package's stable default export. See `peerServiceNameOf` below for how
// that shapes the read order.
import { ATTR_PEER_SERVICE, ATTR_SERVICE_PEER_NAME } from "@opentelemetry/semantic-conventions/incubating";

import type { ShapeChange } from "@nicia-ai/agent-stream-graph";

export type SpanKindName = "INTERNAL" | "SERVER" | "CLIENT" | "PRODUCER" | "CONSUMER";

/** Plain-JSON projection of a `ReadableSpan` — the only span shape `decode.ts` ever sees. */
export type SpanRecord = Readonly<{
  traceId: string;
  spanId: string;
  parentSpanId: string | undefined;
  name: string;
  kind: SpanKindName;
  /** The emitting service — `resource.attributes["service.name"]`, never a span attribute. */
  serviceName: string;
  /** Logical name of the callee, if the span said one. See `peerServiceNameOf`. */
  peerServiceName: string | undefined;
  /** The remote host a CLIENT span addressed, if any — `server.address`. */
  serverAddress: string | undefined;
  startTime: string;
  endTime: string;
}>;

const SPAN_KIND_NAMES: Record<SpanKind, SpanKindName> = {
  [SpanKind.INTERNAL]: "INTERNAL",
  [SpanKind.SERVER]: "SERVER",
  [SpanKind.CLIENT]: "CLIENT",
  [SpanKind.PRODUCER]: "PRODUCER",
  [SpanKind.CONSUMER]: "CONSUMER",
};

function stringAttr(value: AttributeValue | undefined): string | undefined {
  return typeof value === "string" ? value : undefined;
}

/**
 * `peer.service` vs `service.peer.name`: OpenTelemetry's own docs contradict
 * each other on this pairing right now — one page marks `peer.service`
 * deprecated in favour of `service.peer.name`; another still lists
 * `service.peer.name` itself as Development status, i.e. not a settled
 * replacement. The installed package settles WHERE they live, if not which
 * one is "right": both are experimental, both live only under `/incubating`,
 * and `ATTR_PEER_SERVICE`'s own JSDoc there says `@deprecated Replaced by
 * service.peer.name`. So: prefer the newer name when a span carries it, fall
 * back to `peer.service` since it is what most instrumentation in the wild
 * still emits today.
 */
function peerServiceNameOf(attributes: Attributes): string | undefined {
  return stringAttr(attributes[ATTR_SERVICE_PEER_NAME]) ?? stringAttr(attributes[ATTR_PEER_SERVICE]);
}

function hrTimeToIso(hrTime: readonly [number, number]): string {
  const [seconds, nanos] = hrTime;
  return new Date(seconds * 1000 + nanos / 1_000_000).toISOString();
}

function toSpanRecord(span: ReadableSpan): SpanRecord {
  const serviceName = stringAttr(span.resource.attributes[ATTR_SERVICE_NAME]);
  if (serviceName === undefined) {
    throw new Error(`span "${span.name}" has no resource "${ATTR_SERVICE_NAME}" — every span's resource must carry one`);
  }
  return {
    traceId: span.spanContext().traceId,
    spanId: span.spanContext().spanId,
    parentSpanId: span.parentSpanContext?.spanId,
    name: span.name,
    kind: SPAN_KIND_NAMES[span.kind],
    serviceName,
    peerServiceName: peerServiceNameOf(span.attributes),
    serverAddress: stringAttr(span.attributes[ATTR_SERVER_ADDRESS]),
    startTime: hrTimeToIso(span.startTime),
    endTime: hrTimeToIso(span.endTime),
  };
}

/**
 * Buffers every span it is handed as a plain `SpanRecord`, in export order.
 * Register it on a `SimpleSpanProcessor` (or a `BatchSpanProcessor` in a real
 * deployment) so spans reach it as soon as they end.
 */
export class GraphSpanExporter implements SpanExporter {
  private readonly buffered: SpanRecord[] = [];

  export(spans: ReadableSpan[], resultCallback: (result: ExportResult) => void): void {
    try {
      for (const span of spans) this.buffered.push(toSpanRecord(span));
      resultCallback({ code: ExportResultCode.SUCCESS });
    } catch (error) {
      resultCallback({ code: ExportResultCode.FAILED, error: error instanceof Error ? error : new Error(String(error)) });
    }
  }

  async shutdown(): Promise<void> {}

  /** Every span exported so far, in export order. */
  get records(): readonly SpanRecord[] {
    return this.buffered;
  }
}

/**
 * Turns a buffer of `SpanRecord`s into the `ShapeChange` log `mockShapeSource`
 * (or a real ingest pipeline) consumes — spans are immutable once ended, so
 * every entry is an `insert`, offset by its position in the log.
 */
export function toShapeChanges(records: readonly SpanRecord[], shape: string): ShapeChange<SpanRecord>[] {
  return records.map((record, index) => ({
    offset: String(index + 1).padStart(4, "0"),
    shape,
    key: record.spanId,
    operation: "insert",
    value: record,
  }));
}
