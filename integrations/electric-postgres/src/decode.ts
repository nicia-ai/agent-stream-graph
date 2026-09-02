/**
 * Turns one `agent_events` row into the graph events it describes. PURE — no
 * store, no I/O, no knowledge of Electric or Postgres — unit-testable with
 * nothing running, the same contract as any `Decoder<G, V>` in the root repo
 * (see `examples/agents.ts`).
 *
 * `agentEventRowSchema` is the ONE validation boundary: `demo.ts`'s and
 * `seed.ts`'s `toChange` parse a raw Electric/pg row through it before this
 * module ever sees a value, so `decodeAgentEvent` itself never has to guard
 * against a malformed row — only against event_type/finding_id combinations
 * that are ill-formed at the DOMAIN level (see `finding_recorded` below).
 */
import type { Decoder, GraphEvent } from "@nicia-ai/agent-stream-graph";
import { z } from "zod";

import { fleetGraph } from "./graph.js";

const EVENT_TYPES = ["task_claimed", "task_progress", "task_completed", "task_failed", "finding_recorded"] as const;

/**
 * The `agent_events` row shape, straight off the wire — whether that wire is
 * Electric's `ChangeMessage.value` (live) or a plain pg row (the offline seed
 * script also decodes through this same schema, so both paths are proven
 * against one contract).
 *
 * `occurred_at` is typed as `z.string()`, not validated as ISO-8601 here: what
 * Postgres/Electric actually hand back for `timestamptz` is `"2026-08-28
 * 17:20:56.031413+00"` — space-separated, not `T`-separated, not
 * `Z`-terminated. That is NOT valid ISO-8601, and `agent-stream-graph`'s
 * `ValidTime.validFrom` requires ISO-8601. `toIsoInstant` below is the fix;
 * it belongs after this schema's validation, not inside it, because it must
 * run in `decodeAgentEvent` regardless of which layer parsed the raw row.
 */
export const agentEventRowSchema = z.object({
  agent_id: z.string(),
  agent_name: z.string(),
  task_id: z.string(),
  task_title: z.string(),
  event_type: z.enum(EVENT_TYPES),
  status: z.string(),
  finding_id: z.string().nullable(),
  finding_summary: z.string().nullable(),
  severity: z.string().nullable(),
  occurred_at: z.string(),
});

export type AgentEventRow = z.infer<typeof agentEventRowSchema>;

/**
 * Normalize a Postgres `timestamptz` text value to ISO-8601.
 *
 * DISCOVERED AGAINST A LIVE SERVICE: this is a real wire-format gap, not a
 * defensive guess — `curl`ing Electric's own `/v1/shape` endpoint returns
 * `"occurred_at":"2026-08-28 17:20:56.031413+00"` verbatim (see README.md,
 * "What we actually saw"). `new Date(...)` parses Postgres's space-separated
 * form leniently in Node/V8; `.toISOString()` is what actually produces the
 * ISO-8601 string `validFrom` requires.
 */
function toIsoInstant(pgTimestamp: string): string {
  const parsed = new Date(pgTimestamp);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`decodeAgentEvent: could not parse occurred_at "${pgTimestamp}" as a timestamp`);
  }
  return parsed.toISOString();
}

export const decodeAgentEvent: Decoder<typeof fleetGraph, AgentEventRow> = (change, emit) => {
  // agent_events is append-only by convention (see sql/schema.sql) — every
  // change Electric or the seed script produces for it is an insert. A
  // update/delete would mean the table stopped being append-only somewhere,
  // which this decoder has no rule for and should not guess at.
  if (change.operation !== "insert") {
    throw new Error(
      `decodeAgentEvent: agent_events is append-only; got unexpected "${change.operation}" for key "${change.key}"`,
    );
  }

  const row = change.value;
  const occurredAt = toIsoInstant(row.occurred_at);
  const valid = { validFrom: occurredAt };

  switch (row.event_type) {
    case "task_claimed":
      // A denormalized row: title/status are restated on every event for this
      // task (see sql/schema.sql), so a single row is always enough to fully
      // upsert both the Agent and the Task — no lookup against another row,
      // which is the whole point of a pure decoder.
      return [
        emit.nodes.Agent.upsert(row.agent_id, { name: row.agent_name, status: "active" }, valid),
        emit.nodes.Task.upsert(row.task_id, { title: row.task_title, status: row.status }, valid),
        emit.edges.assignedTo.upsert({ kind: "Task", id: row.task_id }, { kind: "Agent", id: row.agent_id }, undefined, valid),
      ];

    case "task_progress":
    case "task_completed":
    case "task_failed":
      return [emit.nodes.Task.upsert(row.task_id, { title: row.task_title, status: row.status }, valid)];

    case "finding_recorded": {
      // finding_id/finding_summary/severity are only populated for this
      // event_type (see sql/schema.sql's comment); a NULL here is a data
      // error, not something to paper over with a fallback value.
      if (row.finding_id === null || row.finding_summary === null || row.severity === null) {
        throw new Error(
          `decodeAgentEvent: "finding_recorded" event for task "${row.task_id}" is missing finding_id/finding_summary/severity`,
        );
      }
      return [
        emit.nodes.Finding.upsert(row.finding_id, { summary: row.finding_summary, severity: row.severity }, valid),
        emit.edges.aboutTask.upsert({ kind: "Finding", id: row.finding_id }, { kind: "Task", id: row.task_id }, undefined, valid),
      ];
    }
  }
};

/** Type-check helper: every `GraphEvent` this decoder can produce, for callers that want the union without importing `fleetGraph` directly. */
export type FleetGraphEvent = GraphEvent<typeof fleetGraph>;
