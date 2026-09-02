/**
 * Live reporters — the SAME `ShapeSource<ReporterEvent>` shape as
 * `reporters/replay.ts`, produced by real Claude agents instead of a checked-in
 * transcript.
 *
 * Each reporter is a single, tool-free `query()` call (`tools: []` — this is a
 * writer, not an agent that touches the filesystem or a shell) given the same
 * underlying source documents `fixtures/dispatches.ts` encodes as structured
 * events, and asked to file its OWN structured events in response. The model
 * is not shown the fixtures — only the prose "documents" a reporter working
 * this beat would have read — so a live run is a genuinely independent
 * production of the same shape, not a re-serialization of the offline one.
 *
 * Gated on `ANTHROPIC_API_KEY`: `desk/run.ts` only reaches this module when
 * the key is set. `liveSource` also checks it directly, so importing this
 * module without a key fails loudly on first read rather than hanging on a
 * doomed subprocess spawn.
 *
 * HONESTY NOTE: this really does call the Claude Agent SDK and really does
 * parse its output into graph events — it is not a stub. What it cannot be is
 * DETERMINISTIC: a live model may phrase a claim differently, omit one, or
 *(rarely) return JSON this module's schema rejects, in which case the error
 * names which reporter and what shape check failed. No API key was available
 * while building this package, so this path is unverified end-to-end against
 * a live model — see the README.
 */
import { mockShapeSource, type ShapeSource } from "@nicia-ai/agent-stream-graph";
import { query } from "@anthropic-ai/claude-agent-sdk";
import { z } from "zod";

import type { ReporterId } from "../../fixtures/dispatches.js";
import type { ClaimEvent, ReporterEvent, SourceEvent, StoryEvent, SubjectRef } from "../decode.js";
import { toReporterEventChanges } from "./shape.js";

const DEFAULT_MODEL = "claude-sonnet-5";

// ============================================================
// The world a live reporter is told about — prose, not events. Producing
// SourceEvent/ClaimEvent/StoryEvent JSON from this is the model's own work.
// ============================================================

const FILING_DOCUMENT =
  "Q1 capital budget filing, City Clerk's office: the fleet modernisation contract was awarded to " +
  "Halcyon Transit Systems (@halcyon). The filing states the contract's value as $41,000,000 and is " +
  "signed off by procurement chief M. Vance.";

const MINUTES_DOCUMENT =
  "Transit council minutes, 14 March: council reviewed the fleet modernisation award to Halcyon " +
  "Transit Systems (@halcyon). Following a March revision, the minutes record the contract's value as " +
  "$38,000,000, approved by procurement director Marisa Vance.";

const TIP_DOCUMENT =
  "An anonymous source, handle 'insider-01', describes an undisclosed payment to a council aide around " +
  "the time of the award and calls the procurement process compromised. Uncorroborated by any document.";

type ReporterBrief = Readonly<{
  persona: string;
  documents: readonly Readonly<{ id: string; text: string }>[];
}>;

const BRIEFS: Readonly<Record<ReporterId, ReporterBrief>> = {
  "reporter-ash": {
    persona:
      "You are Ash, a reporter who works from primary documents. You have read the budget filing and " +
      "picked up an anonymous tip on the same story. Report the award and its value straight from the " +
      "filing; treat the tip as context you are not ready to publish as its own claim.",
    documents: [
      { id: "wire:budget-filing", text: FILING_DOCUMENT },
      { id: "tip:insider-01", text: TIP_DOCUMENT },
    ],
  },
  "reporter-brook": {
    persona:
      "You are Brook, a reporter who cross-references the budget filing against the council's own " +
      "minutes. Where the minutes revise a figure the filing gave, report the minutes' number — that is " +
      "the more recent primary document.",
    documents: [
      { id: "wire:budget-filing", text: FILING_DOCUMENT },
      { id: "wire:council-minutes", text: MINUTES_DOCUMENT },
    ],
  },
  "reporter-cass": {
    persona:
      "You are Cass, a reporter working an anonymous tip alone, with no primary document to corroborate " +
      "it. File the allegation as a single-source claim and a cautious draft story — do not claim more " +
      "certainty than one uncorroborated tip supports.",
    documents: [{ id: "tip:insider-01", text: TIP_DOCUMENT }],
  },
};

const OUTPUT_CONTRACT = `
Respond with ONLY a single JSON array (no prose, no markdown code fences) of "reporter events". Each
element is exactly one of these four shapes — the "type" field selects which:

{"type":"wire","id":"<a source id from your documents>","label":"<short description>","outlet":"<who published it>"}
{"type":"tip","id":"<a source id from your documents>","label":"<short description>","handle":"<the tipster's handle>"}
{"type":"claim","id":"<a new, unique slug>","text":"<one sentence you stand behind>","predicate":"<short predicate name, e.g. contractValue>","value":"<the value, e.g. $41M>","confidence":"<e.g. confirmed, single-source>","subject":{"id":"<slug>","name":"<display name>","handle":"<@handle>","role":"<their role>"},"sources":["<ids of wire/tip events you are also emitting>"],"rule":"<why this follows, e.g. primary document>","validFrom":"<ISO-8601 instant>"}
{"type":"story","id":"<a new, unique slug>","headline":"<headline>","status":"<e.g. draft>","claims":["<claim ids you are also emitting>"],"rule":"<why this follows>","validFrom":"<ISO-8601 instant>"}

Rules:
- Emit a "wire" or "tip" event for every document id you were given, using that EXACT id.
- Every id you list in a claim's "sources" or a story's "claims" MUST be the id of another event you are
  also emitting in this same array.
- "validFrom" must be a full ISO-8601 instant, e.g. "2026-03-01T00:00:00.000Z".
- Only file a "story" if you have a claim ready to publish — not every reporter files one.
`.trim();

function buildPrompt(brief: ReporterBrief): string {
  const documents = brief.documents.map((doc) => `  [${doc.id}] ${doc.text}`).join("\n");
  return (
    `${brief.persona}\n\nYour documents:\n${documents}\n\n${OUTPUT_CONTRACT}`
  );
}

// ============================================================
// Structural validation of the model's JSON — never trust a live response.
// ============================================================

const subjectRefSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  handle: z.string().min(1),
  role: z.string().min(1),
});

const wireEventSchema = z.object({
  type: z.literal("wire"),
  id: z.string().min(1),
  label: z.string().min(1),
  outlet: z.string().min(1),
});

const tipEventSchema = z.object({
  type: z.literal("tip"),
  id: z.string().min(1),
  label: z.string().min(1),
  handle: z.string().min(1),
});

const claimEventSchema = z.object({
  type: z.literal("claim"),
  id: z.string().min(1),
  text: z.string().min(1),
  predicate: z.string().min(1),
  value: z.string().min(1),
  confidence: z.string().min(1),
  subject: subjectRefSchema,
  sources: z.array(z.string().min(1)).min(1),
  rule: z.string().min(1),
  validFrom: z.string().min(1),
  validTo: z.string().min(1).optional(),
});

const storyEventSchema = z.object({
  type: z.literal("story"),
  id: z.string().min(1),
  headline: z.string().min(1),
  status: z.string().min(1),
  claims: z.array(z.string().min(1)).min(1),
  rule: z.string().min(1),
  validFrom: z.string().min(1),
});

const reporterEventSchema = z.discriminatedUnion("type", [
  wireEventSchema,
  tipEventSchema,
  claimEventSchema,
  storyEventSchema,
]);
const reporterEventsSchema = z.array(reporterEventSchema).min(1);

type ParsedEvent = z.infer<typeof reporterEventSchema>;
type ParsedClaimEvent = Extract<ParsedEvent, { type: "claim" }>;

function toSubjectRef(parsed: ParsedClaimEvent["subject"]): SubjectRef {
  return { id: parsed.id, name: parsed.name, handle: parsed.handle, role: parsed.role };
}

/** Reassembled explicitly, never trusted structurally, so `exactOptionalPropertyTypes` holds. */
function toReporterEvent(parsed: ParsedEvent): ReporterEvent {
  switch (parsed.type) {
    case "wire":
    case "tip":
      return parsed satisfies SourceEvent;
    case "story":
      return parsed satisfies StoryEvent;
    case "claim": {
      const base = {
        type: "claim" as const,
        id: parsed.id,
        text: parsed.text,
        predicate: parsed.predicate,
        value: parsed.value,
        confidence: parsed.confidence,
        subject: toSubjectRef(parsed.subject),
        sources: parsed.sources,
        rule: parsed.rule,
        validFrom: parsed.validFrom,
      };
      const event: ClaimEvent = parsed.validTo === undefined ? base : { ...base, validTo: parsed.validTo };
      return event;
    }
  }
}

// ============================================================
// Extracting JSON from a model's free-form text response
// ============================================================

function stripFences(text: string): string | undefined {
  const match = /```(?:json)?\s*([\s\S]*?)```/.exec(text);
  return match?.[1];
}

function firstBracketed(text: string): string | undefined {
  const start = text.indexOf("[");
  const end = text.lastIndexOf("]");
  return start === -1 || end === -1 || end < start ? undefined : text.slice(start, end + 1);
}

function extractReporterEvents(reporterId: ReporterId, responseText: string): readonly ReporterEvent[] {
  const trimmed = responseText.trim();
  const candidates = [trimmed, stripFences(trimmed), firstBracketed(trimmed)].filter(
    (candidate): candidate is string => candidate !== undefined,
  );
  for (const candidate of candidates) {
    let json: unknown;
    try {
      json = JSON.parse(candidate);
    } catch {
      continue;
    }
    const parsed = reporterEventsSchema.safeParse(json);
    if (parsed.success) {
      return parsed.data.map(toReporterEvent);
    }
  }
  throw new Error(
    `liveSource(${reporterId}): the model's response did not contain a JSON array matching the reporter-event ` +
      `schema. Raw response:\n${trimmed}`,
  );
}

// ============================================================
// Running the reporter agent
// ============================================================

async function runReporterAgent(reporterId: ReporterId, brief: ReporterBrief): Promise<readonly ReporterEvent[]> {
  const model = process.env.ANTHROPIC_MODEL ?? DEFAULT_MODEL;
  const stream = query({
    prompt: buildPrompt(brief),
    options: {
      model,
      tools: [],
      maxTurns: 4,
    },
  });

  let resultText: string | undefined;
  for await (const message of stream) {
    if (message.type === "result") {
      if (message.subtype !== "success") {
        throw new Error(
          `liveSource(${reporterId}): query() ended without success (subtype="${message.subtype}", ` +
            `stop_reason=${message.stop_reason ?? "unknown"}).`,
        );
      }
      resultText = message.result;
    }
  }
  if (resultText === undefined) {
    throw new Error(`liveSource(${reporterId}): query() completed with no result message.`);
  }
  return extractReporterEvents(reporterId, resultText);
}

/**
 * A live `ShapeSource<ReporterEvent>` for one reporter. Throws immediately —
 * before spawning any subprocess — when `ANTHROPIC_API_KEY` is unset, so a
 * caller that reaches this module without a key fails with a clear message
 * rather than a hung or cryptic subprocess error.
 */
export function liveSource(reporterId: ReporterId): ShapeSource<ReporterEvent> {
  if (process.env.ANTHROPIC_API_KEY === undefined) {
    throw new Error(
      `liveSource(${reporterId}): ANTHROPIC_API_KEY is not set. The live path calls the Claude Agent SDK and ` +
        `needs real credentials — run without it (the default) to use the offline replay fixtures instead.`,
    );
  }

  const brief = BRIEFS[reporterId];
  let inner: Promise<ShapeSource<ReporterEvent>> | undefined;

  async function ensureInner(): Promise<ShapeSource<ReporterEvent>> {
    inner ??= runReporterAgent(reporterId, brief).then((events) =>
      mockShapeSource(reporterId, toReporterEventChanges(events)),
    );
    return inner;
  }

  return {
    name: reporterId,
    async read(after) {
      const source = await ensureInner();
      return source.read(after);
    },
  };
}
