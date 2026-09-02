/**
 * A small HTTP + SSE server exposing one completed newsroom run as JSON, for
 * a UI to render. The run happens ONCE at startup — `desk/run.ts`'s `runDesk`
 * is the exact same call `main.ts` makes — and every endpoint below reads
 * from that single, already-finished result. This is a REPLAY server, not a
 * live one: `/api/events` streams the run's own timeline to whoever connects,
 * it does not push new events as they happen (there aren't any — the story
 * already ran to completion before the first request is served).
 *
 * See README.md's "HTTP contract" section for the authoritative endpoint list
 * — this file and that section must not drift; if you change one, change
 * both.
 *
 * Run with:  pnpm serve
 * Offline by default, same as `pnpm demo`; `ANTHROPIC_API_KEY` switches the
 * SAME run to live reporters (see README.md).
 */
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { asNodeId } from "@nicia-ai/typegraph";

import { REPORTERS, type ReporterId } from "../fixtures/dispatches.js";
import { provenanceBylines } from "./desk/editor.js";
import { closeDeskRun, type DeskEvent, type DeskRun, runDesk } from "./desk/run.js";
import { claimRows, storyRows, subjectRows } from "./desk/views.js";
import { runAsMain } from "./run-as-main.js";

const DEFAULT_PORT = 8879;

// ============================================================
// JSON shaping — plain data only, no store handles or branded types leak out
// ============================================================

async function reporterPayload(run: DeskRun, reporterId: ReporterId) {
  const materialization = run.newsroom.get(reporterId);
  if (materialization === undefined) throw new Error(`reporterPayload: no materialization for ${reporterId}`);
  const [claims, stories] = await Promise.all([claimRows(materialization.belief), storyRows(materialization.belief)]);
  return {
    id: reporterId,
    processed: materialization.result.processed,
    lastOffset: materialization.result.lastOffset ?? null,
    claims,
    stories,
  };
}

async function reportersPayload(run: DeskRun) {
  return Promise.all(REPORTERS.map((reporterId) => reporterPayload(run, reporterId)));
}

async function canonicalPayload(run: DeskRun) {
  const [subjects, claims, stories] = await Promise.all([
    subjectRows(run.canonical),
    claimRows(run.canonical),
    storyRows(run.canonical),
  ]);
  const bylineMap = await provenanceBylines(run.canonical, subjects.map((subject) => subject.id));
  const bylines: Record<string, readonly string[]> = Object.fromEntries(bylineMap);
  return { subjects, claims, stories, bylines };
}

/** The review queue / commit / refusal timeline, JSON-shaped. */
function reviewPayload(events: readonly DeskEvent[]) {
  return events.map((event) => {
    switch (event.type) {
      case "materialized":
        return event;
      case "review-queue":
        return { type: event.type, reporterId: event.reporterId, plan: event.plan };
      case "stale-refusal":
        return event;
      case "committed":
        return {
          type: event.type,
          reporterId: event.reporterId,
          merged: event.report.merged,
          conflicts: event.report.conflicts,
          resolutions: event.report.resolutions,
        };
    }
  });
}

async function forkPayload(run: DeskRun) {
  return {
    forkOffset: run.fork.forkOffset,
    trunkValue: run.fork.trunkValue ?? null,
    whatIfValue: run.fork.whatIfValue ?? null,
  };
}

function retractionPayload(run: DeskRun) {
  const { before, after, report } = run.retraction;
  return { before, after, report };
}

async function timelinePayload(run: DeskRun) {
  // A recorded-time scrubber's raw material: every reporter's checkpoint
  // anchors, plus canonical's revision right after the retraction — enough
  // for a UI to offer "as of…" points without re-deriving them.
  const reporters = await Promise.all(
    REPORTERS.map(async (reporterId) => {
      const materialization = run.newsroom.get(reporterId);
      if (materialization === undefined) throw new Error(`timelinePayload: no materialization for ${reporterId}`);
      return { reporterId, lastOffset: materialization.result.lastOffset ?? null };
    }),
  );
  return {
    reporters,
    forkOffset: run.fork.forkOffset,
    canonicalRecordedRevision: {
      beforeRetraction: run.retraction.beforeRecordedRevision ?? null,
      afterRetraction: run.retraction.afterRecordedRevision ?? null,
    },
  };
}

// ============================================================
// HTTP plumbing
// ============================================================

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body, null, 2);
  res.writeHead(status, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(payload);
}

function notFound(res: ServerResponse, message: string): void {
  sendJson(res, 404, { error: message });
}

/** Replays `run`'s own timeline over SSE, then ends the connection — see the file header. */
function streamEvents(run: DeskRun, res: ServerResponse): void {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "*",
  });
  const payload = reviewPayload(run.events);
  for (const event of payload) {
    res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
  }
  res.write(`event: fork\ndata: ${JSON.stringify({ forkOffset: run.fork.forkOffset, trunkValue: run.fork.trunkValue ?? null, whatIfValue: run.fork.whatIfValue ?? null })}\n\n`);
  res.write(`event: retracted\ndata: ${JSON.stringify(retractionPayload(run))}\n\n`);
  res.write(`event: done\ndata: {}\n\n`);
  res.end();
}

async function handleRequest(run: DeskRun, req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const path = url.pathname;

  if (path === "/api/health") {
    sendJson(res, 200, { status: "ok", mode: run.mode });
    return;
  }
  if (path === "/api/reporters") {
    sendJson(res, 200, await reportersPayload(run));
    return;
  }
  const reporterMatch = /^\/api\/reporters\/([^/]+)$/.exec(path);
  if (reporterMatch !== null) {
    const id = reporterMatch[1];
    const reporterId = REPORTERS.find((candidate) => candidate === id);
    if (reporterId === undefined) {
      notFound(res, `unknown reporter "${id ?? ""}"`);
      return;
    }
    sendJson(res, 200, await reporterPayload(run, reporterId));
    return;
  }
  if (path === "/api/canonical") {
    sendJson(res, 200, await canonicalPayload(run));
    return;
  }
  if (path === "/api/review") {
    sendJson(res, 200, reviewPayload(run.events));
    return;
  }
  if (path === "/api/fork") {
    sendJson(res, 200, await forkPayload(run));
    return;
  }
  if (path === "/api/retraction") {
    sendJson(res, 200, retractionPayload(run));
    return;
  }
  if (path === "/api/timeline") {
    sendJson(res, 200, await timelinePayload(run));
    return;
  }
  if (path === "/api/events") {
    streamEvents(run, res);
    return;
  }
  notFound(res, `no route for ${req.method ?? "GET"} ${path}`);
}

// ============================================================
// Main
// ============================================================

export async function main(): Promise<void> {
  console.log(`newsroom server: running the desk once (mode: ${process.env.ANTHROPIC_API_KEY === undefined ? "replay" : "live"})…`);
  const run = await runDesk();
  console.log(`newsroom server: desk run complete (mode: ${run.mode}). Starting HTTP server…`);

  const port = process.env.PORT === undefined ? DEFAULT_PORT : Number(process.env.PORT);
  const server = createServer((req, res) => {
    handleRequest(run, req, res).catch((error: unknown) => {
      console.error(error);
      sendJson(res, 500, { error: error instanceof Error ? error.message : "internal error" });
    });
  });

  server.listen(port, () => {
    console.log(`newsroom server: listening on http://localhost:${port} — see README.md for the endpoint list`);
  });

  process.on("SIGINT", () => {
    server.close();
    closeDeskRun(run)
      .catch((error: unknown) => console.error(error))
      .finally(() => process.exit(0));
  });
}

runAsMain(import.meta.url, main);
