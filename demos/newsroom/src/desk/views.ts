/**
 * desk/views.ts — read-only query shapes shared by `main.ts` (console) and
 * `server.ts` (HTTP/JSON), so both presentations read canonical and each
 * reporter's belief the same way.
 *
 * Each shape is defined once as a query BUILDER. Where the desk reads a single
 * shape at one narrative point, it takes an eager wrapper; where it assembles a
 * whole payload, it takes `deskRows` / `reporterRows`, which hand the same
 * builders to `store.batchOnce()` — one SQL statement instead of one per shape.
 */
import type { DeskHistoryStore } from "../backend.js";
import type { NewsroomGraph } from "../graph.js";

export type ClaimRow = Readonly<{ id: string; text: string; predicate: string; value: string; confidence: string }>;
export type StoryRow = Readonly<{ id: string; headline: string; status: string }>;
export type SubjectRow = Readonly<{ id: string; name: string; handle: string; role: string }>;

type Desk = DeskHistoryStore<NewsroomGraph>;

const claimQuery = (store: Desk) =>
  store
    .query()
    .from("Claim", "c")
    .select((c) => ({ id: c.c.id, text: c.c.text, predicate: c.c.predicate, value: c.c.value, confidence: c.c.confidence }));

const storyQuery = (store: Desk) =>
  store
    .query()
    .from("Story", "s")
    .select((c) => ({ id: c.s.id, headline: c.s.headline, status: c.s.status }));

const subjectQuery = (store: Desk) =>
  store
    .query()
    .from("Subject", "s")
    .select((c) => ({ id: c.s.id, name: c.s.name, handle: c.s.handle, role: c.s.role }));

export async function claimRows(store: Desk): Promise<readonly ClaimRow[]> {
  return claimQuery(store).execute();
}

export async function subjectRows(store: Desk): Promise<readonly SubjectRow[]> {
  return subjectQuery(store).execute();
}

/** Everything a canonical payload needs, in ONE statement. */
export async function deskRows(
  store: Desk,
): Promise<Readonly<{ subjects: readonly SubjectRow[]; claims: readonly ClaimRow[]; stories: readonly StoryRow[] }>> {
  const [subjects, claims, stories] = await store.batchOnce(() => [
    subjectQuery(store),
    claimQuery(store),
    storyQuery(store),
  ]);
  return { subjects, claims, stories };
}

/** Everything a reporter payload needs, in ONE statement. */
export async function reporterRows(
  store: Desk,
): Promise<Readonly<{ claims: readonly ClaimRow[]; stories: readonly StoryRow[] }>> {
  const [claims, stories] = await store.batchOnce(() => [claimQuery(store), storyQuery(store)]);
  return { claims, stories };
}
