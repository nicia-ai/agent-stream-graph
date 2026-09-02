/**
 * desk/views.ts — read-only query shapes shared by `main.ts` (console) and
 * `server.ts` (HTTP/JSON), so both presentations read canonical and each
 * reporter's belief the same way.
 */
import type { DeskHistoryStore } from "../backend.js";
import type { NewsroomGraph } from "../graph.js";

export type ClaimRow = Readonly<{ id: string; text: string; predicate: string; value: string; confidence: string }>;
export type StoryRow = Readonly<{ id: string; headline: string; status: string }>;
export type SubjectRow = Readonly<{ id: string; name: string; handle: string; role: string }>;

export async function claimRows(store: DeskHistoryStore<NewsroomGraph>): Promise<readonly ClaimRow[]> {
  return store
    .query()
    .from("Claim", "c")
    .select((c) => ({ id: c.c.id, text: c.c.text, predicate: c.c.predicate, value: c.c.value, confidence: c.c.confidence }))
    .execute();
}

export async function storyRows(store: DeskHistoryStore<NewsroomGraph>): Promise<readonly StoryRow[]> {
  return store
    .query()
    .from("Story", "s")
    .select((c) => ({ id: c.s.id, headline: c.s.headline, status: c.s.status }))
    .execute();
}

export async function subjectRows(store: DeskHistoryStore<NewsroomGraph>): Promise<readonly SubjectRow[]> {
  return store
    .query()
    .from("Subject", "s")
    .select((c) => ({ id: c.s.id, name: c.s.name, handle: c.s.handle, role: c.s.role }))
    .execute();
}
