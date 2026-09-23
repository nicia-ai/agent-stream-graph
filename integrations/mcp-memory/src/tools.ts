/**
 * The three memory tools, as plain functions over an open `MemoryStore`.
 * `server.ts` wraps these for MCP; `demo.ts` and the tests call them
 * directly — there is exactly one implementation of each tool's logic.
 */
import type { CheckpointBook } from "@nicia-ai/agent-stream-graph";
import { asNodeId, recordedInstantRevision } from "@nicia-ai/typegraph";

import { factId, VERIFIED_PREDICATE } from "./graph.js";
import type { MemoryStore } from "./store.js";

export type PersonSnapshot = Readonly<{
  kind: "Person";
  id: string;
  name: string;
  email: string;
  title: string;
  aliases: readonly string[];
}>;
export type OrgSnapshot = Readonly<{ kind: "Org"; id: string; name: string; domain: string; aliases: readonly string[] }>;
export type ResolvedEntity = PersonSnapshot | OrgSnapshot;

/**
 * Entity resolution: given ANY handle a source has used for an entity — its
 * stable key (email/domain), its display name, or one of the name spellings
 * folded into `aliases` by the projector in `store.ts` — returns the one
 * canonical row. This is what lets `recall("J. Doe")` and `recall("Jane Doe")`
 * answer with the same person.
 *
 * The stable key needs no rung of its own: `personId`/`orgId` derive the id
 * from it, trimmed and lowercased, so matching the id against the normalized
 * handle IS the case-insensitive email/domain match.
 */
async function resolveEntity(store: MemoryStore, handle: string): Promise<ResolvedEntity | undefined> {
  const trimmed = handle.trim();
  const normalizedKey = trimmed.toLowerCase();

  const [person] = await store
    .query()
    .from("Person", "p")
    .whereNode("p", (p) => p.id.eq(normalizedKey).or(p.name.eq(trimmed)).or(p.aliases.contains(trimmed)))
    .select((c) => ({ id: c.p.id, name: c.p.name, email: c.p.email, title: c.p.title, aliases: c.p.aliases }))
    .orderBy("p", "id")
    .limit(1)
    .execute();
  if (person !== undefined) return { kind: "Person", ...person };

  const [org] = await store
    .query()
    .from("Org", "o")
    .whereNode("o", (o) => o.id.eq(normalizedKey).or(o.name.eq(trimmed)).or(o.aliases.contains(trimmed)))
    .select((c) => ({ id: c.o.id, name: c.o.name, domain: c.o.domain, aliases: c.o.aliases }))
    .orderBy("o", "id")
    .limit(1)
    .execute();
  if (org !== undefined) return { kind: "Org", ...org };

  return undefined;
}

export type RecallResult =
  | Readonly<{ found: true; entity: ResolvedEntity; employer?: string; verified: boolean }>
  | Readonly<{ found: false; handle: string }>;

/** Resolved lookup: what is currently believed about `handle`, including
 * every alias that collapsed into the entity it resolves to. */
export async function recall(store: MemoryStore, handle: string): Promise<RecallResult> {
  const entity = await resolveEntity(store, handle);
  if (entity === undefined) return { found: false, handle };
  if (entity.kind === "Org") return { found: true, entity, verified: false };

  const [employment] = await store
    .query()
    .from("Person", "p")
    .whereNode("p", (p) => p.id.eq(entity.id))
    .traverse("worksAt", "w")
    .to("Org", "o")
    .select((c) => ({ orgName: c.o.name }))
    .orderBy("o", "id")
    .limit(1)
    .execute();
  const employer = employment?.orgName;

  const fact = await store.nodes.Fact.getById(asNodeId(factId(entity.id, VERIFIED_PREDICATE)));

  return {
    found: true,
    entity,
    ...(employer === undefined ? {} : { employer }),
    verified: fact !== undefined,
  };
}

export type BelievedAtResult =
  | Readonly<{ found: true; agent: string; offset: string; revision: number; people: readonly Omit<PersonSnapshot, "kind">[] }>
  | Readonly<{ found: false; agent: string; offset: string }>;

/** Time travel: what did `agent`'s stream believe once it had reached
 * `offset`? `book.anchorFor` names the recorded instant the durable
 * consumer checkpointed at that offset; `store.asOfRecorded` reconstructs
 * the belief graph as it stood then, even if it has since been corrected. */
export async function believedAt(store: MemoryStore, book: CheckpointBook, agent: string, offset: string): Promise<BelievedAtResult> {
  const anchor = await book.anchorFor(agent, offset);
  if (anchor === undefined) return { found: false, agent, offset };

  // `RecordedStoreView.query()` is not declared on the class itself — it's
  // inherited from `CoordinatePinnedView`, the base every store view (live
  // or recorded) shares. Reading only a view's own declared members misses
  // it; its per-collection `nodes.X.scan()` is a bounded single page (1,000
  // rows max) and the wrong tool for "give me everything as of this anchor".
  const people = await store
    .asOfRecorded(anchor)
    .query()
    .from("Person", "p")
    .select((c) => ({ id: c.p.id, name: c.p.name, email: c.p.email, title: c.p.title, aliases: c.p.aliases }))
    .execute();

  return { found: true, agent, offset, revision: recordedInstantRevision(anchor), people };
}

export type SourceSupport = Readonly<{ sourceId: string; label: string; retracted: boolean }>;
export type WhySoFarResult =
  | Readonly<{ found: true; entity: string; predicate: string; currentlyHeld: boolean; value?: string; supportedBy: readonly SourceSupport[] }>
  | Readonly<{ found: false; entity: string; predicate: string }>;

/** Provenance: walks Source --premiseOf--> Justification --derives--> Fact
 * to the fact `{entity, predicate}` names, reporting every source that
 * justifies it and whether that source has been retracted — including after
 * the fact itself is no longer held. `currentlyHeld` comes from `getById`,
 * which stops finding a fully-unsupported fact once the retraction
 * capability soft-deletes it. */
export async function whySoFar(store: MemoryStore, entity: string, predicate: string): Promise<WhySoFarResult> {
  const resolved = await resolveEntity(store, entity);
  if (resolved === undefined || resolved.kind !== "Person") return { found: false, entity, predicate };

  const targetFactId = factId(resolved.id, predicate);
  const currentFact = await store.nodes.Fact.getById(asNodeId(targetFactId));

  // Read through tombstones: once every supporting source is retracted, the
  // retraction capability soft-deletes the fact, and a current-mode traversal
  // into it finds nothing — erasing the explanation exactly when it matters.
  const supportedBy = await store
    .view({ mode: "includeTombstones" })
    .query()
    .from("Source", "s")
    .traverse("premiseOf", "p")
    .to("Justification", "j")
    .traverse("derives", "d")
    .to("Fact", "f")
    .whereNode("f", (f) => f.id.eq(targetFactId))
    .select((c) => ({ sourceId: c.s.id, label: c.s.label, retracted: c.s.retracted }))
    .orderBy("s", "id")
    .execute();

  return {
    found: true,
    entity: resolved.id,
    predicate,
    currentlyHeld: currentFact !== undefined,
    ...(currentFact === undefined ? {} : { value: currentFact.value }),
    supportedBy,
  };
}
