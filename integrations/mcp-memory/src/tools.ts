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
 * The resolution ladder both `Person` and `Org` climb: an exact id, a
 * caller-supplied stable key (email/domain, case-insensitively), an exact
 * display name, or a spelling folded into `aliases`.
 */
function matchesHandle(candidate: { id: string; name: string; aliases: readonly string[] }, stableKey: string, trimmed: string, lower: string): boolean {
  return candidate.id === lower || stableKey.toLowerCase() === lower || candidate.name === trimmed || candidate.aliases.includes(trimmed);
}

/**
 * Entity resolution: given ANY handle a source has used for an entity — its
 * id, its stable key (email/domain), or one of the name spellings folded
 * into `aliases` by the projector in `store.ts` — returns the one canonical
 * row. This is what lets `recall("J. Doe")` and `recall("Jane Doe")` answer
 * with the same person.
 */
async function resolveEntity(store: MemoryStore, handle: string): Promise<ResolvedEntity | undefined> {
  const trimmed = handle.trim();
  const lower = trimmed.toLowerCase();

  const people = await store
    .query()
    .from("Person", "p")
    .select((c) => ({ id: c.p.id, name: c.p.name, email: c.p.email, title: c.p.title, aliases: c.p.aliases }))
    .execute();
  const person = people.find((p) => matchesHandle(p, p.email, trimmed, lower));
  if (person !== undefined) return { kind: "Person", ...person };

  const orgs = await store
    .query()
    .from("Org", "o")
    .select((c) => ({ id: c.o.id, name: c.o.name, domain: c.o.domain, aliases: c.o.aliases }))
    .execute();
  const org = orgs.find((o) => matchesHandle(o, o.domain, trimmed, lower));
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

  const employment = await store
    .query()
    .from("Person", "p")
    .traverse("worksAt", "w")
    .to("Org", "o")
    .select((c) => ({ personId: c.p.id, orgName: c.o.name }))
    .execute();
  const employer = employment.find((e) => e.personId === entity.id)?.orgName;

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
 * backward from the fact `{entity, predicate}` names, reporting every
 * source that justifies it and whether that source has been retracted. A
 * fact with no live (non-retracted) support is no longer held —
 * `currentlyHeld` reflects that directly, since a fully-unsupported fact is
 * soft-deleted by the retraction capability and `getById` stops finding it. */
export async function whySoFar(store: MemoryStore, entity: string, predicate: string): Promise<WhySoFarResult> {
  const resolved = await resolveEntity(store, entity);
  if (resolved === undefined || resolved.kind !== "Person") return { found: false, entity, predicate };

  const targetFactId = factId(resolved.id, predicate);
  const currentFact = await store.nodes.Fact.getById(asNodeId(targetFactId));

  const derivations = await store
    .query()
    .from("Justification", "j")
    .traverse("derives", "d")
    .to("Fact", "f")
    .select((c) => ({ justificationId: c.j.id, factId: c.f.id }))
    .execute();
  const relevantJustificationIds = new Set(derivations.filter((d) => d.factId === targetFactId).map((d) => d.justificationId));

  const premises = await store
    .query()
    .from("Source", "s")
    .traverse("premiseOf", "p")
    .to("Justification", "j")
    .select((c) => ({ sourceId: c.s.id, label: c.s.label, retracted: c.s.retracted, justificationId: c.j.id }))
    .execute();
  const supportedBy = premises
    .filter((p) => relevantJustificationIds.has(p.justificationId))
    .map((p) => ({ sourceId: p.sourceId, label: p.label, retracted: p.retracted }));

  return {
    found: true,
    entity: resolved.id,
    predicate,
    currentlyHeld: currentFact !== undefined,
    ...(currentFact === undefined ? {} : { value: currentFact.value }),
    supportedBy,
  };
}
