/**
 * The belief graph this package materializes: a small "agent fleet" domain —
 * agents claim tasks, work them, and record findings along the way.
 *
 * Three node kinds, two edges. Nothing here is Electric-specific; this is the
 * same `defineGraph` shape any TypeGraph consumer would write. The Electric
 * part is entirely in `decode.ts` (turning a Postgres row into events) and
 * `demo.ts` (wiring a live shape into `consume()`).
 */
import { defineEdge, defineGraph, defineNode, searchable } from "@nicia-ai/typegraph";
import { z } from "zod";

export const Agent = defineNode("Agent", {
  schema: z.object({
    name: searchable({ language: "english" }),
    status: z.string(),
  }),
});

export const Task = defineNode("Task", {
  schema: z.object({
    title: searchable({ language: "english" }),
    status: z.string(),
  }),
});

export const Finding = defineNode("Finding", {
  schema: z.object({
    summary: searchable({ language: "english" }),
    severity: z.string(),
  }),
});

/** A task's current assignee. Matched by endpoints, so a re-assignment closes the old edge and opens the new one. */
export const assignedTo = defineEdge("assignedTo", { schema: z.object({}) });

/** A finding produced in the course of working a task. */
export const aboutTask = defineEdge("aboutTask", { schema: z.object({}) });

export const fleetGraph = defineGraph({
  id: "agent_fleet",
  nodes: {
    Agent: { type: Agent },
    Task: { type: Task },
    Finding: { type: Finding },
  },
  edges: {
    assignedTo: { type: assignedTo, from: [Task], to: [Agent] },
    aboutTask: { type: aboutTask, from: [Finding], to: [Task] },
  },
});
