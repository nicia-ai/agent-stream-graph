-- agent_events: the append-only log an agent fleet actually writes.
--
-- Every row is one thing an agent did or observed, denormalized enough that a
-- pure decoder (see src/decode.ts) can turn ANY single row into a full graph
-- upsert with no lookup against other rows. That is what a decoder is allowed
-- to assume of a change message — TypeGraph's node.upsert always takes the
-- full prop set, never a diff (see src/graph-events.ts's ValidTime docs one
-- level up in the library) — so a realistic event-sourced table denormalizes
-- exactly like this rather than normalizing task title into a side table.
--
-- Electric watches this table over logical replication. It is APPEND-ONLY by
-- convention (the demo seed never UPDATEs or DELETEs a row), so every change
-- Electric emits for it is an `insert` — the decoder distinguishes "what kind
-- of event this is" using `event_type`, a column, not Electric's row-level
-- operation.
CREATE TABLE IF NOT EXISTS agent_events (
  id              BIGSERIAL PRIMARY KEY,
  agent_id        TEXT NOT NULL,
  agent_name      TEXT NOT NULL,
  task_id         TEXT NOT NULL,
  task_title      TEXT NOT NULL,
  event_type      TEXT NOT NULL CHECK (
                    event_type IN ('task_claimed', 'task_progress', 'task_completed', 'task_failed', 'finding_recorded')
                  ),
  -- The task's status AS OF this event — a snapshot, not a delta, for the same
  -- reason task_title is repeated on every row instead of looked up once.
  status          TEXT NOT NULL,
  -- Only set on event_type = 'finding_recorded'.
  finding_id      TEXT,
  finding_summary TEXT,
  severity        TEXT,
  occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS agent_events_occurred_at_idx ON agent_events (occurred_at);
CREATE INDEX IF NOT EXISTS agent_events_task_id_idx ON agent_events (task_id);
