-- Source-of-truth schema. One table is enough to exercise every gate;
-- see SPEC.md 4 for why multi-table replication is out of scope.

CREATE TABLE IF NOT EXISTS records (
    id              BIGSERIAL PRIMARY KEY,
    external_id     UUID NOT NULL DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    email           TEXT NOT NULL,
    company         TEXT NOT NULL,
    city            TEXT NOT NULL,
    country         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    tags            TEXT[] NOT NULL DEFAULT '{}',
    -- kept as TEXT (not NUMERIC) on purpose: it lets the failure-simulation
    -- path write a non-numeric value ("N/A") to deliberately produce a
    -- record the Elasticsearch sink will reject at bulk-item level, without
    -- needing a separate "corrupt" code path in the pipeline itself. See
    -- SPEC.md and README "Where AI deviated from spec".
    amount          TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    -- TIMESTAMPTZ(3), not the default microsecond precision: the
    -- incremental watermark round-trips through a JS Date (millisecond
    -- precision) on its way to/from the checkpoint table, and comparing a
    -- millisecond-truncated watermark against a microsecond-precision
    -- column made `updated_at > last_watermark` true forever for any row
    -- sharing the truncated millisecond — an infinite reprocessing loop
    -- caught in local testing (see README "Where AI deviated from spec").
    created_at      TIMESTAMPTZ(3) NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ(3) NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ(3)
);

-- Watermark scan: "everything changed since (last_watermark, last_id)".
-- The (updated_at, id) composite index makes that a pure index range scan
-- even at multi-million row counts.
CREATE INDEX IF NOT EXISTS idx_records_watermark ON records (updated_at, id);

-- Backfill uses keyset pagination on id, independent of the watermark index.
CREATE INDEX IF NOT EXISTS idx_records_id ON records (id);

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- One row per pipeline mode. Updated only after a batch is durably committed
-- to Postgres, i.e. AFTER the sinks have ack'd the batch — see SPEC.md 2.4.
CREATE TABLE IF NOT EXISTS pipeline_checkpoint (
    mode                TEXT PRIMARY KEY,             -- 'backfill' | 'incremental'
    status              TEXT NOT NULL DEFAULT 'idle',  -- idle | running | paused | completed
    last_id             BIGINT NOT NULL DEFAULT 0,      -- backfill cursor (keyset)
    last_watermark      TIMESTAMPTZ(3),                 -- incremental cursor
    last_watermark_id   BIGINT NOT NULL DEFAULT 0,      -- tie-breaker within same watermark
    rows_processed      BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO pipeline_checkpoint (mode, status) VALUES ('backfill', 'idle')
    ON CONFLICT (mode) DO NOTHING;
INSERT INTO pipeline_checkpoint (mode, status) VALUES ('incremental', 'idle')
    ON CONFLICT (mode) DO NOTHING;

-- DLQ for the Elasticsearch-sink leg specifically (bulk-item-level
-- failures). The RabbitMQ leg has its own broker-native DLX; see SPEC.md
-- 2.6 / README ADR for why the two failure modes get two different DLQs.
CREATE TABLE IF NOT EXISTS dlq_sink_failures (
    id                  BIGSERIAL PRIMARY KEY,
    source_record_id    BIGINT NOT NULL,
    external_id         UUID NOT NULL,
    sink                TEXT NOT NULL,              -- 'elasticsearch'
    payload             JSONB NOT NULL,
    error               TEXT NOT NULL,
    attempt_count       INTEGER NOT NULL DEFAULT 1,
    status              TEXT NOT NULL DEFAULT 'pending', -- pending | replayed | resolved
    first_failed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq_sink_failures (status);

-- Backfill and incremental can both independently observe the same failing
-- row (e.g. a row written after backfill's cursor but caught by both
-- workers' overlapping scans — see README "Where AI deviated from spec").
-- One DLQ row per (record, sink) keeps replay/count semantics honest
-- instead of the DLQ silently double-counting the same failure.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dlq_record_sink ON dlq_sink_failures (source_record_id, sink);
