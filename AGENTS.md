# AGENTS.md

Instructions for an AI agent (or a human) picking up work in this repository.

## What this repo is

A data replication pipeline: Postgres (source) → Elasticsearch (search
index) + RabbitMQ (event stream), with a backfill worker and an incremental
worker running concurrently, a DLQ for bad rows, an independent RabbitMQ
consumer, and a React UI. Built for Optio's "Kill It Twice" take-home — see
[SPEC.md](./SPEC.md) for the full design rationale and its revision history,
and [README.md](./README.md) for architecture, ADRs, and gate results.

## Layout

```
infra/postgres/init/     Postgres schema (runs once, on first container start)
scripts/seed.js          Data generator: bulk / drip / corrupt subcommands
services/pipeline/       Backfill + incremental workers (same image, MODE env var picks the loop)
services/consumer/       Independent RabbitMQ consumer (analytics + dedupe demo)
services/api/            Express API the UI talks to (status, records, DLQ, control, simulation)
ui/                      React + Vite dashboard
docker-compose.yml       Whole stack
Makefile                 up / seed / verify entry points
verify.sh                Automated gate checker (G1-G5) — the main deliverable
```

## Conventions

- Every backend service is plain Node.js (CommonJS, no framework) — see
  SPEC.md's v2 changelog for why NestJS was dropped. Don't reintroduce a
  framework for one service without doing it for all three; the point was
  consistency.
- No ORM. Raw SQL via the `pg` driver, because every query here is either a
  simple keyset/watermark scan or a targeted single-row lookup — an ORM
  would add a layer without simplifying anything.
- Checkpoints only advance in `services/pipeline/src/db.js` /
  `advanceBackfillCheckpoint` / `advanceIncrementalCheckpoint`, and only
  after `sinkWriter.writeBatchToSinks` resolves. Do not reorder this — it's
  the entire basis of the crash-safety story (gate G1). If you change the
  batch loop, keep "write sinks, then advance checkpoint" as the last two
  steps, in that order.
- DLQ writes are upserts on `(source_record_id, sink)`, not inserts — see
  the `uq_dlq_record_sink` comment in `infra/postgres/init/001_schema.sql`.
  Backfill and incremental can both observe the same failing row; don't
  reintroduce a plain INSERT there, it'll double-count DLQ entries again.
- DLQ replay re-reads the CURRENT source row (`db.getCurrentRecord`), not
  the frozen payload captured at failure time. If you touch
  `POST /api/dlq/:id/replay`, keep it that way — see SPEC.md v2 item 4 for
  why replaying the frozen payload is wrong.
- Timestamps that participate in the incremental watermark comparison
  (`records.updated_at`, `pipeline_checkpoint.last_watermark`) are
  `TIMESTAMPTZ(3)`, not the default microsecond precision. This is load
  bearing — see the schema comment and SPEC.md v2 item 2. If you add a new
  timestamp column that a watermark scan will compare against a JS `Date`
  round-tripped through Postgres, declare it `TIMESTAMPTZ(3)` too.

## What not to touch without a good reason

- `docker-compose.yml` service names / ports — `verify.sh` and the API's
  env vars (`PIPELINE_BACKFILL_URL` etc.) hardcode container hostnames.
- The chaos-flag keys (`chaos:es_down`, `chaos:mq_down` in Redis) — the UI's
  Simulate tab and `services/pipeline/src/{es,mq}.js` both read these
  exact keys.

## How to verify your own work

- `make seed ROWS=<n>` (or `docker compose --profile tools run --rm seeder
  bulk --rows=<n>`) to load data.
- `docker compose up -d --build <service>` to rebuild one service after an
  edit — compose does NOT auto-rebuild on `up -d` without `--build`.
- `make verify` (`./verify.sh`) is the real test: it tears down and
  rebuilds the whole stack, seeds fresh data, and runs all five gates for
  real (kills containers, stops Elasticsearch, injects bad rows). Run it
  after any change to the pipeline, sinks, checkpoint logic, or DLQ — a
  change that looks correct by reading it has, more than once during this
  build, turned out not to be (see SPEC.md v2 for two real examples caught
  this way, not by review).
- For a quick manual check instead of the full gate suite: `curl
  localhost:8080/api/status | python3 -m json.tool`.
- There is no unit/integration test suite beyond `verify.sh` — this was a
  deliberate scope cut given the time box; see README "What I didn't
  build" for the reasoning.
