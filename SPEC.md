# SPEC.md — v1 (initial)

> Status: DRAFT v1. This is the specification as understood before implementation
> begins. It will change as reality pushes back — see the "Changelog" section at
> the bottom, which is appended to (never rewritten) as decisions change.

## 1. Problem

Optio's assignment: build a data replication system that reads from a source of
truth (a relational database) and fans out to two independent sinks:

1. A **search index** (current-state view, queryable) — Elasticsearch.
2. An **event stream** (change log, fan-out to consumers) — RabbitMQ.

The system must run two modes concurrently:
- **Backfill**: one-time full extraction of all existing rows.
- **Incremental sync**: ongoing extraction of rows changed since the last
  checkpoint, on a poll interval.

The exercise is explicitly *not* about proving the happy path works — it is
about proving the system survives crashes, sink outages, and partial batch
failures without losing data, without duplicating data unrecoverably, and
without leaving an operator blind. Five gates (G1–G5) must be independently
verifiable via a single `make verify` command.

## 2. Decisions already made (v1)

### 2.1 Stack

Optio's own stack is NestJS / Angular / RabbitMQ / Redis / Elasticsearch /
Docker / S3 / ClickHouse / NiFi. The brief explicitly allows deviating from it.
Decision: **stay close to their stack** (NestJS, RabbitMQ, Redis, Elasticsearch,
Docker) because (a) it's a stack this team runs day to day, so building on it
is a more honest signal of fit than picking something unfamiliar to them, and
(b) all four pieces are genuinely the right tool for their respective jobs
here, independent of the interview-signal argument. Deviations:

- **Source DB**: PostgreSQL. Not in Optio's list, but "a relational database"
  is the spec's requirement and Postgres is the least-friction choice for a
  logical-replication-free polling CDC approach (see ADR-1).
- **Frontend**: plain React + Vite instead of Angular. Rationale is a
  time/value trade-off, recorded as an explicit AI deviation in the README's
  "where AI deviated from spec" section, not a silent substitution — the
  functional requirements (status, data browser, controls, simulation) don't
  depend on the frontend framework, and Angular's added scaffolding cost
  doesn't buy anything the gates check.

### 2.2 Change detection strategy

No logical replication / WAL tailing (too heavy for the time box, and
overkill for the stated data volume). Instead: a `updated_at` watermark
column + monotonic `id`, polled on an interval. Deletes need a soft-delete
flag (`deleted_at`) since a watermark scan can't see hard deletes. This is a
real limitation, documented as such, not hidden.

### 2.3 Delivery guarantee

Target: **at-least-once**, with **effectively-once at the sinks** —
Elasticsearch writes are naturally idempotent (upsert by primary key, so
replays converge to the same document); RabbitMQ consumers must dedupe
downstream (a consumer-side idempotency key = source row id + version). This
is stated as a decision, not a discovery — see ADR-2.

### 2.4 Checkpointing

Checkpoint (last-seen watermark + offset within that watermark tie) persisted
to Postgres (a dedicated `pipeline_checkpoint` table) after each committed
batch, not before — so a crash mid-batch replays that batch (at-least-once),
never skips one.

### 2.5 Data volume

Target scale: **~2,000,000 source rows**, seeded before backfill starts, with
a live incremental trickle during the run. Enough that "load it all into
memory" visibly breaks (rules out naive `SELECT *` + JSON array in RAM) and
that a kill mid-backfill lands somewhere clearly in the middle rather than
finishing before the kill lands. Full reasoning in README "Capacity Notes".

### 2.6 Batch size

500 rows per batch end-to-end (extraction page size = sink write batch size),
matching the gate's own G4 scenario (3 bad rows in a 500-row batch).

## 3. Scope (what's in v1 of the build)

- [ ] Postgres source schema + seed script (`make seed`) generating ~2M rows
      across at least one table with realistic-enough columns to make search
      meaningful.
- [ ] Pipeline service (NestJS) with two concurrent workers: backfill worker,
      incremental worker. Shared checkpoint store.
- [ ] Elasticsearch sink writer (bulk API, upsert semantics).
- [ ] RabbitMQ event publisher (change events, one per row change).
- [ ] At least one independent consumer of the RabbitMQ stream (separate
      process/container) — proves the event stream is a real fan-out point,
      not decoration.
- [ ] DLQ: failed-row landing zone with enough context (source id, payload,
      failure reason, attempt count) to be replayed via a UI/CLI action.
- [ ] Metrics + health endpoints (Prometheus-style `/metrics` + `/health`)
      feeding the UI and the verify script.
- [ ] UI (React) — 4 panels: pipeline status, data browser, controls,
      failure simulation.
- [ ] `verify.sh` / `make verify` — scripted, automated proof for G1–G5.
- [ ] `docker-compose.yml` bringing up: Postgres, RabbitMQ, Elasticsearch,
      Redis, pipeline service, consumer service, API, UI.

## 4. Explicitly out of scope (v1) — see README "what I didn't build"

- Exactly-once at the RabbitMQ leg (at-least-once + idempotent consumer
  instead — cheaper and just as correct for this problem).
- Multi-table / multi-source replication (one source table is enough to prove
  the gates; N tables is repetition, not new risk surface).
- Schema evolution / migrations handling on the source.
- Authn/authz on the UI or API.
- Horizontal scaling of the pipeline worker (single active instance per mode;
  the gates test crash-recovery of one instance, not leader election across
  many).

## 5. Open questions at v1 (to resolve during build)

- Exact Postgres → ES field mapping / analyzers for the search index.
- Whether the incremental worker and backfill worker share one process or
  run as two — leaning two, so G1 (kill mid-backfill) doesn't also kill
  incremental sync.
- DLQ storage: Postgres table vs RabbitMQ dead-letter exchange vs both.
  Leaning both: RabbitMQ DLX for the event-stream leg, Postgres table for the
  ES-sink leg (the two failure modes are different: broker-side reject vs
  sink-side bulk-item failure).

## Changelog

- v1 (initial): this document.

- v2 (during backend build, before UI): four changes, all discovered by
  actually running the system, not by re-reading the spec:

  1. **Dropped NestJS for the pipeline/consumer/api services**, using plain
     Node.js instead. Rationale: the pipeline and consumer are background
     workers with no HTTP surface of their own beyond a `/health` and
     `/metrics` endpoint — they don't benefit from Nest's HTTP-centric DI
     container. Once two of three services didn't need Nest, using it only
     for the API would have meant two different conventions for no real
     gain. This is a scope-speed trade-off, not a correctness one — see
     README "Where AI deviated from spec" for the honest version of why.

  2. **`updated_at`/`created_at` changed from `TIMESTAMPTZ` to
     `TIMESTAMPTZ(3)`.** Found by actually running the incremental worker:
     Postgres stores microsecond precision by default, but the watermark
     round-trips through a JS `Date` (millisecond precision) on its way
     into and back out of `pipeline_checkpoint`. Comparing a
     millisecond-truncated watermark against microsecond-precision rows
     made `updated_at > last_watermark` true forever for any row sharing
     that truncated millisecond — an actual infinite reprocessing loop,
     caught because rows_processed climbed to 75x the seeded row count
     instead of converging. Truncating the column itself to millisecond
     precision makes the round-trip lossless. See README "Where AI deviated
     from spec" for the full incident.

  3. **DLQ upserts on `(source_record_id, sink)` instead of inserting
     unconditionally.** Backfill and incremental both scan forward
     independently and can both observe the same failing row (e.g. a row
     written after backfill's cursor position, which both workers' next
     batch will pick up). Without a unique constraint, one bad row produced
     two DLQ entries instead of one — undercutting the exact "3 rows in, 3
     rows in DLQ" story gate G4 is supposed to demonstrate.

  4. **DLQ replay re-reads the current source row, not the frozen bad
     payload.** The original design replayed whatever JSON was captured at
     failure time. That's wrong: the entire point of a DLQ is that someone
     fixes the underlying data (or bug) and then replays — replaying the
     original bad payload verbatim would just fail again, identically,
     forever.
