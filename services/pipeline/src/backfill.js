const db = require('./db');
const es = require('./es');
const { sleep } = require('./util');
const { writeBatchToSinks } = require('./sinkWriter');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '500', 10);
const IDLE_POLL_MS = 2000;

// One-time full extraction, keyset-paginated on id so it never loads more
// than BATCH_SIZE rows into memory at once (see README Capacity Notes for
// why that matters at the seeded volume). Resumable: every iteration reads
// the checkpoint fresh, so a process restart just continues from
// `last_id` — it never re-reads from row 0. That's gate G1.
async function runBackfill(metrics) {
  await es.ensureIndex();

  for (;;) {
    const cp = await db.getCheckpoint('backfill');

    if (cp.status === 'paused') {
      metrics.setGauge('mode_running', 0);
      await sleep(1000);
      continue;
    }

    const rows = await db.fetchBackfillBatch(cp.last_id, BATCH_SIZE);

    if (rows.length === 0) {
      if (cp.status !== 'completed') await db.setStatus('backfill', 'completed');
      metrics.setGauge('mode_running', 0);
      metrics.setGauge('backfill_complete', 1);
      await sleep(IDLE_POLL_MS); // idle poll — new rows can still arrive after "completion"
      continue;
    }

    if (cp.status !== 'running') await db.setStatus('backfill', 'running');
    metrics.setGauge('mode_running', 1);
    metrics.setGauge('backfill_complete', 0);
    metrics.setGauge('backfill_last_id', rows[rows.length - 1].id);

    await writeBatchToSinks(rows, metrics);

    const lastId = rows[rows.length - 1].id;
    await db.advanceBackfillCheckpoint(lastId, rows.length);
    metrics.inc('batches_total');
  }
}

module.exports = { runBackfill };
