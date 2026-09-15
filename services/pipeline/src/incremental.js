const db = require('./db');
const es = require('./es');
const { sleep } = require('./util');
const { writeBatchToSinks } = require('./sinkWriter');

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '500', 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '2000', 10);
const EPOCH = new Date(0).toISOString();

// Continuous (updated_at, id) watermark scan — runs forever, independent of
// the backfill worker's process/container, so killing one never stops the
// other (see SPEC.md open question 5, resolved: two separate processes).
async function runIncremental(metrics) {
  await es.ensureIndex();

  for (;;) {
    const cp = await db.getCheckpoint('incremental');

    if (cp.status === 'paused') {
      metrics.setGauge('mode_running', 0);
      await sleep(1000);
      continue;
    }

    const watermark = cp.last_watermark ? cp.last_watermark.toISOString() : EPOCH;
    const rows = await db.fetchIncrementalBatch(watermark, cp.last_watermark_id, BATCH_SIZE);

    if (rows.length === 0) {
      // Fully caught up: lag is "how old is the oldest unprocessed change",
      // not "how long since anything last changed" — with no pending rows
      // those are very different numbers, and reporting the latter made a
      // perfectly healthy, idle worker look like it was falling further
      // and further behind the longer nothing happened. Caught while
      // reading verify.sh output on a static (no-drip) dataset: lag was
      // climbing past 50s+ on a worker that had already processed every
      // row. See SPEC.md v3 / README "Where AI deviated from spec".
      metrics.setGauge('lag_seconds', 0);
      metrics.setGauge('mode_running', 1);
      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    // oldest pending change's age at the moment we picked up the batch
    const lagMs = Date.now() - new Date(rows[0].updated_at).getTime();
    metrics.setGauge('lag_seconds', Math.max(0, lagMs / 1000).toFixed(1));

    await db.setStatus('incremental', 'running');
    metrics.setGauge('mode_running', 1);

    await writeBatchToSinks(rows, metrics);

    const last = rows[rows.length - 1];
    await db.advanceIncrementalCheckpoint(last.updated_at, last.id, rows.length);
    metrics.inc('batches_total');
  }
}

module.exports = { runIncremental };
