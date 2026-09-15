const es = require('./es');
const mq = require('./mq');
const db = require('./db');
const { sleep, backoffMs } = require('./util');

// Writes one batch to both sinks. Blocks (with capped backoff, never a
// busy-loop) until BOTH sinks have durably accepted the batch, then
// returns. The caller only advances its checkpoint after this resolves —
// that ordering is what makes G1/G3 safe. Per-row Elasticsearch rejections
// (bad data, not an outage) are routed to the DLQ instead of blocking the
// batch — that's G4.
async function writeBatchToSinks(rows, metrics) {
  if (rows.length === 0) return;

  // --- Elasticsearch leg: retry the whole batch on connection failure,
  // but a per-document rejection is not a connection failure — it goes to
  // the DLQ and the batch proceeds.
  let esAttempt = 0;
  let dlqCount = 0;
  for (;;) {
    const { failedRows, connectionError } = await es.bulkUpsert(rows);
    if (connectionError) {
      metrics.inc('es_connection_errors');
      metrics.setGauge('es_up', 0);
      const wait = backoffMs(esAttempt++);
      await sleep(wait);
      continue;
    }
    metrics.setGauge('es_up', 1);
    for (const { row, error } of failedRows) {
      await db.writeDlq(row, 'elasticsearch', error);
      dlqCount++;
    }
    metrics.inc('dlq_total', dlqCount);
    metrics.inc('es_indexed_total', rows.length - failedRows.length);
    break;
  }

  // --- RabbitMQ leg: same retry-the-whole-batch policy on connection loss.
  let mqAttempt = 0;
  for (;;) {
    try {
      await mq.publishBatch(rows);
      metrics.setGauge('mq_up', 1);
      metrics.inc('mq_published_total', rows.length);
      break;
    } catch (err) {
      metrics.inc('mq_connection_errors');
      metrics.setGauge('mq_up', 0);
      const wait = backoffMs(mqAttempt++);
      await sleep(wait);
    }
  }

  metrics.recordRows(rows.length);
}

module.exports = { writeBatchToSinks };
