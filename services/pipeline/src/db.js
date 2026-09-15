const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/killittwice',
  max: 10,
});

async function getCheckpoint(mode) {
  const { rows } = await pool.query('SELECT * FROM pipeline_checkpoint WHERE mode = $1', [mode]);
  return rows[0];
}

async function setStatus(mode, status) {
  await pool.query('UPDATE pipeline_checkpoint SET status = $1, updated_at = now() WHERE mode = $2', [status, mode]);
}

// Advance checkpoint ONLY after both sinks have durably accepted the batch.
// This is the crash-safety line: if the process dies before this commits,
// the next run re-reads the OLD checkpoint and reprocesses the batch
// (at-least-once), never skips one.
async function advanceBackfillCheckpoint(lastId, rowsInBatch) {
  await pool.query(
    `UPDATE pipeline_checkpoint
     SET last_id = $1, rows_processed = rows_processed + $2, updated_at = now()
     WHERE mode = 'backfill'`,
    [lastId, rowsInBatch]
  );
}

async function advanceIncrementalCheckpoint(watermark, watermarkId, rowsInBatch) {
  await pool.query(
    `UPDATE pipeline_checkpoint
     SET last_watermark = $1, last_watermark_id = $2, rows_processed = rows_processed + $3, updated_at = now()
     WHERE mode = 'incremental'`,
    [watermark, watermarkId, rowsInBatch]
  );
}

async function fetchBackfillBatch(afterId, limit) {
  const { rows } = await pool.query(
    `SELECT * FROM records WHERE id > $1 ORDER BY id ASC LIMIT $2`,
    [afterId, limit]
  );
  return rows;
}

async function fetchIncrementalBatch(afterWatermark, afterWatermarkId, limit) {
  const { rows } = await pool.query(
    `SELECT * FROM records
     WHERE (updated_at, id) > ($1::timestamptz, $2::bigint)
     ORDER BY updated_at ASC, id ASC
     LIMIT $3`,
    [afterWatermark, afterWatermarkId, limit]
  );
  return rows;
}

async function countRecords() {
  const { rows } = await pool.query('SELECT count(*)::bigint AS n FROM records');
  return Number(rows[0].n);
}

// Upsert on (source_record_id, sink): backfill and incremental can both
// independently hit the same failing row (overlapping scans), and a replay
// that fails again should reopen the same row rather than pile up a new
// one. See the schema migration comment on uq_dlq_record_sink.
async function writeDlq(row, sink, error) {
  await pool.query(
    `INSERT INTO dlq_sink_failures (source_record_id, external_id, sink, payload, error)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (source_record_id, sink) DO UPDATE SET
       payload = excluded.payload,
       error = excluded.error,
       attempt_count = dlq_sink_failures.attempt_count + 1,
       last_failed_at = now(),
       status = 'pending'`,
    [row.id, row.external_id, sink, JSON.stringify(row), String(error).slice(0, 2000)]
  );
}

module.exports = {
  pool,
  getCheckpoint,
  setStatus,
  advanceBackfillCheckpoint,
  advanceIncrementalCheckpoint,
  fetchBackfillBatch,
  fetchIncrementalBatch,
  countRecords,
  writeDlq,
};
