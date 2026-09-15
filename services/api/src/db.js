const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/killittwice',
  max: 10,
});

async function getCheckpoints() {
  const { rows } = await pool.query('SELECT * FROM pipeline_checkpoint');
  return rows;
}

async function setCheckpointStatus(mode, status) {
  await pool.query('UPDATE pipeline_checkpoint SET status = $1, updated_at = now() WHERE mode = $2', [status, mode]);
}

async function countRecords() {
  const { rows } = await pool.query('SELECT count(*)::bigint AS n FROM records WHERE deleted_at IS NULL');
  return Number(rows[0].n);
}

async function maxId() {
  const { rows } = await pool.query('SELECT coalesce(max(id), 0)::bigint AS n FROM records');
  return Number(rows[0].n);
}

async function listDlq({ status = 'pending', limit = 50, offset = 0 }) {
  const { rows } = await pool.query(
    `SELECT * FROM dlq_sink_failures WHERE status = $1 ORDER BY first_failed_at DESC LIMIT $2 OFFSET $3`,
    [status, limit, offset]
  );
  return rows;
}

async function countDlq(status = 'pending') {
  const { rows } = await pool.query('SELECT count(*)::bigint AS n FROM dlq_sink_failures WHERE status = $1', [status]);
  return Number(rows[0].n);
}

async function getDlqById(id) {
  const { rows } = await pool.query('SELECT * FROM dlq_sink_failures WHERE id = $1', [id]);
  return rows[0];
}

// Replay must re-read the CURRENT source row, not the frozen bad payload
// captured at failure time — the whole point of a DLQ is that someone (or
// something) fixes the underlying data, and replay should pick that fix up.
async function getCurrentRecord(id) {
  const { rows } = await pool.query('SELECT * FROM records WHERE id = $1', [id]);
  return rows[0];
}

async function markDlqReplayed(id) {
  await pool.query(`UPDATE dlq_sink_failures SET status = 'replayed' WHERE id = $1`, [id]);
}

async function bumpDlqAttempt(id, error) {
  await pool.query(
    `UPDATE dlq_sink_failures SET attempt_count = attempt_count + 1, error = $2, last_failed_at = now() WHERE id = $1`,
    [id, String(error).slice(0, 2000)]
  );
}

const FIRST = ['Nino', 'Giorgi', 'Ana', 'Luka', 'Mariam'];
const LAST = ['Beridze', 'Kapanadze', 'Lomidze'];

async function insertCorruptRows(count = 3) {
  const ids = [];
  for (let i = 0; i < count; i++) {
    const name = `${FIRST[i % FIRST.length]} ${LAST[i % LAST.length]}`;
    const { rows } = await pool.query(
      `INSERT INTO records (name, email, company, city, country, status, tags, amount)
       VALUES ($1, $2, 'SimCo', 'Tbilisi', 'Georgia', 'active', '{simulated}', 'N/A')
       RETURNING id`,
      [name, `sim.${Date.now()}.${i}@example.com`]
    );
    ids.push(rows[0].id);
  }
  return ids;
}

module.exports = {
  pool,
  getCheckpoints,
  setCheckpointStatus,
  countRecords,
  maxId,
  listDlq,
  countDlq,
  getDlqById,
  getCurrentRecord,
  markDlqReplayed,
  bumpDlqAttempt,
  insertCorruptRows,
};
