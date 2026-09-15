const { Client } = require('@elastic/elasticsearch');
const redis = require('./redisClient');

const INDEX = process.env.ES_INDEX || 'records';
const CHAOS_KEY = 'chaos:es_down';

const client = new Client({
  node: process.env.ES_URL || 'http://localhost:9200',
  requestTimeout: 10000,
});

async function ensureIndex() {
  const exists = await client.indices.exists({ index: INDEX });
  if (exists) return;
  try {
    await createIndex();
  } catch (err) {
    // backfill and incremental start concurrently and both race this
    // check-then-create — losing the race is expected, not a fatal error.
    const already = err?.meta?.body?.error?.type === 'resource_already_exists_exception';
    if (!already) throw err;
  }
}

async function createIndex() {
  await client.indices.create({
    index: INDEX,
    mappings: {
      properties: {
        external_id: { type: 'keyword' },
        name: { type: 'text' },
        email: { type: 'keyword' },
        company: { type: 'keyword' },
        city: { type: 'keyword' },
        country: { type: 'keyword' },
        status: { type: 'keyword' },
        tags: { type: 'keyword' },
        // strict numeric mapping is what turns a "N/A" amount into a
        // bulk-item-level failure (used to drive gate G4) instead of a
        // silently-swallowed string.
        amount: { type: 'double' },
        version: { type: 'integer' },
        created_at: { type: 'date' },
        updated_at: { type: 'date' },
      },
    },
  });
}

function toDoc(row) {
  return {
    external_id: row.external_id,
    name: row.name,
    email: row.email,
    company: row.company,
    city: row.city,
    country: row.country,
    status: row.status,
    tags: row.tags,
    amount: row.amount,
    version: row.version,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

// Bulk-upserts `rows` (id = source primary key, so replays converge to the
// same document -> idempotent by construction, see SPEC.md 2.3 / ADR-2).
// Soft-deleted rows are deleted from the index outright.
//
// Returns { failedRows: [{row, error}], connectionError } — connectionError
// means the WHOLE request failed (ES unreachable) and the caller should
// retry the batch later without advancing the checkpoint; failedRows means
// the request succeeded but specific documents were rejected (bad data),
// which the caller routes to the DLQ while the rest of the batch commits.
async function bulkUpsert(rows) {
  if (rows.length === 0) return { failedRows: [], connectionError: null };

  // Software fault injection for the UI's "simulate sink outage" control —
  // an alternative to `docker stop elasticsearch` that doesn't require
  // giving any container access to the host Docker socket. verify.sh uses
  // the real `docker stop` for gate G3; this flag is for interactive demos.
  const chaos = await redis.get(CHAOS_KEY).catch(() => null);
  if (chaos) {
    return { failedRows: [], connectionError: new Error('chaos: simulated Elasticsearch outage') };
  }

  const operations = [];
  for (const row of rows) {
    if (row.deleted_at) {
      operations.push({ delete: { _index: INDEX, _id: String(row.id) } });
    } else {
      operations.push({ index: { _index: INDEX, _id: String(row.id) } });
      operations.push(toDoc(row));
    }
  }

  let result;
  try {
    result = await client.bulk({ operations, refresh: false });
  } catch (err) {
    return { failedRows: [], connectionError: err };
  }

  const failedRows = [];
  if (result.errors) {
    result.items.forEach((item, idx) => {
      const op = item.index || item.delete;
      if (op && op.error) {
        // deletes of already-absent docs (404) are not real failures
        if (item.delete && op.status === 404) return;
        const row = rows[idx];
        failedRows.push({ row, error: `${op.error.type}: ${op.error.reason}` });
      }
    });
  }
  return { failedRows, connectionError: null };
}

async function health() {
  try {
    const h = await client.cluster.health({ timeout: '2s' });
    return { ok: h.status !== 'red', status: h.status };
  } catch (err) {
    return { ok: false, status: 'unreachable' };
  }
}

module.exports = { client, ensureIndex, bulkUpsert, health, INDEX };
