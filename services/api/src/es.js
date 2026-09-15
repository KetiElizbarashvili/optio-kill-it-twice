const { Client } = require('@elastic/elasticsearch');

const INDEX = process.env.ES_INDEX || 'records';
const client = new Client({ node: process.env.ES_URL || 'http://localhost:9200', requestTimeout: 8000 });

async function searchRecords({ q, status, country, page = 1, limit = 25 }) {
  const must = [];
  if (q) must.push({ multi_match: { query: q, fields: ['name', 'email', 'company'] } });
  if (status) must.push({ term: { status } });
  if (country) must.push({ term: { country } });

  const result = await client.search({
    index: INDEX,
    from: (page - 1) * limit,
    size: limit,
    sort: [{ updated_at: 'desc' }],
    query: must.length ? { bool: { must } } : { match_all: {} },
  });

  return {
    total: result.hits.total.value,
    records: result.hits.hits.map((h) => ({ id: h._id, ...h._source })),
  };
}

async function getRecordById(id) {
  try {
    const result = await client.get({ index: INDEX, id: String(id) });
    return { id: result._id, ...result._source };
  } catch (err) {
    if (err.meta && err.meta.statusCode === 404) return null;
    throw err;
  }
}

async function reindexDoc(id, doc) {
  await client.index({ index: INDEX, id: String(id), document: doc });
}

async function health() {
  try {
    const h = await client.cluster.health({ timeout: '2s' });
    return { ok: h.status !== 'red', status: h.status };
  } catch {
    return { ok: false, status: 'unreachable' };
  }
}

module.exports = { searchRecords, getRecordById, reindexDoc, health };
