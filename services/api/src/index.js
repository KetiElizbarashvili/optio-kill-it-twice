const express = require('express');
const cors = require('cors');
const db = require('./db');
const es = require('./es');
const rabbitmq = require('./rabbitmq');
const redis = require('./redisClient');

const PORT = parseInt(process.env.PORT || '8080', 10);
const PIPELINE_BACKFILL_URL = process.env.PIPELINE_BACKFILL_URL || 'http://pipeline-backfill:9100';
const PIPELINE_INCREMENTAL_URL = process.env.PIPELINE_INCREMENTAL_URL || 'http://pipeline-incremental:9100';
const CONSUMER_URL = process.env.CONSUMER_URL || 'http://consumer:9200';

async function fetchJson(url, timeoutMs = 3000) {
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// Express 4 does not catch a rejected promise from an async handler — it
// becomes an unhandled rejection, and Node terminates the whole process on
// those by default. Without this wrapper, a single transient Postgres
// hiccup during a `/api/status` poll (the UI hits this every 2s) would
// crash the entire API service, not just fail one request. Every route
// below goes through this rather than relying on its own try/catch.
const wrap = (fn) => (req, res, next) => fn(req, res, next).catch(next);

const app = express();
app.use(cors());
app.use(express.json());

app.get('/health', wrap(async (req, res) => {
  let dbOk = true;
  try { await db.pool.query('SELECT 1'); } catch { dbOk = false; }
  res.json({ ok: dbOk, service: 'api' });
}));

app.get('/api/status', wrap(async (req, res) => {
  const [checkpoints, totalSource, maxIdVal, dlqPending, esHealth, mqHealth,
    backfillStats, incrementalStats, consumerStats, queueStats, dlqQueueStats,
    esDown, mqDown] = await Promise.all([
    db.getCheckpoints(),
    db.countRecords(),
    db.maxId(),
    db.countDlq('pending'),
    es.health(),
    rabbitmq.health(),
    fetchJson(`${PIPELINE_BACKFILL_URL}/status`),
    fetchJson(`${PIPELINE_INCREMENTAL_URL}/status`),
    fetchJson(`${CONSUMER_URL}/status`),
    rabbitmq.getQueueStats('records.events.q'),
    rabbitmq.getQueueStats('records.events.dlq'),
    redis.get('chaos:es_down'),
    redis.get('chaos:mq_down'),
  ]);

  const backfillCp = checkpoints.find((c) => c.mode === 'backfill') || {};
  const incrementalCp = checkpoints.find((c) => c.mode === 'incremental') || {};

  const progressPct = maxIdVal > 0 ? Math.min(100, (Number(backfillCp.last_id || 0) / maxIdVal) * 100) : 0;

  res.json({
    source: { total_records: totalSource, max_id: maxIdVal },
    backfill: {
      status: backfillCp.status,
      last_id: Number(backfillCp.last_id || 0),
      rows_processed: Number(backfillCp.rows_processed || 0),
      progress_pct: Number(progressPct.toFixed(2)),
      throughput_rows_per_sec: backfillStats?.throughput_rows_per_sec || 0,
      reachable: backfillStats !== null,
    },
    incremental: {
      status: incrementalCp.status,
      lag_seconds: incrementalStats?.gauges?.lag_seconds ?? null,
      rows_processed: Number(incrementalCp.rows_processed || 0),
      throughput_rows_per_sec: incrementalStats?.throughput_rows_per_sec || 0,
      reachable: incrementalStats !== null,
    },
    sinks: {
      elasticsearch: { ...esHealth },
      rabbitmq: { ...mqHealth, queue: queueStats, dlq_queue: dlqQueueStats },
    },
    consumer: {
      reachable: consumerStats !== null,
      processed_total: consumerStats?.counters?.processed || 0,
      duplicates_skipped_total: consumerStats?.counters?.duplicates_skipped || 0,
      errors_total: consumerStats?.counters?.errors || 0,
    },
    dlq: { pending_count: dlqPending },
    chaos: { es_down: !!esDown, mq_down: !!mqDown },
    overall_health: computeOverallHealth(esHealth, mqHealth, backfillStats, incrementalStats) ? 'healthy' : 'degraded',
  });
}));

function computeOverallHealth(esHealth, mqHealth, backfillStats, incrementalStats) {
  return esHealth.ok && mqHealth.ok && backfillStats !== null && incrementalStats !== null;
}

app.get('/api/records', wrap(async (req, res) => {
  const { q, status, country, page, limit } = req.query;
  const result = await es.searchRecords({
    q, status, country,
    page: parseInt(page || '1', 10),
    limit: Math.min(100, parseInt(limit || '25', 10)),
  });
  res.json(result);
}));

app.get('/api/records/:id', wrap(async (req, res) => {
  const record = await es.getRecordById(req.params.id);
  if (!record) return res.status(404).json({ error: 'not found' });
  res.json(record);
}));

app.get('/api/dlq', wrap(async (req, res) => {
  const status = req.query.status || 'pending';
  const [rows, count] = await Promise.all([
    db.listDlq({ status, limit: parseInt(req.query.limit || '50', 10) }),
    db.countDlq(status),
  ]);
  res.json({ total: count, rows });
}));

app.post('/api/dlq/:id/replay', wrap(async (req, res) => {
  const dlqRow = await db.getDlqById(req.params.id);
  if (!dlqRow) return res.status(404).json({ error: 'not found' });

  // Re-read the CURRENT source row rather than resubmitting the frozen bad
  // payload — replay is only meaningful after the underlying data (or bug)
  // has actually been fixed.
  const current = await db.getCurrentRecord(dlqRow.source_record_id);
  if (!current) {
    return res.status(404).json({ error: 'source record no longer exists' });
  }
  if (String(current.amount) === 'N/A' || isNaN(Number(current.amount))) {
    return res.status(422).json({ error: 'source row still has a non-numeric amount — fix it before replaying' });
  }

  try {
    await es.reindexDoc(current.id, {
      external_id: current.external_id,
      name: current.name,
      email: current.email,
      company: current.company,
      city: current.city,
      country: current.country,
      status: current.status,
      tags: current.tags,
      amount: current.amount,
      version: current.version,
      created_at: current.created_at,
      updated_at: current.updated_at,
    });
    await db.markDlqReplayed(dlqRow.id);
    res.json({ ok: true });
  } catch (err) {
    await db.bumpDlqAttempt(dlqRow.id, err.message || err);
    res.status(500).json({ error: String(err.message || err) });
  }
}));

app.post('/api/control/:mode/:action', wrap(async (req, res) => {
  const { mode, action } = req.params;
  if (!['backfill', 'incremental'].includes(mode)) return res.status(400).json({ error: 'bad mode' });
  if (!['pause', 'resume'].includes(action)) return res.status(400).json({ error: 'bad action' });
  await db.setCheckpointStatus(mode, action === 'pause' ? 'paused' : 'idle');
  res.json({ ok: true });
}));

app.post('/api/simulate/corrupt', wrap(async (req, res) => {
  const count = parseInt(req.body?.count || '3', 10);
  const ids = await db.insertCorruptRows(count);
  res.json({ ok: true, ids });
}));

app.post('/api/simulate/outage/:sink/:action', wrap(async (req, res) => {
  const { sink, action } = req.params;
  if (!['elasticsearch', 'rabbitmq'].includes(sink)) return res.status(400).json({ error: 'bad sink' });
  if (!['start', 'stop'].includes(action)) return res.status(400).json({ error: 'bad action' });
  const key = sink === 'elasticsearch' ? 'chaos:es_down' : 'chaos:mq_down';
  if (action === 'start') await redis.set(key, '1');
  else await redis.del(key);
  res.json({ ok: true, sink, active: action === 'start' });
}));

// Catches whatever `wrap` forwards via next(err) — keeps one bad request
// from ever taking down the whole process.
app.use((err, req, res, next) => {
  console.error(`[api] ${req.method} ${req.path} error:`, err.message || err);
  if (res.headersSent) return next(err);
  res.status(500).json({ error: String(err.message || err) });
});

app.listen(PORT, () => console.log(`[api] listening on :${PORT}`));
