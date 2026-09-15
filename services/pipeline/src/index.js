const { makeMetrics } = require('./metrics');
const db = require('./db');
const es = require('./es');
const mq = require('./mq');

const MODE = process.env.MODE; // 'backfill' | 'incremental'
const PORT = parseInt(process.env.PORT || '9100', 10);

if (MODE !== 'backfill' && MODE !== 'incremental') {
  console.error('MODE must be "backfill" or "incremental"');
  process.exit(1);
}

const metrics = makeMetrics(MODE);

async function healthCheck() {
  const [esHealth, mqHealth] = await Promise.all([es.health(), mq.health()]);
  let dbOk = true;
  try { await db.pool.query('SELECT 1'); } catch { dbOk = false; }
  const ok = dbOk && esHealth.ok && mqHealth.ok;
  return { ok, status: ok ? 'healthy' : 'degraded', checks: { db: dbOk, elasticsearch: esHealth, rabbitmq: mqHealth } };
}

metrics.startServer(PORT, healthCheck);

async function main() {
  if (MODE === 'backfill') {
    await require('./backfill').runBackfill(metrics);
  } else {
    await require('./incremental').runIncremental(metrics);
  }
}

main().catch((err) => {
  console.error(`[${MODE}] fatal error`, err);
  process.exit(1);
});

process.on('SIGTERM', () => {
  console.log(`[${MODE}] SIGTERM received, exiting`);
  process.exit(0);
});
