// Independent consumer of the `records.events` stream. Deliberately its own
// process/deploy unit with its own state (Redis) — proves the event stream
// is a real fan-out point, not just an internal pipeline detail. See
// SPEC.md 3 and README ADR on delivery guarantees.
const amqp = require('amqplib');
const Redis = require('ioredis');
const { makeMetrics } = require('./metrics');
const { sleep, backoffMs } = require('./util');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const PORT = parseInt(process.env.PORT || '9200', 10);
const QUEUE = 'records.events.q';
const EXCHANGE = 'records.events';
const DLX = 'records.events.dlx';
const DLQ = 'records.events.dlq';
const DEDUPE_TTL_SEC = 24 * 60 * 60;

const redis = new Redis(REDIS_URL);
const metrics = makeMetrics();
let connected = false;

async function handleMessage(ch, msg) {
  if (!msg) return;
  try {
    const evt = JSON.parse(msg.content.toString());
    const dedupeKey = `consumer:seen:${evt.id}:${evt.version}`;

    // idempotency key = source row id + version. SET NX gives us an
    // atomic "have I processed this exact version before" check — this is
    // the consumer-side half of the "effectively-once" guarantee declared
    // in SPEC.md 2.3 (the broker itself only promises at-least-once).
    const claimed = await redis.set(dedupeKey, '1', 'EX', DEDUPE_TTL_SEC, 'NX');

    if (!claimed) {
      metrics.inc('duplicates_skipped');
      ch.ack(msg);
      return;
    }

    const pipeline = redis.pipeline();
    pipeline.incr('analytics:total_processed');
    pipeline.hincrby('analytics:by_status', evt.data.status || 'unknown', 1);
    pipeline.hincrby('analytics:by_country', evt.data.country || 'unknown', 1);
    pipeline.hincrby('analytics:by_op', evt.op, 1);
    await pipeline.exec();

    metrics.inc('processed');
    ch.ack(msg);
  } catch (err) {
    console.error('[consumer] processing error', err);
    metrics.inc('errors');
    // requeue=false -> broker routes to the DLX/DLQ configured on this
    // queue, instead of an infinite redelivery loop.
    ch.nack(msg, false, false);
  }
}

// Connects, consumes, and on any connection loss reconnects with capped
// backoff IN-PROCESS rather than exiting and relying on the container's
// restart policy. Mirrors the pipeline's own mq.js reconnect strategy —
// found this consumer did NOT do this (crashed on every broker restart,
// unlike the pipeline workers which stayed up) while testing a RabbitMQ
// restart by hand; see README "Where AI deviated from spec".
async function connectAndConsume(attempt = 0) {
  let conn;
  try {
    conn = await amqp.connect(RABBITMQ_URL);
  } catch (err) {
    connected = false;
    const wait = backoffMs(attempt);
    console.error(`[consumer] connect failed (${err.message}), retrying in ${wait}ms`);
    await sleep(wait);
    return connectAndConsume(attempt + 1);
  }

  let closed = false;
  const reconnect = (reason) => {
    if (closed) return; // avoid double-reconnect from both 'error' and 'close'
    closed = true;
    connected = false;
    console.error(`[consumer] connection lost (${reason}), reconnecting`);
    connectAndConsume(0);
  };
  conn.on('error', (err) => reconnect(err.message));
  conn.on('close', () => reconnect('closed'));

  const ch = await conn.createChannel();
  await ch.prefetch(50);

  // idempotent topology setup — safe even if this starts before the pipeline
  await ch.assertExchange(EXCHANGE, 'topic', { durable: true });
  await ch.assertExchange(DLX, 'fanout', { durable: true });
  await ch.assertQueue(DLQ, { durable: true });
  await ch.bindQueue(DLQ, DLX, '#');
  await ch.assertQueue(QUEUE, { durable: true, arguments: { 'x-dead-letter-exchange': DLX } });
  await ch.bindQueue(QUEUE, EXCHANGE, '#');

  connected = true;
  console.log('[consumer] connected, waiting for events...');
  ch.consume(QUEUE, (msg) => handleMessage(ch, msg));
}

metrics.startServer(PORT, async () => {
  let redisOk = true;
  try { await redis.ping(); } catch { redisOk = false; }
  return { ok: redisOk && connected, checks: { redis: redisOk, rabbitmq: connected } };
});

connectAndConsume().catch((err) => {
  // only reachable if connectAndConsume's own retry loop somehow throws
  // past its catch, e.g. a bug — a real crash here should still be loud.
  console.error('[consumer] unrecoverable', err);
  process.exit(1);
});
