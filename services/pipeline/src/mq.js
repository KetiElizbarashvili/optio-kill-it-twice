const amqp = require('amqplib');
const redis = require('./redisClient');

const URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const CHAOS_KEY = 'chaos:mq_down';
const EXCHANGE = 'records.events';
const DLX = 'records.events.dlx';
const QUEUE = 'records.events.q';
const DLQ = 'records.events.dlq';

let connection = null;
let channel = null;
let connecting = null;

async function connect() {
  if (channel) return channel;
  if (connecting) return connecting;

  connecting = (async () => {
    const conn = await amqp.connect(URL);
    conn.on('error', () => { connection = null; channel = null; });
    conn.on('close', () => { connection = null; channel = null; });

    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(EXCHANGE, 'topic', { durable: true });
    await ch.assertExchange(DLX, 'fanout', { durable: true });
    await ch.assertQueue(DLQ, { durable: true });
    await ch.bindQueue(DLQ, DLX, '#');
    await ch.assertQueue(QUEUE, {
      durable: true,
      arguments: { 'x-dead-letter-exchange': DLX },
    });
    await ch.bindQueue(QUEUE, EXCHANGE, '#');

    connection = conn;
    channel = ch;
    return ch;
  })();

  try {
    return await connecting;
  } finally {
    connecting = null;
  }
}

function opFor(row) {
  if (row.deleted_at) return 'deleted';
  return row.version > 1 ? 'updated' : 'created';
}

// Publishes one event per row and waits for broker confirms on all of them.
// Throws (without partial bookkeeping) if the connection drops mid-batch —
// caller retries the WHOLE batch, which is safe because the sink write
// (Elasticsearch, keyed by row.id) and the consumer-side dedupe (row.id +
// version) are both idempotent. See SPEC.md 2.3.
async function publishBatch(rows) {
  if (rows.length === 0) return;

  const chaos = await redis.get(CHAOS_KEY).catch(() => null);
  if (chaos) throw new Error('chaos: simulated RabbitMQ outage');

  const ch = await connect();

  await Promise.all(
    rows.map(
      (row) =>
        new Promise((resolve, reject) => {
          const op = opFor(row);
          const payload = Buffer.from(JSON.stringify({
            id: row.id,
            external_id: row.external_id,
            version: row.version,
            op,
            data: row,
            emitted_at: new Date().toISOString(),
          }));
          ch.publish(EXCHANGE, `record.${op}`, payload, { persistent: true }, (err) => {
            if (err) reject(err); else resolve();
          });
        })
    )
  );
}

async function health() {
  try {
    await connect();
    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err.message || err) };
  }
}

module.exports = { connect, publishBatch, health, EXCHANGE, QUEUE, DLX, DLQ };
