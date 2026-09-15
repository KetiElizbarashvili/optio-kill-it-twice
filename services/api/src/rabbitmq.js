const MGMT_URL = process.env.RABBITMQ_MGMT_URL || 'http://localhost:15672';
const AUTH = 'Basic ' + Buffer.from('guest:guest').toString('base64');

async function getQueueStats(name) {
  try {
    const res = await fetch(`${MGMT_URL}/api/queues/%2F/${encodeURIComponent(name)}`, {
      headers: { Authorization: AUTH },
      signal: AbortSignal.timeout(3000),
    });
    if (!res.ok) return { exists: false };
    const data = await res.json();
    return {
      exists: true,
      messages: data.messages,
      messages_ready: data.messages_ready,
      consumers: data.consumers,
      publish_rate: data.message_stats?.publish_details?.rate || 0,
      deliver_rate: data.message_stats?.deliver_get_details?.rate || 0,
    };
  } catch {
    return { exists: false, unreachable: true };
  }
}

async function health() {
  try {
    const res = await fetch(`${MGMT_URL}/api/overview`, { headers: { Authorization: AUTH }, signal: AbortSignal.timeout(3000) });
    return { ok: res.ok };
  } catch {
    return { ok: false };
  }
}

module.exports = { getQueueStats, health };
