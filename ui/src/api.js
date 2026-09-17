const BASE = '/api';

async function req(path, opts) {
  const res = await fetch(`${BASE}${path}`, opts);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export const api = {
  status: () => req('/status'),
  records: (params) => req(`/records?${new URLSearchParams(params)}`),
  record: (id) => req(`/records/${id}`),
  dlq: (status = 'pending') => req(`/dlq?status=${status}`),
  replayDlq: (id) => req(`/dlq/${id}/replay`, { method: 'POST' }),
  control: (mode, action) => req(`/control/${mode}/${action}`, { method: 'POST' }),
  simulateCorrupt: (count) =>
    req('/simulate/corrupt', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count }),
    }),
  simulateDrip: (count) =>
    req('/simulate/drip', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count }),
    }),
  simulateOutage: (sink, action) => req(`/simulate/outage/${sink}/${action}`, { method: 'POST' }),
};
