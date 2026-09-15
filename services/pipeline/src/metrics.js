const http = require('http');

function makeMetrics(mode) {
  const counters = Object.create(null);
  const gauges = Object.create(null);
  const samples = []; // {t, n} rolling window for rows/sec

  function inc(name, val = 1) {
    counters[name] = (counters[name] || 0) + val;
  }
  function setGauge(name, val) {
    gauges[name] = val;
  }
  function recordRows(n) {
    const now = Date.now();
    samples.push({ t: now, n });
    while (samples.length && now - samples[0].t > 30000) samples.shift();
  }
  function throughput() {
    if (samples.length < 2) return 0;
    const now = Date.now();
    const windowStart = Math.max(samples[0].t, now - 30000);
    const total = samples.reduce((s, x) => s + x.n, 0);
    const seconds = Math.max(1, (now - windowStart) / 1000);
    return total / seconds;
  }

  function prometheusText() {
    const lines = [];
    const push = (name, val, help) => {
      if (help) lines.push(`# HELP ${name} ${help}`);
      lines.push(`${name}{mode="${mode}"} ${val}`);
    };
    for (const [k, v] of Object.entries(counters)) push(`pipeline_${k}_total`, v);
    for (const [k, v] of Object.entries(gauges)) push(`pipeline_${k}`, v);
    push('pipeline_throughput_rows_per_sec', throughput().toFixed(2));
    return lines.join('\n') + '\n';
  }

  function startServer(port, healthCheckFn) {
    const server = http.createServer(async (req, res) => {
      if (req.url === '/metrics') {
        res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
        res.end(prometheusText());
      } else if (req.url === '/health') {
        const h = await healthCheckFn();
        res.writeHead(h.ok ? 200 : 503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(h));
      } else if (req.url === '/status') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ mode, counters, gauges, throughput_rows_per_sec: throughput() }));
      } else {
        res.writeHead(404); res.end();
      }
    });
    server.listen(port, () => console.log(`[${mode}] metrics/health on :${port}`));
    return server;
  }

  return { inc, setGauge, recordRows, throughput, prometheusText, startServer, counters, gauges };
}

module.exports = { makeMetrics };
