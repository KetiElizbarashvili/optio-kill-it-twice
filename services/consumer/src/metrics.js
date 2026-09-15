const http = require('http');

function makeMetrics() {
  const counters = Object.create(null);
  function inc(name, val = 1) { counters[name] = (counters[name] || 0) + val; }

  function prometheusText() {
    return Object.entries(counters)
      .map(([k, v]) => `consumer_${k}_total ${v}`)
      .join('\n') + '\n';
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
        res.end(JSON.stringify({ counters }));
      } else {
        res.writeHead(404); res.end();
      }
    });
    server.listen(port, () => console.log(`[consumer] metrics/health on :${port}`));
    return server;
  }

  return { inc, prometheusText, startServer, counters };
}

module.exports = { makeMetrics };
