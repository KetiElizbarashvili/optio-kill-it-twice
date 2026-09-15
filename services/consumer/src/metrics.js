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
    // See the pipeline's metrics.js for why this try/catch matters: a throw
    // in an async http.createServer callback is an unhandled rejection,
    // which kills the process by default — over what should be a single
    // failed health-check response.
    const server = http.createServer(async (req, res) => {
      try {
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
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: String(err.message || err) }));
      }
    });
    server.listen(port, () => console.log(`[consumer] metrics/health on :${port}`));
    return server;
  }

  return { inc, prometheusText, startServer, counters };
}

module.exports = { makeMetrics };
