function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

// Exponential backoff with jitter, capped — this is what keeps a sink
// outage (G3) from turning into a CPU busy-loop while still retrying
// promptly once the sink recovers.
function backoffMs(attempt, base = 500, max = 10000) {
  const exp = Math.min(max, base * 2 ** attempt);
  return Math.floor(exp * (0.5 + Math.random() * 0.5));
}

module.exports = { sleep, backoffMs };
