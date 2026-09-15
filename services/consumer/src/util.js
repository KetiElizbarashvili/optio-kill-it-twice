function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

// Exponential backoff with jitter, capped — same shape as the pipeline's
// util.js, kept duplicated rather than shared since these are deliberately
// independent services (see AGENTS.md).
function backoffMs(attempt, base = 500, max = 10000) {
  const exp = Math.min(max, base * 2 ** attempt);
  return Math.floor(exp * (0.5 + Math.random() * 0.5));
}

module.exports = { sleep, backoffMs };
