const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  lazyConnect: false,
  retryStrategy: (times) => Math.min(times * 200, 5000),
});
redis.on('error', () => {}); // logged via health checks instead of noisy stderr

module.exports = redis;
