import Redis from 'ioredis';

export function createRedisClient(): Redis | null {
  const url = process.env.REDIS_URL;
  if (!url) {
    console.warn('REDIS_URL not set — graph update endpoint will be unavailable');
    return null;
  }
  const client = new Redis(url);
  client.on('connect', () => console.log('Redis connected'));
  client.on('error', (err) => console.error('Redis error:', err.message));
  return client;
}
