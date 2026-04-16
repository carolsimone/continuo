import Redis from 'ioredis';

export function createRedisClient(): Redis | null {
  const url = process.env.REDIS_URL;
  if (!url) {
    console.warn('REDIS_URL not set — graph update endpoint will be unavailable');
    return null;
  }
  return new Redis(url);
}
