import Redis from 'ioredis';

export function createRedisClient(): Redis {
  const url = process.env.REDIS_URL;
  if (!url) {
    throw new Error('REDIS_URL environment variable is required');
  }
  return new Redis(url);
}
