import { Router } from 'express';
import Redis from 'ioredis';

const VALID_SOURCES = ['s3', 'local'];

export function createGraphRouter(redisClient: Redis) {
  const router = Router();

  // POST /api/graph/update — publish update.graph:v1 to Redis
  router.post('/update', async (req, res) => {
    const source = req.body?.source || 's3';

    if (!VALID_SOURCES.includes(source)) {
      return res.status(400).json({ error: 'source must be "s3" or "local"' });
    }

    try {
      await redisClient.xadd('update.graph:v1', '*', 'source', source);
      res.json({ ok: true, source });
    } catch (err: any) {
      console.error('Failed to publish graph update:', err.message);
      res.status(500).json({ error: 'failed to publish graph update' });
    }
  });

  return router;
}
