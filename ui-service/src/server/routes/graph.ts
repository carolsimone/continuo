import { Router } from 'express';
import Redis from 'ioredis';

// Until a TS generator exists for pkg/streams/contract.yaml, this is the
// single source of truth in ui-service for the update.graph:v1 stream
// name. Keep it aligned with the Go/Python generated constants.
export const UPDATE_GRAPH_STREAM = 'update.graph:v1';

const VALID_SOURCES = ['s3', 'local'];

async function publishGraphUpdate(redis: Redis, source: string, requestId: string): Promise<void> {
  await redis.xadd(UPDATE_GRAPH_STREAM, '*', 'source', source);
  console.log(`graph.update published source=${source}${requestId ? ` request_id=${requestId}` : ''}`);
}

export function createGraphRouter(redisClient: Redis | null) {
  const router = Router();

  // POST /api/graph/update — publish update.graph:v1 to Redis.
  // When GRAPH_UPDATE_TOKEN is set (non-empty), requests must carry a
  // matching `Authorization: Bearer <token>` header. When unset or empty
  // the endpoint is open, which is the local-dev and e2e default.
  router.post('/update', async (req, res) => {
    const expected = (process.env.GRAPH_UPDATE_TOKEN || '').trim();
    if (expected.length > 0) {
      const header = req.header('authorization') || '';
      const provided = header.startsWith('Bearer ') ? header.slice('Bearer '.length).trim() : '';
      if (provided !== expected) {
        return res.status(401).json({ error: 'unauthorized' });
      }
    }

    if (!redisClient) {
      return res.status(503).json({ error: 'Redis not configured (REDIS_URL not set)' });
    }

    const source = req.body?.source || 's3';

    if (!VALID_SOURCES.includes(source)) {
      return res.status(400).json({ error: 'source must be "s3" or "local"' });
    }

    const requestId = req.header('x-request-id') || '';

    try {
      await publishGraphUpdate(redisClient, source, requestId);
      res.json({ ok: true, source });
    } catch (err: any) {
      console.error('Failed to publish graph update:', err.message);
      res.status(500).json({ error: 'failed to publish graph update' });
    }
  });

  return router;
}

export function createDashboardGraphRouter(redisClient: Redis | null) {
  const router = Router();

  // POST /api/dashboard/graph-update — same-origin browser route for the
  // dashboard's manual rebuild button. Gated by Sec-Fetch-Site instead of
  // a bearer, because the React frontend cannot safely carry the
  // server-only GRAPH_UPDATE_TOKEN. Reaches the same Redis stream as the
  // bearer-gated CI route.
  router.post('/graph-update', async (req, res) => {
    const fetchSite = req.header('sec-fetch-site');
    if (fetchSite !== 'same-origin') {
      return res.status(403).json({ error: 'same-origin only; use /api/graph/update with bearer token for non-browser callers' });
    }

    if (!redisClient) {
      return res.status(503).json({ error: 'Redis not configured (REDIS_URL not set)' });
    }

    const source = req.body?.source || 's3';

    if (!VALID_SOURCES.includes(source)) {
      return res.status(400).json({ error: 'source must be "s3" or "local"' });
    }

    const requestId = req.header('x-request-id') || '';

    try {
      await publishGraphUpdate(redisClient, source, requestId);
      res.json({ ok: true, source });
    } catch (err: any) {
      console.error('Failed to publish graph update:', err.message);
      res.status(500).json({ error: 'failed to publish graph update' });
    }
  });

  return router;
}
