import { Router } from 'express';
import { ReleaseClient } from '../release-client';

// GET /api/pipeline — what the release pipeline is doing right now: the
// active run of either kind, or nothing. Polled by the Releases tab's
// in-flight strip; unlimited, like current-prod.
export function createPipelineRouter(client: ReleaseClient) {
  const router = Router();
  router.get('/', async (_req, res) => {
    try {
      res.json(await client.getPipeline());
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });
  return router;
}
