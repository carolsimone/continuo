import { Router } from 'express';
import { ReleaseClient } from '../clients/release-client';

// createLiveReleaseRouter serves GET /api/live-release: the release the
// install runs on now, as release-controller's GET /live-release answers it.
// It sits outside /api/releases so no release id can reach it. The Releases
// tab polls it every 5s, so it has no rate limit.
export function createLiveReleaseRouter(client: ReleaseClient) {
  const router = Router();
  router.get('/', async (_req, res) => {
    try {
      res.json(await client.getLiveRelease());
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });
  return router;
}
