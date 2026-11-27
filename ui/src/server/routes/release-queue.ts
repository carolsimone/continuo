import { Router } from 'express';
import { ReleaseClient } from '../clients/release-client';

// GET /api/release-queue — the release check release-controller is running
// right now, of either kind (a candidate release's or a fix's), or nothing.
// Polled by the Releases tab's in-flight strip; unlimited, like live-release.
export function createReleaseQueueRouter(client: ReleaseClient) {
  const router = Router();
  router.get('/', async (_req, res) => {
    try {
      res.json(await client.getReleaseQueue());
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });
  return router;
}
