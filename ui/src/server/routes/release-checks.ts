import { Router } from 'express';
import { ReleaseClient } from '../clients/release-client';

// A fix's release check is read-only from the UI: the detail page polls one
// by id. Submission belongs to agent-remediation, never to an operator.
export function createReleaseChecksRouter(client: ReleaseClient) {
  const router = Router();
  // GET /api/release-checks/:id
  router.get('/:id', async (req, res) => {
    try {
      res.json(await client.getReleaseCheck(req.params.id));
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });
  return router;
}
