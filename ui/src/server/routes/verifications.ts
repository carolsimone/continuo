import { Router } from 'express';
import { ReleaseClient } from '../release-client';

// Verification runs are read-only from the UI: the detail page polls one run
// by id. Submission belongs to agent-remediation, never to an operator.
export function createVerificationsRouter(client: ReleaseClient) {
  const router = Router();
  // GET /api/verifications/:id
  router.get('/:id', async (req, res) => {
    try {
      res.json(await client.getVerificationRun(req.params.id));
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });
  return router;
}
