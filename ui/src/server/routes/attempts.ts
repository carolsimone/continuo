import { Router } from 'express';
import { getLogObject } from '../clients/s3';

// createAttemptsRouter serves the logs of task attempts under /api/attempts.
export function createAttemptsRouter() {
  const router = Router();

  // GET /api/attempts/:id/logs?key= — the pod log of an attempt, read from
  // object storage at the S3 key the attempt reports (log_s3_key).
  router.get('/:id/logs', async (req, res) => {
    const key = req.query.key as string;
    if (!key) {
      return res.status(400).json({ error: 'key query param is required' });
    }
    try {
      const content = await getLogObject(key);
      res.setHeader('Content-Type', 'text/plain');
      res.send(content);
    } catch (err: any) {
      res.status(502).json({ error: 'Failed to fetch log from storage' });
    }
  });

  return router;
}
