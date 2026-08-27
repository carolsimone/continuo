import { Router } from 'express';
import { ReleaseClient } from '../release-client';

// normalizeKey strips a leading s3://<bucket>/ so getLogObject receives a key.
function normalizeKey(raw: string): string {
  const m = raw.match(/^s3:\/\/[^/]+\/(.+)$/);
  return m ? m[1] : raw;
}

export function createReleasesRouter(
  client: ReleaseClient,
  getLog: (key: string) => Promise<string>,
) {
  const router = Router();

  // GET /api/releases/log?key=  — registered before /:id so "log" is not treated as an id.
  router.get('/log', async (req, res) => {
    const key = (req.query.key as string) || '';
    if (!key) return res.status(400).json({ error: 'key query param is required' });
    try {
      const content = await getLog(normalizeKey(key));
      res.setHeader('Content-Type', 'text/plain');
      res.send(content);
    } catch {
      res.status(502).json({ error: 'Failed to fetch log from storage' });
    }
  });

  // GET /api/releases/current-prod
  router.get('/current-prod', async (_req, res) => {
    try {
      res.json(await client.getCurrentProd());
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });

  // GET /api/releases?status=&limit=&cursor=
  router.get('/', async (req, res) => {
    const query: Record<string, string> = {};
    for (const k of ['status', 'limit', 'cursor']) {
      const v = req.query[k];
      if (typeof v === 'string' && v !== '') query[k] = v;
    }
    try {
      res.json(await client.listReleases(query));
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });

  // POST /api/releases/:id/retry-remediation — start another remediation round.
  // release-controller's status and body are passed through: 202 with the new
  // round, or 409 with a reason the page turns into "look at the proposal instead".
  router.post('/:id/retry-remediation', async (req, res) => {
    try {
      const { status, body } = await client.retryRemediation(req.params.id);
      res.status(status).json(body);
    } catch {
      res.status(502).json({ error: 'release-controller request failed' });
    }
  });

  // GET /api/releases/:id
  router.get('/:id', async (req, res) => {
    try {
      res.json(await client.getRelease(req.params.id));
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });

  return router;
}
