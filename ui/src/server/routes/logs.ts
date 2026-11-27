import { Router } from 'express';

// normalizeKey strips a leading s3://<bucket>/ so getLog receives a key.
function normalizeKey(raw: string): string {
  const m = raw.match(/^s3:\/\/[^/]+\/(.+)$/);
  return m ? m[1] : raw;
}

// createLogsRouter serves GET /api/logs?key=: the text of one stored object
// (a leg's dbt log, a fix's diff), read from object storage by its key or its
// s3:// URI. It sits outside /api/releases so no release id can reach it.
export function createLogsRouter(getLog: (key: string) => Promise<string>) {
  const router = Router();
  router.get('/', async (req, res) => {
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
  return router;
}
