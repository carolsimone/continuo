import { Router } from 'express';
import { getLogObject } from '../s3';

export function createTaskExecutionRouter() {
  const router = Router();

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
