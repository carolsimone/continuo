import { Router } from 'express';
import { readFileSync } from 'fs';

export function createConfigRouter(configFilePath: string) {
  const router = Router();

  router.get('/', (_req, res) => {
    try {
      // Reads on every request intentionally — admin-only path, allows config hot-reload without restart.
      const raw = readFileSync(configFilePath, 'utf8');
      const parsed = JSON.parse(raw);
      res.json(parsed);
    } catch {
      res.status(503).json({ error: 'Configuration unavailable' });
    }
  });

  return router;
}
