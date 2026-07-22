import { Router } from 'express';
import { readFileSync } from 'fs';

export function createConfigRouter(configFilePath: string) {
  const router = Router();

  router.get('/', (_req, res) => {
    try {
      // Reads on every request intentionally — admin-only path, allows config hot-reload without restart.
      const raw = readFileSync(configFilePath, 'utf8');
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed.cancellation_reasons)) {
        res.status(503).json({ error: 'Configuration unavailable' });
        return;
      }
      // Return only the whitelisted keys; never echo the raw file, so a stray
      // or deprecated key (e.g. cancel_by_emails) can never reach the client.
      res.json({ cancellation_reasons: parsed.cancellation_reasons });
    } catch {
      res.status(503).json({ error: 'Configuration unavailable' });
    }
  });

  return router;
}
