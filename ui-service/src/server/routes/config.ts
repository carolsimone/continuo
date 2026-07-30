import { Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { readFileSync } from 'fs';

export function createConfigRouter(configFilePath: string) {
  const router = Router();

  // Each request re-reads the config file from disk (intentional, see below),
  // so bound it per client IP. The UI fetches this once per cancel dialog.
  // xForwardedForHeader validation is off because the service runs behind an
  // ingress that always sets the header while Express has no trust proxy.
  router.use(
    rateLimit({
      windowMs: 60 * 1000,
      limit: 120,
      standardHeaders: true,
      legacyHeaders: false,
      validate: { xForwardedForHeader: false },
    }),
  );

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
