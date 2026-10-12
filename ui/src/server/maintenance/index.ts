import type { RequestHandler, Response } from 'express';

// Maintenance mode: while MAINTENANCE_ENABLED is "true", continuo starts no new
// work. These literals equal pkg/maintenance on the Go side; the deploycheck
// wiring guard fails when the two drift.
export const MAINTENANCE_MESSAGE = 'continuo is in maintenance mode: new work is not accepted until it is turned off';
export const MAINTENANCE_CODE = 'maintenance';
export const MAINTENANCE_TRAILER_KEY = 'continuo-maintenance';
export const MAINTENANCE_RETRY_AFTER_SECONDS = 300;

// loadMaintenance reads MAINTENANCE_ENABLED, which must be exactly "true" or
// "false"; anything else stops the ui at boot rather than guessing.
export function loadMaintenance(env: NodeJS.ProcessEnv): boolean {
  const raw = env.MAINTENANCE_ENABLED;
  if (raw === 'true') return true;
  if (raw === 'false') return false;
  throw new Error(`MAINTENANCE_ENABLED must be exactly "true" or "false" (got ${raw === undefined ? 'nothing' : JSON.stringify(raw)})`);
}

// sendMaintenance answers a request refused because maintenance mode is on.
export function sendMaintenance(res: Response): void {
  res.set('Retry-After', String(MAINTENANCE_RETRY_AFTER_SECONDS));
  res.status(503).json({ error: MAINTENANCE_MESSAGE, code: MAINTENANCE_CODE });
}

// The routes that start new work, as paths below /api. Matched without regard
// to case or a trailing slash, as Express routes them.
const GATED: ReadonlyArray<readonly [string, RegExp]> = [
  ['POST', /^\/schedules\/[^/]+\/trigger\/?$/i],
  ['POST', /^\/schedulers\/[^/]+\/rerun\/?$/i],
  ['POST', /^\/schedulers\/[^/]+\/rebase\/?$/i],
  ['POST', /^\/nodes\/[^/]+\/[^/]+\/[^/]+\/run\/?$/i],
  ['POST', /^\/v1\/releases\/?$/i],
  ['POST', /^\/releases\/[^/]+\/retry-remediation\/?$/i],
];

// maintenanceGate refuses the routes that start new work while enabled; it is
// mounted on /api after authentication, so an unauthenticated caller still
// gets its 401.
export function maintenanceGate(enabled: boolean): RequestHandler {
  return (req, res, next) => {
    if (enabled && GATED.some(([method, path]) => req.method === method && path.test(req.path))) {
      sendMaintenance(res);
      return;
    }
    next();
  };
}

// isMaintenanceRefusal reports whether a gRPC error from a backend is a
// maintenance refusal: the call failed with the maintenance trailer set.
export function isMaintenanceRefusal(err: unknown): boolean {
  const md = (err as { metadata?: { get?: (key: string) => unknown[] } } | null)?.metadata;
  return typeof md?.get === 'function' && md.get(MAINTENANCE_TRAILER_KEY)?.[0] === 'true';
}
