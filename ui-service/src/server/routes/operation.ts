// parseOperation normalises the run-operation body field to the wire values the
// state trigger RPCs accept. "" and "run" both mean the default run operation
// (sent as ""); "test"/"build" pass through; anything else is rejected (null),
// so the route can 400 before ever calling gRPC.
const ALLOWED = new Set(['', 'run', 'test', 'build']);

export function parseOperation(raw: unknown): string | null {
  if (typeof raw !== 'string' && raw != null) return null;
  const v = raw == null ? '' : raw;
  if (!ALLOWED.has(v)) return null;
  return v === 'run' ? '' : v;
}
