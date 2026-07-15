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

// parseNodeOperation normalises the Nodes-tab operation filter to the DB domain
// the state read RPCs expect: 'run' | 'test' | 'build'. Empty/absent and 'run'
// both mean model ('run'); 'test'/'build' pass through; anything else is
// rejected (null) so the route can 400 before calling gRPC.
const NODE_OPS = new Set(['run', 'test', 'build']);

export function parseNodeOperation(raw: unknown): 'run' | 'test' | 'build' | null {
  if (raw == null || raw === '') return 'run';
  if (typeof raw !== 'string' || !NODE_OPS.has(raw)) return null;
  return raw as 'run' | 'test' | 'build';
}
