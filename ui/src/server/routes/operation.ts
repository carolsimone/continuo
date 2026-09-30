import { NODE_ONLY_OPERATIONS, RUN_OPERATIONS, type RunOperation } from '../shared/operation';

// parseOperation normalises the run-operation body field to the wire values the
// state trigger RPCs accept. "" and "run" both mean the default run operation
// (sent as ""); "test"/"build" pass through; anything else is rejected (null),
// so the route can 400 before ever calling gRPC.
const ALLOWED: ReadonlySet<string> = new Set<string>(['', ...RUN_OPERATIONS]);

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
const NODE_OPS: ReadonlySet<string> = new Set<string>(RUN_OPERATIONS);

export function parseNodeOperation(raw: unknown): RunOperation | null {
  if (raw == null || raw === '') return 'run';
  if (typeof raw !== 'string' || !NODE_OPS.has(raw)) return null;
  return raw as RunOperation;
}

// parseNodeRunOperation normalises the single-node run body field. It accepts
// every parseOperation value plus the single-node-only operations
// ("full_refresh", which rebuilds one dbt model or seed from scratch), which
// pass through unchanged.
const NODE_ONLY_OPS: ReadonlySet<string> = new Set<string>(NODE_ONLY_OPERATIONS);

export function parseNodeRunOperation(raw: unknown): string | null {
  if (typeof raw === 'string' && NODE_ONLY_OPS.has(raw)) return raw;
  return parseOperation(raw);
}
