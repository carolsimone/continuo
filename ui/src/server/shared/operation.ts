// Operation values the ui offers and sends, declared once for its server and
// client alike. The module depends on nothing, so the client bundle can import
// it without pulling in server code.

// RUN_OPERATIONS are the operations a run applies to its nodes: a schedule
// trigger and a single-node run both offer them, and a node's run history is
// filtered by them.
export const RUN_OPERATIONS = ['run', 'test', 'build'] as const;
export type RunOperation = (typeof RUN_OPERATIONS)[number];

// NODE_ONLY_OPERATIONS exist only for a single-node run: full_refresh rebuilds
// one dbt model or seed from scratch.
export const NODE_ONLY_OPERATIONS = ['full_refresh'] as const;

// NODE_RUN_OPERATIONS are every operation a single-node run accepts.
export const NODE_RUN_OPERATIONS = [...RUN_OPERATIONS, ...NODE_ONLY_OPERATIONS] as const;
export type NodeOperation = (typeof NODE_RUN_OPERATIONS)[number];

export function isNodeOperation(v: unknown): v is NodeOperation {
  return typeof v === 'string' && (NODE_RUN_OPERATIONS as readonly string[]).includes(v);
}

// WireOperation is an operation as the ui reports it: a known NodeOperation,
// or the raw string a backend newer than this ui sent. The intersection with
// {} keeps the known literals visible to autocomplete and narrowing.
export type WireOperation = NodeOperation | (string & {});

// operationFromWire maps the state service's operation to the value the ui
// reports. An empty or absent value is a plain run. A value this ui does not
// know is passed through unchanged so a newer backend's operation is shown as
// sent rather than disguised as a run.
export function operationFromWire(v: unknown): WireOperation {
  if (typeof v !== 'string' || v === '') return 'run';
  return v;
}
