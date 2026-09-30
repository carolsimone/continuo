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
