// Pure functions for the topology-drift badge shown next to the run-level
// Rerun button. No React, no DOM. Single source of truth for drift semantics.
//
// Backend contract:
//   run_topology_generation = 0 → drift unknown (run pre-dates tracking).
//   run_topology_generation < latest_topology_generation → stale.
//   run_topology_generation >= latest_topology_generation → fresh.

export type DriftState = 'fresh' | 'stale' | 'unknown';

/**
 * Compute the drift state for a run pinned to `runGen` against the
 * orchestrator's current `latestGen`.
 */
export function getDriftState(
  runGen: number | null | undefined,
  latestGen: number | null | undefined,
): DriftState {
  if (runGen === null || runGen === undefined) return 'unknown';
  const r = Number(runGen);
  if (Number.isNaN(r)) return 'unknown';
  if (r === 0) return 'unknown';
  const l = Number.isFinite(Number(latestGen)) ? Number(latestGen) : 0;
  if (r >= l) return 'fresh';
  return 'stale';
}

/**
 * Render the badge label shown next to the Rerun button when the run is
 * pinned to an older topology generation than the latest. Callers invoke
 * this only when state !== 'fresh'.
 */
export function getDriftBadge(
  state: 'stale' | 'unknown',
  runGen: number,
  latestGen: number,
): string {
  if (state === 'unknown') return 'topology version unknown';
  return `source ${latestGen - runGen} gen behind latest`;
}
