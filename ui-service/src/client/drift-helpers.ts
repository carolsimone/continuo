// Pure functions that compute topology-drift state and render all user-facing
// copy. No React, no DOM. Single source of truth for drift semantics — both
// the dashboard strip and the rerun-confirm modal call into here.
//
// Backend contract (from feat/topology-drift-detection):
//   run_topology_generation = 0 → run pre-dates topology tracking (drift
//                                  unknown — we cannot say if the pinned
//                                  snapshot matches the current topology).
//   run_topology_generation < latest_topology_generation → stale.
//   run_topology_generation >= latest_topology_generation → fresh
//                                  (>= rather than === is defensive against
//                                  an invariant violation; the orchestrator
//                                  logs the inversion separately).

export type DriftState = 'fresh' | 'stale' | 'unknown';

/**
 * Compute the drift state for a run pinned to `runGen` against the
 * orchestrator's current `latestGen`.
 *
 * Defensive: any non-numeric or null/undefined `runGen` returns 'unknown'.
 * `latestGen` of null/undefined coerces to 0 (treated as "no manifest ever
 * ingested"), in which case any positive `runGen` is 'fresh' by the
 * `>=` rule. `runGen === 0` always wins → 'unknown'.
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

export interface DriftMessage {
  stripLine: string;
  modalTitle: string;
  modalChip: string;
  modalBody: string;
  modalCta: string;
}

/**
 * Render the user-facing copy for a drift state. The numeric arguments are
 * only consulted in the 'stale' branch; the 'unknown' branch ignores them.
 * Callers are expected to invoke this only when state !== 'fresh'.
 */
export function getDriftMessage(
  state: 'stale' | 'unknown',
  runGen: number,
  latestGen: number,
): DriftMessage {
  if (state === 'unknown') {
    return {
      stripLine:
        'Topology version unknown for this run. May not match the current topology.',
      modalTitle: 'Topology unknown',
      modalChip: 'no version recorded',
      modalBody:
        "This run started before topology tracking existed. We can't tell if its pinned snapshot matches the current topology. The rerun will use whatever the run captured at start.\n\nTo guarantee a current run, trigger a fresh schedule run instead.",
      modalCta: 'Rerun anyway',
    };
  }

  const delta = latestGen - runGen;
  const noun = delta === 1 ? 'time' : 'times';
  return {
    stripLine: `Stale — pinned to topology gen ${runGen}; latest is ${latestGen}. Rerun will use the older snapshot.`,
    modalTitle: 'Stale topology',
    modalChip: `${runGen} → ${latestGen}`,
    modalBody: `The dbt manifest has been re-ingested ${delta} ${noun} since this run started. Rerunning will not pick up those changes — it reuses the original snapshot.\n\nTo run with the latest topology, trigger a fresh schedule run from the dashboard instead.`,
    modalCta: 'Rerun with old snapshot',
  };
}
