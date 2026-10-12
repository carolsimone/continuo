// Pure functions for the topology-drift badge shown next to the run-level
// Rerun button. No React, no DOM. Single source of truth for drift semantics.
//
// Backend contract:
//   run_promotion_seq = 0 → drift unknown (the run carries no promotion seq).
//   run_promotion_seq < latest_promotion_seq → stale.
//   run_promotion_seq >= latest_promotion_seq → fresh.

export type DriftState = 'fresh' | 'stale' | 'unknown';

/**
 * Compute the drift state for a run created under promotion seq `runSeq`
 * against the live promotion seq `latestSeq`.
 */
export function getDriftState(
  runSeq: number | null | undefined,
  latestSeq: number | null | undefined,
): DriftState {
  if (runSeq === null || runSeq === undefined) return 'unknown';
  const r = Number(runSeq);
  if (Number.isNaN(r)) return 'unknown';
  if (r === 0) return 'unknown';
  const l = Number.isFinite(Number(latestSeq)) ? Number(latestSeq) : 0;
  if (r >= l) return 'fresh';
  return 'stale';
}

/**
 * Render the badge label shown when the run was created under an older
 * promotion seq than the live one: the number of promotions since, worded as
 * releases, singular for one. Callers invoke this only when state === 'stale';
 * the 'unknown' state (a run without a promotion seq) is never surfaced in the UI.
 */
export function getDriftBadge(
  state: 'stale',
  runSeq: number,
  latestSeq: number,
): string {
  const behind = latestSeq - runSeq;
  return `source ${behind} ${behind === 1 ? 'release' : 'releases'} behind latest`;
}
