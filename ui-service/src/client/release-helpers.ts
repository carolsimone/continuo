import { ReleaseListItem } from './types';

// In-flight = a release actively moving through the pipeline, in lifecycle order:
// received -> compiling -> parsing -> seed_building -> validating.
export const IN_FLIGHT_STATUSES = ['received', 'compiling', 'parsing', 'seed_building', 'validating'];

// firstInFlight returns the newest non-terminal release (the candidate currently
// moving through the pipeline), or null if every listed release is terminal.
export function firstInFlight(items: ReleaseListItem[]): ReleaseListItem | null {
  return items.find(r => IN_FLIGHT_STATUSES.includes(r.status)) ?? null;
}

// releasePillClass maps a status to a design-system pill variant. It handles
// release lifecycle statuses (promoted/rejected/validating/…) and per-node
// validation statuses (`ok`/`failed`), and falls back to run-style keyword
// matching, defaulting to pending when nothing matches.
export function releasePillClass(status: string): string {
  switch (status) {
    case 'promoted':   return 'pill--succeeded';
    case 'ok':         return 'pill--succeeded';
    case 'rejected':   return 'pill--failed';
    case 'validating':
    case 'seed_building':
    case 'compiling':
    case 'parsing':    return 'pill--running';
    case 'received':   return 'pill--pending';
    case 'superseded': return 'pill--cancelled';
  }
  if (status.includes('succeed')) return 'pill--succeeded';
  if (status.includes('fail'))    return 'pill--failed';
  if (status.includes('cancel'))  return 'pill--cancelled';
  if (status.includes('run'))     return 'pill--running';
  return 'pill--pending';
}
