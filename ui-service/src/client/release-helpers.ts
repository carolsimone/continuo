import { ReleaseListItem } from './types';

export const IN_FLIGHT_STATUSES = ['received', 'parsing', 'validating'];

// firstInFlight returns the newest non-terminal release (the candidate currently
// moving through the pipeline), or null if every listed release is terminal.
export function firstInFlight(items: ReleaseListItem[]): ReleaseListItem | null {
  return items.find(r => IN_FLIGHT_STATUSES.includes(r.status)) ?? null;
}
