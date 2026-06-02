import { describe, it, expect } from 'vitest';
import { firstInFlight, IN_FLIGHT_STATUSES } from '../../src/client/release-helpers';
import { ReleaseListItem } from '../../src/client/types';

const mk = (id: string, status: string): ReleaseListItem => ({
  release_id: id, status, created_at: '', resolved_at: null, node_count: 0, bootstrap: false,
});

describe('firstInFlight', () => {
  it('returns the first non-terminal release', () => {
    const list = [mk('a', 'promoted'), mk('b', 'validating'), mk('c', 'received')];
    expect(firstInFlight(list)?.release_id).toBe('b');
  });
  it('returns null when all terminal', () => {
    expect(firstInFlight([mk('a', 'promoted'), mk('b', 'rejected')])).toBeNull();
  });
  it('IN_FLIGHT_STATUSES covers the three active states', () => {
    expect(IN_FLIGHT_STATUSES).toEqual(['received', 'parsing', 'validating']);
  });
});
