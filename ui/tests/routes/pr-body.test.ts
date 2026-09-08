import { describe, expect, it } from 'vitest';
import { buildPullRequestBody } from '../../src/server/routes/pr-body';

describe('buildPullRequestBody', () => {
  it('puts the diff before the rationale and keeps the rationale line breaks', () => {
    const body = buildPullRequestBody({
      nodeIds: ['analytics.report'],
      releaseId: 'rel-1',
      files: [{ path: 'services/core/models/orders.sql', target_node_id: 'analytics.orders' }],
      rationale: 'Failed: `analytics.report`\nModel\'s note: kept both',
      diff: '-select amount\n+select amount_eur, amount_eur as amount',
    });
    const changes = body.indexOf('### Changes');
    const rationale = body.indexOf('### Rationale');
    expect(changes).toBeGreaterThan(body.indexOf('### Files changed'));
    expect(rationale).toBeGreaterThan(changes);
    expect(body).toContain('```diff\n-select amount\n+select amount_eur, amount_eur as amount\n```');
    expect(body).toContain('### Rationale\nFailed: `analytics.report`\nModel\'s note: kept both');
    expect(body).toContain('- `services/core/models/orders.sql` (fixes `analytics.orders`)');
  });

  it('omits the sections it has nothing for', () => {
    const body = buildPullRequestBody({ nodeIds: ['a'], releaseId: 'r', files: [{ path: 'p' }] });
    expect(body).not.toContain('### Changes');
    expect(body).not.toContain('### Rationale');
    expect(body).toContain('**Nodes:** `a`');
  });
});
