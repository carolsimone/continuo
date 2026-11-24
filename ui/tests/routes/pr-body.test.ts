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

  it('names the path a renamed file moves from, and keeps the line of every other file', () => {
    const body = buildPullRequestBody({
      nodeIds: ['analytics.table_b'],
      releaseId: 'rel-1',
      files: [
        {
          path: 'services/marketing/models/table_b_v2.sql',
          renamed_from: 'services/marketing/models/table_b.sql',
          target_node_id: 'analytics.table_b',
        },
        { path: 'services/marketing/models/table_c_v2.sql', renamed_from: 'services/marketing/models/table_c.sql' },
        { path: 'services/core/models/orders.sql', target_node_id: 'analytics.orders' },
        { path: 'services/core/models/shared.sql' },
      ],
    });
    expect(body).toContain(
      '### Files changed\n' +
        '- `services/marketing/models/table_b_v2.sql` (renamed from `services/marketing/models/table_b.sql`; fixes `analytics.table_b`)\n' +
        '- `services/marketing/models/table_c_v2.sql` (renamed from `services/marketing/models/table_c.sql`)\n' +
        '- `services/core/models/orders.sql` (fixes `analytics.orders`)\n' +
        '- `services/core/models/shared.sql`\n',
    );
  });

  it('omits the sections it has nothing for', () => {
    const body = buildPullRequestBody({ nodeIds: ['a'], releaseId: 'r', files: [{ path: 'p' }] });
    expect(body).not.toContain('### Changes');
    expect(body).not.toContain('### Rationale');
    expect(body).toContain('**Nodes:** `a`');
  });

  it('links to the Remediation tab on the absolute continuo URL, never a path GitHub would resolve against github.com', () => {
    const body = buildPullRequestBody({
      nodeIds: ['a'],
      releaseId: 'r',
      files: [{ path: 'p' }],
      uiUrl: 'https://continuo.example.com',
    });
    expect(body).toContain('[View in continuo UI](https://continuo.example.com/?tab=remediation)');
    expect(body).not.toContain('](/');
  });

  it('omits the link when the UI has no public URL', () => {
    const body = buildPullRequestBody({ nodeIds: ['a'], releaseId: 'r', files: [{ path: 'p' }] });
    expect(body).not.toContain('View in continuo UI');
  });

  describe('owners', () => {
    const base = { nodeIds: ['a.x'], releaseId: 'r', files: [{ path: 'p' }] };

    it('lists the owners in the body', () => {
      const body = buildPullRequestBody({
        ...base,
        owners: [
          { nodeId: 'a.x', owner: 'acme/finance', review: 'requested' },
          { nodeId: 'a.y', owner: 'finance team', review: 'listed' },
          { nodeId: 'a.z', owner: 'bob', review: 'refused' },
        ],
      });
      expect(body).toContain(
        '### Owners\n- `a.x`: acme/finance (review requested)\n- `a.y`: finance team (listed only)\n- `a.z`: bob (review request refused)',
      );
    });

    it('says a node has no owner when it has none', () => {
      const body = buildPullRequestBody({
        ...base,
        owners: [{ nodeId: 'a.x', owner: '', review: 'listed' }],
      });
      expect(body).toContain('### Owners\n- `a.x`: no owner');
      expect(body).not.toContain('review requested');
      expect(body).not.toContain('review request refused');
      expect(body).not.toContain('listed only');
    });

    it('sets the owners after the files and before the diff', () => {
      const body = buildPullRequestBody({
        ...base,
        diff: '-a\n+b',
        rationale: 'why',
        owners: [{ nodeId: 'a.x', owner: 'alice', review: 'requested' }],
      });
      const files = body.indexOf('### Files changed');
      const owners = body.indexOf('### Owners');
      const changes = body.indexOf('### Changes');
      expect(owners).toBeGreaterThan(files);
      expect(changes).toBeGreaterThan(owners);
      expect(body.indexOf('### Rationale')).toBeGreaterThan(changes);
    });

    it('keeps each owner to one line', () => {
      const body = buildPullRequestBody({
        ...base,
        owners: [{ nodeId: 'a.x', owner: 'finance\n### Injected\t team', review: 'listed' }],
      });
      expect(body).toContain('- `a.x`: finance ### Injected team (listed only)');
      expect(body).not.toContain('\n### Injected');
    });

    it('leaves the section out when no owner was recorded', () => {
      expect(buildPullRequestBody(base)).not.toContain('### Owners');
      expect(buildPullRequestBody({ ...base, owners: [] })).not.toContain('### Owners');
    });
  });
});
