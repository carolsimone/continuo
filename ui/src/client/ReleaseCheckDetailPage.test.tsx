// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';
import ReleaseCheckDetailPage from './ReleaseCheckDetailPage';

const check = {
  check_id: 'verify-rel-1-core-a2', status: 'failed', changed_project: 'core', verifies_release_id: 'rel-1', attempt: 2,
  created_at: '2026-09-02T10:00:00Z', activated_at: '2026-09-02T10:01:00Z', finished_at: '2026-09-02T10:09:00Z',
  transitions: [{ to: 'received', at: '2026-09-02T10:00:00Z' }, { to: 'failed', at: '2026-09-02T10:09:00Z' }],
  validation_node_ids: ['model.core.orders'], failing_nodes: ['model.core.orders'],
  fail_reason: 'validation_failed', fail_detail: '',
  per_node_results: [{ stage: 'validation', node_id: 'model.core.orders', status: 'failed', dbt_log_uri: 's3://b/logs/x.log' }],
  image_tags: { core: 'img:1', finance: 'img:2' }, manifest_kind: 'dbt',
};

function renderAt(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/release-checks/${id}`]}>
      <Routes><Route path="/release-checks/:id" element={<ReleaseCheckDetailPage />} /></Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  global.fetch = vi.fn((url: string) => {
    if (String(url) === '/api/release-checks/verify-rel-1-core-a2') {
      return Promise.resolve({ ok: true, json: () => Promise.resolve(check) });
    }
    return Promise.resolve({ ok: false, status: 404, json: () => Promise.resolve({}) });
  }) as unknown as typeof fetch;
});

describe('ReleaseCheckDetailPage', () => {
  it('names the release check, what it verifies, and its per-node results', async () => {
    renderAt('verify-rel-1-core-a2');
    expect(await screen.findByText('verify-rel-1-core-a2')).toBeInTheDocument();
    expect(document.querySelector('header .pill')?.textContent).toBe('failed');
    expect(screen.getByRole('link', { name: 'rel-1' })).toHaveAttribute('href', '/releases/rel-1');
    expect(screen.getByText(/attempt 2/)).toBeInTheDocument();
    expect(screen.getByText(/project core/)).toBeInTheDocument();
    // 'Validation' appears twice: the failure-reason strip (reasonLabel) and
    // the per-node results section header (stageLabel) for the same stage.
    expect(screen.getAllByText('Validation').length).toBeGreaterThan(0);
    expect(screen.getByText('model.core.orders')).toBeInTheDocument();
    expect(screen.getByText('view')).toBeInTheDocument(); // the log viewer
    expect(screen.queryByText('Fix')).toBeNull();          // no FIX column on a fix's release check
  });

  it('shows the failure reason strip', async () => {
    renderAt('verify-rel-1-core-a2');
    await screen.findAllByText('Validation');
    expect(document.querySelector('.info-strip--error')?.textContent).toContain('Validation');
  });
});

describe('ReleaseCheckDetailPage — parse timings', () => {
  const parsed = {
    ...check,
    transitions: [
      { to: 'received', at: '2026-09-02T10:00:00Z' },
      { to: 'parsing', at: '2026-09-02T10:01:00Z' },
      { to: 'failed', at: '2026-09-02T10:09:00Z' },
    ],
    parse_ms: 4180, contracts_ms: 1260, contracts_by_family: { dbt: 3, python: 2 },
  };

  function serve(body: object) {
    global.fetch = vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(body) })) as unknown as typeof fetch;
  }

  const lines = () => Array.from(document.querySelectorAll('.release-timeline__line')).map(el => el.textContent);

  it('shows how long the parse and the contract generation took, as a release page does', async () => {
    serve(parsed);
    renderAt('verify-rel-1-core-a2');
    await screen.findByText('verify-rel-1-core-a2');
    expect(lines()).toEqual(['parsing 4.2 s', 'generating contracts 1.3 s · 5\u00a0contracts (dbt 3, python 2)']);
  });

  it('shows no line for a release check whose parse reported nothing', async () => {
    serve({ ...parsed, parse_ms: undefined, contracts_ms: undefined, contracts_by_family: undefined });
    renderAt('verify-rel-1-core-a2');
    await screen.findByText('verify-rel-1-core-a2');
    expect(lines()).toEqual([]);
  });
});

describe('ReleaseCheckDetailPage — brand header', () => {
  it('starts the header with the brand, before the back link', async () => {
    renderAt('verify-rel-1-core-a2');
    await screen.findByText('verify-rel-1-core-a2');

    const header = document.querySelector('.page-header')!;
    const brand = header.querySelector('a.brand');
    expect(brand).toHaveAttribute('href', '/');
    const back = header.querySelector('.detail-back-link')!;
    expect(brand!.compareDocumentPosition(back) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});

describe('ReleaseCheckDetailPage — projects and timeline', () => {
  it('renders the release check\'s images as project tiles and its transitions as a timeline', async () => {
    renderAt('verify-rel-1-core-a2');
    await screen.findByText('verify-rel-1-core-a2');
    const tile = document.querySelector('.project-tile--changed')!;
    expect(tile.querySelector('.project-tile__name')?.textContent).toBe('core');
    expect(tile.querySelector('.project-tile__tag')?.textContent).toBe('img:1');
    expect(document.querySelector('.section-header__sub')?.textContent).toBe('core is new in this release check · 1 carried over from rel-1');
    const steps = Array.from(document.querySelectorAll('.release-timeline__step .pill-sm')).map(e => e.textContent);
    expect(steps).toEqual(['received', 'failed']);
    expect(document.body.textContent).not.toContain('Image tags:');
  });
});
