// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';
import VerificationDetailPage from './VerificationDetailPage';

const run = {
  run_id: 'verify-rel-1-core-a2', status: 'failed', changed_service: 'core', verifies_release_id: 'rel-1', attempt: 2,
  created_at: '2026-09-02T10:00:00Z', activated_at: '2026-09-02T10:01:00Z', finished_at: '2026-09-02T10:09:00Z',
  transitions: [{ to: 'received', at: '2026-09-02T10:00:00Z' }, { to: 'failed', at: '2026-09-02T10:09:00Z' }],
  validation_node_ids: ['model.core.orders'], failing_nodes: ['model.core.orders'],
  fail_reason: 'validation_failed', fail_detail: '',
  per_node_results: [{ stage: 'validation', node_id: 'model.core.orders', status: 'failed', dbt_log_uri: 's3://b/logs/x.log' }],
  image_tags: { core: 'img:1' }, manifest_kind: 'dbt',
};

function renderAt(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/verifications/${id}`]}>
      <Routes><Route path="/verifications/:id" element={<VerificationDetailPage />} /></Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  global.fetch = vi.fn((url: string) => {
    if (String(url) === '/api/verifications/verify-rel-1-core-a2') {
      return Promise.resolve({ ok: true, json: () => Promise.resolve(run) });
    }
    return Promise.resolve({ ok: false, status: 404, json: () => Promise.resolve({}) });
  }) as unknown as typeof fetch;
});

describe('VerificationDetailPage', () => {
  it('names the run, what it verifies, and its per-node results', async () => {
    renderAt('verify-rel-1-core-a2');
    expect(await screen.findByText('verify-rel-1-core-a2')).toBeInTheDocument();
    expect(document.querySelector('header .pill')?.textContent).toBe('failed');
    expect(screen.getByRole('link', { name: 'rel-1' })).toHaveAttribute('href', '/releases/rel-1');
    expect(screen.getByText(/attempt 2/)).toBeInTheDocument();
    expect(screen.getByText(/service core/)).toBeInTheDocument();
    // 'Validation' appears twice: the failure-reason strip (reasonLabel) and
    // the per-node results section header (stageLabel) for the same stage.
    expect(screen.getAllByText('Validation').length).toBeGreaterThan(0);
    expect(screen.getByText('model.core.orders')).toBeInTheDocument();
    expect(screen.getByText('view')).toBeInTheDocument(); // the log viewer
    expect(screen.queryByText('Fix')).toBeNull();          // no FIX column on a verification
  });

  it('shows the failure reason strip', async () => {
    renderAt('verify-rel-1-core-a2');
    await screen.findAllByText('Validation');
    expect(document.querySelector('.info-strip--error')?.textContent).toContain('Validation');
  });
});

describe('VerificationDetailPage — brand header', () => {
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

describe('VerificationDetailPage — services and timeline', () => {
  it('renders the run\'s images as service tiles and its transitions as a timeline', async () => {
    renderAt('verify-rel-1-core-a2');
    await screen.findByText('verify-rel-1-core-a2');
    const tile = document.querySelector('.service-tile--changed')!;
    expect(tile.querySelector('.service-tile__name')?.textContent).toBe('core');
    expect(tile.querySelector('.service-tile__tag')?.textContent).toBe('img:1');
    expect(document.querySelector('.section-header__sub')?.textContent).toBe('core is new in this verification run');
    const steps = Array.from(document.querySelectorAll('.release-timeline__step .pill-sm')).map(e => e.textContent);
    expect(steps).toEqual(['received', 'failed']);
    expect(document.body.textContent).not.toContain('Image tags:');
  });
});
