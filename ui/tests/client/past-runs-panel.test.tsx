// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, cleanup } from '@testing-library/react';
import PastRunsPanel from '../../src/client/PastRunsPanel';
import { RunSummary } from '../../src/client/types';

afterEach(cleanup);

function runs(n: number): RunSummary[] {
  return Array.from({ length: n }, (_, i) => ({
    run_id: `run-${i}`,
    schedule_name: 'daily',
    terminal_status: 'succeeded',
    created_at: '2026-10-14T23:00:00Z',
    completed_at: '2026-10-14T23:03:00Z',
    failure_reason: null,
  }));
}

function renderPanel(props: { page: number; total: number; onPageChange?: (p: number) => void }) {
  const onPageChange = props.onPageChange ?? vi.fn();
  render(
    <PastRunsPanel
      runs={runs(10)}
      liveRunId={null}
      liveStatus={null}
      selectedRunId={null}
      onSelectRun={vi.fn()}
      page={props.page}
      pageSize={10}
      total={props.total}
      onPageChange={onPageChange}
    />,
  );
  return onPageChange;
}

describe('PastRunsPanel pager', () => {
  it('is hidden when every run fits on one page', () => {
    renderPanel({ page: 0, total: 10 });

    expect(screen.queryByRole('button', { name: 'Older' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Newer' })).toBeNull();
  });

  it('is hidden when the parent passes no paging props', () => {
    render(
      <PastRunsPanel runs={runs(3)} liveRunId={null} liveStatus={null}
                     selectedRunId={null} onSelectRun={vi.fn()} />,
    );

    expect(screen.queryByRole('button', { name: 'Older' })).toBeNull();
  });

  it('shows the range and disables Newer on the first page', () => {
    renderPanel({ page: 0, total: 50 });

    expect(screen.getByText('1–10 of 50')).toBeTruthy();
    expect((screen.getByRole('button', { name: 'Newer' }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole('button', { name: 'Older' }) as HTMLButtonElement).disabled).toBe(false);
  });

  it('clamps the range end to the total and disables Older on the last page', () => {
    renderPanel({ page: 4, total: 45 });

    expect(screen.getByText('41–45 of 45')).toBeTruthy();
    expect((screen.getByRole('button', { name: 'Older' }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole('button', { name: 'Newer' }) as HTMLButtonElement).disabled).toBe(false);
  });

  it('asks the parent for the adjacent page', () => {
    const onPageChange = renderPanel({ page: 2, total: 50 });

    fireEvent.click(screen.getByRole('button', { name: 'Older' }));
    fireEvent.click(screen.getByRole('button', { name: 'Newer' }));

    expect(onPageChange).toHaveBeenNthCalledWith(1, 3);
    expect(onPageChange).toHaveBeenNthCalledWith(2, 1);
  });
});
