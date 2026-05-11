// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import RunSourcePickerDialog from '../../src/client/RunSourcePickerDialog';
import type { NodeRun } from '../../src/client/types';

const mkRun = (over: Partial<NodeRun>): NodeRun => ({
  run_id: 'r1', schedule_name: 'daily', kind: 'cron',
  terminal_status: 'succeeded', task_id: 't1',
  task_status: 'succeeded', retry_count: 0,
  image_tag: 'v1', manifest_version: 'm1',
  created_at: '2026-05-10T10:00:00Z',
  started_at: '2026-05-10T10:00:05Z',
  completed_at: '2026-05-10T10:01:00Z',
  error_message: null, log_s3_key: null,
  ...over,
});

describe('RunSourcePickerDialog', () => {
  it('lists only terminal task statuses', () => {
    const runs: NodeRun[] = [
      mkRun({ run_id: 'r1', task_status: 'succeeded' }),
      mkRun({ run_id: 'r2', task_status: 'failed' }),
      mkRun({ run_id: 'r3', task_status: 'running', completed_at: null }),
    ];
    render(<RunSourcePickerDialog runs={runs} onPick={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByRole('button', { name: /r1/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /r2/ })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /r3/ })).toBeNull();
  });

  it('calls onPick with the selected run_id', () => {
    const onPick = vi.fn();
    const runs: NodeRun[] = [mkRun({ run_id: 'pick-me' })];
    render(<RunSourcePickerDialog runs={runs} onPick={onPick} onClose={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: /pick-me/ }));
    expect(onPick).toHaveBeenCalledWith('pick-me');
  });

  it('calls onClose when the backdrop is clicked', () => {
    const onClose = vi.fn();
    render(<RunSourcePickerDialog runs={[]} onPick={vi.fn()} onClose={onClose} />);
    const dialogOverlay = document.querySelector('.dialog-overlay');
    expect(dialogOverlay).toBeTruthy();
    fireEvent.click(dialogOverlay!);
    expect(onClose).toHaveBeenCalled();
  });

  it('renders empty-state message when no eligible runs', () => {
    render(<RunSourcePickerDialog runs={[]} onPick={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByText(/no past runs available/i)).toBeInTheDocument();
  });
});
