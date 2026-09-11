// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import RunSourcePickerDialog from '../../src/client/RunSourcePickerDialog';
import type { NodeRun } from '../../src/client/types';

const mkRun = (over: Partial<NodeRun>): NodeRun => ({
  run_id: 'r1', schedule_name: 'daily', kind: 'cron',
  terminal_status: 'succeeded', task_id: 't1',
  task_status: 'succeeded', retry_count: 0,
  image_tag: 'v1', manifest_version: 'm1', operation: 'run',
  created_at: '2026-05-10T10:00:00Z',
  started_at: '2026-05-10T10:00:05Z',
  completed_at: '2026-05-10T10:01:00Z',
  error_message: null, log_s3_key: null, run_results_uri: null,
  ...over,
});

describe('RunSourcePickerDialog', () => {
  it('collapses runs sharing a snapshot into one selectable row', () => {
    const runs: NodeRun[] = [
      mkRun({ run_id: 'a1', image_tag: 'img-a', manifest_version: 'm1', created_at: '2026-05-10T10:00:00Z' }),
      mkRun({ run_id: 'a2', image_tag: 'img-a', manifest_version: 'm1', created_at: '2026-05-09T10:00:00Z' }),
      mkRun({ run_id: 'b1', image_tag: 'img-b', manifest_version: 'm1', created_at: '2026-05-08T10:00:00Z' }),
    ];
    render(<RunSourcePickerDialog runs={runs} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    expect(document.querySelectorAll('.pick-row')).toHaveLength(2);
    expect(screen.getByText('img-a')).toBeInTheDocument();
    expect(screen.getByText('img-b')).toBeInTheDocument();
  });

  it('excludes an in-flight source run whose scheduler is not terminal', () => {
    const runs: NodeRun[] = [
      mkRun({ run_id: 'done', terminal_status: 'succeeded', image_tag: 'img-done' }),
      mkRun({ run_id: 'inflight', terminal_status: '', image_tag: 'img-inflight' }),
    ];
    render(<RunSourcePickerDialog runs={runs} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByText('img-done')).toBeInTheDocument();
    expect(screen.queryByText('img-inflight')).toBeNull();
  });

  it('picks the snapshot\'s most-recent terminal run', () => {
    const onPick = vi.fn();
    const runs: NodeRun[] = [
      mkRun({ run_id: 'older', image_tag: 'img', manifest_version: 'm', created_at: '2026-05-09T10:00:00Z' }),
      mkRun({ run_id: 'newest', image_tag: 'img', manifest_version: 'm', created_at: '2026-05-10T10:00:00Z' }),
    ];
    render(<RunSourcePickerDialog runs={runs} operation="run" onPick={onPick} onClose={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: /img/ }));
    expect(onPick).toHaveBeenCalledWith('newest');
  });

  it('shows the representative run\'s status as a pill', () => {
    const runs: NodeRun[] = [
      mkRun({ run_id: 'failed-latest', image_tag: 'img', task_status: 'failed',
              created_at: '2026-05-10T10:00:00Z' }),
    ];
    render(<RunSourcePickerDialog runs={runs} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    expect(document.querySelector('.pick-row .pill-sm--failed')).toBeTruthy();
  });

  it('shows manifest_version in the row meta when present', () => {
    const runs: NodeRun[] = [mkRun({ image_tag: 'img', manifest_version: 'mani-42' })];
    render(<RunSourcePickerDialog runs={runs} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByText(/mani-42/)).toBeInTheDocument();
  });

  it('distinguishes same-image snapshots by manifest in the accessible name', () => {
    // Same image tag, same run count, same status — only the manifest differs.
    // The manifest must reach the accessible name or the two are announced alike.
    const runs: NodeRun[] = [
      mkRun({ run_id: 'r14', image_tag: 'img', manifest_version: 'm14', created_at: '2026-05-10T10:00:00Z' }),
      mkRun({ run_id: 'r13', image_tag: 'img', manifest_version: 'm13', created_at: '2026-05-09T10:00:00Z' }),
    ];
    render(<RunSourcePickerDialog runs={runs} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByRole('button', { name: /img snapshot, manifest m14/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /img snapshot, manifest m13/i })).toBeInTheDocument();
  });

  it('scrolls the list inside a bounded region rather than growing the dialog', () => {
    render(<RunSourcePickerDialog runs={[mkRun({})]} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    expect(document.querySelector('.pick-list')).toBeTruthy();
  });

  it('calls onClose when the backdrop is clicked', () => {
    const onClose = vi.fn();
    render(<RunSourcePickerDialog runs={[]} operation="run" onPick={vi.fn()} onClose={onClose} />);
    const dialogOverlay = document.querySelector('.dialog-overlay');
    expect(dialogOverlay).toBeTruthy();
    fireEvent.click(dialogOverlay!);
    expect(onClose).toHaveBeenCalled();
  });

  it('renders a neutral info-strip empty state when no eligible runs', () => {
    render(<RunSourcePickerDialog runs={[]} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    const empty = screen.getByText(/no past snapshots available/i);
    expect(empty).toBeInTheDocument();
    expect(empty.closest('.info-strip--neutral')).toBeTruthy();
    expect(document.querySelectorAll('.pick-row')).toHaveLength(0);
  });

  it('reflects the operation verb in the dialog copy', () => {
    render(<RunSourcePickerDialog runs={[]} operation="test" onPick={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByText(/test this node/i)).toBeInTheDocument();
  });
});

describe('RunSourcePickerDialog — buttons use .btn foundation', () => {
  it('Cancel button uses .btn.btn--secondary, no legacy class', () => {
    render(<RunSourcePickerDialog runs={[]} operation="run" onPick={vi.fn()} onClose={vi.fn()} />);
    const cancel = screen.getByRole('button', { name: /cancel/i });
    expect(cancel.className).toMatch(/\bbtn\b/);
    expect(cancel.className).toMatch(/\bbtn--secondary\b/);
    expect(cancel.className).not.toMatch(/\bdialog-btn\b/);
  });
});
