// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import RunProgressHeader from '../../src/client/RunProgressHeader';
import type { Task } from '../../src/client/types';

const t = (status: string): Task => ({
  task_id: Math.random().toString(), service_name: 'core', schema_name: 'a', table_name: 'x',
  job_name: 'j', status, attempt: 0, max_retries: 2, created_at: null,
});

describe('RunProgressHeader', () => {
  it('counts each status bucket and computes percent complete', () => {
    render(<RunProgressHeader tasks={[t('succeeded'), t('succeeded'), t('running'), t('failed'), t('pending')]} />);
    expect(screen.getByTestId('count-succeeded')).toHaveTextContent('2');
    expect(screen.getByTestId('count-running')).toHaveTextContent('1');
    expect(screen.getByTestId('count-failed')).toHaveTextContent('1');
    expect(screen.getByTestId('count-pending')).toHaveTextContent('1');
    // done = succeeded+failed = 3 of 5 = 60%
    expect(screen.getByTestId('run-progress-pct')).toHaveTextContent('60%');
  });

  it('counts dispatched and queued tasks on their own, before running, and not toward completion', () => {
    const { container } = render(<RunProgressHeader tasks={[t('dispatched'), t('queued'), t('queued'), t('running'), t('succeeded')]} />);
    expect(screen.getByTestId('count-dispatched')).toHaveTextContent('1');
    expect(screen.getByTestId('count-queued')).toHaveTextContent('2');
    expect(screen.getByTestId('count-pending')).toHaveTextContent('0');
    expect(screen.getByTestId('run-progress-pct')).toHaveTextContent('20%');
    const order = [...container.querySelectorAll('.rp-count b')].map((b) => b.getAttribute('data-testid'));
    expect(order.indexOf('count-queued')).toBe(order.indexOf('count-stopping') - 1);
    expect(order.indexOf('count-dispatched')).toBe(order.indexOf('count-queued') - 1);
  });

  it('counts a stopping task in its own bucket, just before running, and not toward completion', () => {
    const { container } = render(<RunProgressHeader tasks={[t('stopping'), t('stopping'), t('running'), t('cancelled'), t('succeeded')]} />);
    expect(screen.getByTestId('count-stopping')).toHaveTextContent('2');
    expect(screen.getByTestId('count-pending')).toHaveTextContent('0');
    // done = cancelled + succeeded = 2 of 5 = 40%
    expect(screen.getByTestId('run-progress-pct')).toHaveTextContent('40%');
    const order = [...container.querySelectorAll('.rp-count b')].map((b) => b.getAttribute('data-testid'));
    expect(order.indexOf('count-stopping')).toBe(order.indexOf('count-running') - 1);
    expect(container.querySelectorAll('.rp-stopping').length).toBeGreaterThan(0);
  });

  it('counts cancelled separately and toward completion, not as pending', () => {
    render(<RunProgressHeader tasks={[t('succeeded'), t('cancelled')]} />);
    expect(screen.getByTestId('count-cancelled')).toHaveTextContent('1');
    expect(screen.getByTestId('count-pending')).toHaveTextContent('0');
    // a cancelled task is terminal: done = succeeded + cancelled = 2 of 2 = 100%
    expect(screen.getByTestId('run-progress-pct')).toHaveTextContent('100%');
  });

  it('draws no bar segment for a status with no tasks, yet still counts it', () => {
    const { container } = render(<RunProgressHeader tasks={[t('succeeded'), t('running')]} />);
    expect(container.querySelector('.rp-seg.rp-failed')).toBeNull();
    expect(container.querySelector('.rp-seg.rp-succeeded')).not.toBeNull();
    expect(screen.getByTestId('count-failed')).toHaveTextContent('0');
  });

  it('groups the thousands of a large run in its total too', () => {
    const tasks = Array.from({ length: 2000 }, () => t('succeeded'));
    render(<RunProgressHeader tasks={tasks} />);
    expect(screen.getByText('2,000 of 2,000 nodes complete')).toBeInTheDocument();
  });

  it('says node for a run of one task, as the Runs card does', () => {
    render(<RunProgressHeader tasks={[t('running')]} />);
    expect(screen.getByText('0 of 1 node complete')).toBeInTheDocument();
  });

  it('marks the failed count when a task failed', () => {
    const { container } = render(<RunProgressHeader tasks={[t('failed'), t('running')]} />);
    expect(container.querySelector('.rp-count--alert')).toHaveTextContent('1 failed');
  });

  it('reads the same percentage as the Runs card for a share that rounds on a floating-point edge', () => {
    const tasks = [...Array.from({ length: 23 }, () => t('succeeded')), ...Array.from({ length: 17 }, () => t('pending'))];
    render(<RunProgressHeader tasks={tasks} />);
    expect(screen.getByTestId('run-progress-pct')).toHaveTextContent('58%');
  });

  it('links a run to its release', () => {
    render(<MemoryRouter><RunProgressHeader tasks={[]} releaseId="rel-a1b2c3d-201" /></MemoryRouter>);
    expect(screen.getByRole('link', { name: 'rel-a1b2c3d-201' })).toHaveAttribute('href', '/releases/rel-a1b2c3d-201');
  });

  it('shows no release link when the run has no release', () => {
    render(<MemoryRouter><RunProgressHeader tasks={[]} releaseId={null} /></MemoryRouter>);
    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.queryByText(/Release/)).toBeNull();
  });
});
