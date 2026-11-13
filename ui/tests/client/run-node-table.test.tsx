// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import RunNodeTable from '../../src/client/RunNodeTable';
import { groupAttemptsByTask } from '../../src/client/detail-page-helpers';
import type { Task, TaskAttempt } from '../../src/client/types';

const task = (name: string, status: string, overrides: Partial<Task> = {}): Task => ({
  task_id: name, service_name: 'core', schema_name: 'a', table_name: name,
  job_name: name, status, attempt: 0, max_retries: 2, created_at: null,
  ...overrides,
});
const tasks = [task('ok_node', 'succeeded'), task('bad_node', 'failed')];
const noAttempts = new Map<string, TaskAttempt[]>();

function attempt(task_id: string, n: number, overrides: Partial<TaskAttempt> = {}): TaskAttempt {
  return {
    task_id, attempt: n, job_name: `${task_id}-${n}`, status: 'failed',
    failure_class: null, failure_reason: null,
    dispatched_at: null, queued_at: null, started_at: null, finished_at: null,
    execution_seconds: null, error_message: null, log_s3_key: null, run_results_uri: null,
    ...overrides,
  };
}

describe('RunNodeTable', () => {
  it('filters to failed nodes only', () => {
    render(
      <RunNodeTable tasks={tasks} attemptsByTask={noAttempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    expect(screen.getByText('ok_node')).toBeInTheDocument();
    // Anchored regex: a service group header is also `role="button"` and its
    // accessible name can contain the word "failed" (its rolled-up status
    // pill) — anchoring to the start matches only the filter button's own
    // label ("Failed 1"), not the group header.
    fireEvent.click(screen.getByRole('button', { name: /^failed/i }));
    expect(screen.queryByText('ok_node')).not.toBeInTheDocument();
    expect(screen.getByText('bad_node')).toBeInTheDocument();
  });

  // The brief's illustrative selector (`th, .th, [class*="head"]`) doesn't match
  // NodesPanel's actual markup: header cells are plain `<th>` text nodes with no
  // extra class. The intent — every status column survives the extraction/wrap
  // into RunNodeTable — is what this asserts, against the real <th> elements.
  it('keeps every status column header', () => {
    render(
      <RunNodeTable tasks={tasks} attemptsByTask={noAttempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    for (const col of ['Status', 'Attempt', 'Error', 'Started', 'Completed', 'Logs']) {
      expect(screen.getByRole('columnheader', { name: col })).toBeInTheDocument();
    }
  });

  it('orders a group\'s rows by dependency depth via intraServiceOrder', () => {
    const depTasks = [task('downstream', 'succeeded'), task('upstream', 'succeeded')];
    const edges = [{ from_node_id: 'core.a.upstream', to_node_id: 'core.a.downstream' }];
    const { container } = render(
      <RunNodeTable tasks={depTasks} attemptsByTask={noAttempts} edges={edges}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    const names = [...container.querySelectorAll('.nodes-node-name')].map((el) => el.textContent);
    expect(names).toEqual(['upstream', 'downstream']);
  });

  it('Failed badge counts failed AND skipped, matching what the filter reveals', () => {
    const mixed = [task('ok_node', 'succeeded'), task('bad_node', 'failed'), task('skip_node', 'skipped')];
    render(
      <RunNodeTable tasks={mixed} attemptsByTask={noAttempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    // Badge reads failed(1) + skipped(1) = 2, matching the rows the click shows.
    const failedBtn = screen.getByRole('button', { name: /^failed 2$/i });
    fireEvent.click(failedBtn);
    expect(screen.getByText('bad_node')).toBeInTheDocument();
    expect(screen.getByText('skip_node')).toBeInTheDocument();
    expect(screen.queryByText('ok_node')).not.toBeInTheDocument();
  });

  it('auto-expands and shows a matching group when filtering by text search', () => {
    render(
      <RunNodeTable tasks={tasks} attemptsByTask={noAttempts} edges={[]}
        expandedServices={new Set()} onServiceToggle={vi.fn()} />,
    );
    // Collapsed by default (expandedServices is empty): rows are hidden.
    expect(screen.queryByText('bad_node')).not.toBeInTheDocument();
    fireEvent.change(screen.getByPlaceholderText(/filter nodes/i), { target: { value: 'bad' } });
    expect(screen.getByText('bad_node')).toBeInTheDocument();
    expect(screen.queryByText('ok_node')).not.toBeInTheDocument();
  });

  it('reads the Attempt column as attempt+1 / max_retries+1', () => {
    const { container } = render(
      <RunNodeTable tasks={[task('retried', 'running', { attempt: 1, max_retries: 2 })]}
        attemptsByTask={noAttempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    expect(container.querySelector('.nodes-attempt')?.textContent).toBe('2 / 3');
  });

  it('reads the Attempt column as the attempt number alone when max_retries is 0', () => {
    // max_retries 0 is a task with no stored retry budget, or the default
    // one; "1 / 1" would claim it has a single allowed attempt.
    const { container } = render(
      <RunNodeTable tasks={[task('unbudgeted', 'running', { attempt: 0, max_retries: 0 })]}
        attemptsByTask={noAttempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    expect(container.querySelector('.nodes-attempt')?.textContent).toBe('1');
  });

  it('expands a task row into every attempt with its status, class · reason, wait, run time and log', () => {
    const attempts = groupAttemptsByTask([
      attempt('bad_node', 0, {
        status: 'failed', failure_class: 'infra', failure_reason: 'oom',
        queued_at: '2026-01-01T10:00:00Z', started_at: '2026-01-01T10:00:05Z',
        finished_at: '2026-01-01T10:00:35Z', execution_seconds: 30,
        error_message: 'first failure', log_s3_key: 'logs/bad_node-0.log',
      }),
      attempt('bad_node', 1, {
        status: 'failed', failure_class: 'user_code', failure_reason: 'compile_error',
        queued_at: '2026-01-01T10:01:00Z', started_at: '2026-01-01T10:02:05Z',
        finished_at: '2026-01-01T10:03:00Z', execution_seconds: 55,
        error_message: 'second failure', log_s3_key: 'logs/bad_node-1.log',
      }),
    ]);
    const { container } = render(
      <RunNodeTable tasks={[task('bad_node', 'failed', { attempt: 1 })]} attemptsByTask={attempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );

    // The task row reads its Error and Logs cells from the highest-numbered attempt.
    expect(screen.getAllByText('second failure').length).toBeGreaterThan(0);
    expect(screen.queryByText('first failure')).toBeNull();
    expect(screen.getByRole('link', { name: 'logs' }).getAttribute('href'))
      .toContain(encodeURIComponent('logs/bad_node-1.log'));

    // Collapsed by default: no attempts block until the row is opened.
    const row = screen.getByText('bad_node').closest('tr')!;
    expect(row.getAttribute('aria-expanded')).toBe('false');
    expect(container.querySelector('.remediation-attempts')).toBeNull();

    fireEvent.click(row);
    expect(row.getAttribute('aria-expanded')).toBe('true');
    const block = container.querySelector('.remediation-attempts') as HTMLElement;
    expect(block).toBeTruthy();
    expect(block.closest('td')?.getAttribute('colspan')).toBe('7');
    const headers = within(block).getAllByRole('columnheader').map((th) => th.textContent);
    expect(headers).toEqual(['#', 'Status', 'Class · reason', 'Wait', 'Run time', 'Log']);

    const rows = within(block).getAllByRole('row').slice(1);
    expect(rows).toHaveLength(2);
    const cells = rows.map((r) => Array.from(r.querySelectorAll('td'), (td) => td.textContent));
    expect(cells[0]).toEqual(['1', 'failed', 'infra · oom', '5s', '30s', 'open log ↗']);
    expect(cells[1]).toEqual(['2', 'failed', 'user_code · compile_error', '1m 5s', '55s', 'open log ↗']);
    expect(rows[1].querySelector('.pill-sm')?.className).toContain('pill-sm--failed');
    expect(within(rows[1]).getByRole('link', { name: 'open log ↗' }).getAttribute('href'))
      .toContain(encodeURIComponent('logs/bad_node-1.log'));

    // Enter on the row closes it again (Grouped table rows keyboard contract).
    fireEvent.keyDown(row, { key: 'Enter' });
    expect(container.querySelector('.remediation-attempts')).toBeNull();
  });

  it('a task without attempts has no expander', () => {
    const attempts = groupAttemptsByTask([attempt('bad_node', 0, { status: 'dispatched' })]);
    const { container } = render(
      <RunNodeTable tasks={tasks} attemptsByTask={attempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    const okRow = screen.getByText('ok_node').closest('tr')!;
    expect(okRow.getAttribute('aria-expanded')).toBeNull();
    expect(okRow.getAttribute('role')).toBeNull();
    expect(okRow.querySelector('.remediation-chevron')).toBeNull();
    fireEvent.click(okRow);
    expect(container.querySelector('.remediation-attempts')).toBeNull();

    const badRow = screen.getByText('bad_node').closest('tr')!;
    expect(badRow.querySelector('.remediation-chevron')).toBeTruthy();
    fireEvent.click(badRow);
    const block = container.querySelector('.remediation-attempts') as HTMLElement;
    const cells = Array.from(within(block).getAllByRole('row')[1].querySelectorAll('td'), (td) => td.textContent);
    // A dispatched attempt has no class, no wait, no run time and no log yet.
    expect(cells).toEqual(['1', 'dispatched', '—', '—', '—', '—']);
    expect(within(block).getAllByRole('row')[1].querySelector('.pill-sm')?.className)
      .toContain('pill-sm--pending');
  });

  it('shows a task operation and its reason', () => {
    render(
      <RunNodeTable
        tasks={[
          task('seed_table_1', 'succeeded', { operation: 'full_refresh', plan_reason: 'columns changed' }),
          task('seed_table_1_view', 'succeeded', {
            operation: 'run', plan_reason: 'rebuild: dropped by the load of e2e_schema.seed_table_1',
          }),
          task('plain', 'succeeded', { operation: 'run', plan_reason: null }),
        ]}
        attemptsByTask={noAttempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    expect(screen.getAllByText('Full refresh')).toHaveLength(1);
    expect(screen.getByText('Full refresh').className).toContain('pill-sm--muted');
    expect(screen.getByText('columns changed').className).toContain('task-reason');
    expect(screen.getByText('rebuild: dropped by the load of e2e_schema.seed_table_1')).toBeInTheDocument();
    expect(screen.getByText('plain').closest('td')!.querySelector('.task-reason')).toBeNull();
  });

  it('lists the findings of a failed seed preflight in the expanded attempt', () => {
    const attempts = groupAttemptsByTask([attempt('seed_table_1', 0, {
      failure_class: 'invalid', failure_reason: 'unowned_dependents',
      seed_preflight: {
        drop_semantics: 'cascade', completeness: 'complete', checked: true,
        dependents: [
          { schema: 'bi', name: 'fx_rates_report', kind: 'view', owner: 'bi', depth: 1, allowed: false },
          { schema: 'e2e_schema', name: 'seed_table_1_view', kind: 'view', owner: 'core', depth: 1, allowed: true },
        ],
        probes: [
          { schema: 'bi', name: 'broken_view', ok: false, error: 'relation does not exist' },
          { schema: 'bi', name: 'fine_view', ok: true, error: '' },
        ],
      },
    })]);
    render(
      <RunNodeTable tasks={[task('seed_table_1', 'failed')]} attemptsByTask={attempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    expect(screen.queryByText('bi.fx_rates_report (view, owner bi)')).toBeNull();
    fireEvent.click(screen.getByText('seed_table_1').closest('tr')!);
    expect(screen.getByText('bi.fx_rates_report (view, owner bi)')).toBeInTheDocument();
    expect(screen.getByText('bi.broken_view: relation does not exist')).toBeInTheDocument();
    expect(screen.queryByText(/seed_table_1_view/)).toBeNull();
    expect(screen.queryByText(/fine_view/)).toBeNull();
  });

  it('adds no findings row when the preflight found nothing outside the allowed set', () => {
    const attempts = groupAttemptsByTask([attempt('seed_table_1', 0, {
      status: 'succeeded',
      seed_preflight: {
        drop_semantics: 'cascade', completeness: 'complete', checked: true,
        dependents: [{ schema: 'e2e_schema', name: 'v', kind: 'view', owner: 'core', depth: 1, allowed: true }],
        probes: [],
      },
    })]);
    const { container } = render(
      <RunNodeTable tasks={[task('seed_table_1', 'succeeded')]} attemptsByTask={attempts} edges={[]}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    fireEvent.click(screen.getByText('seed_table_1').closest('tr')!);
    expect(container.querySelector('.remediation-verif')).toBeNull();
  });
});
