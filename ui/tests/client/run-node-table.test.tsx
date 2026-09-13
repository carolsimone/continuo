// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import RunNodeTable from '../../src/client/RunNodeTable';
import type { Task, TaskExecution } from '../../src/client/types';

const task = (name: string, status: string): Task => ({
  task_id: name, service_name: 'core', schema_name: 'a', table_name: name,
  job_name: name, status, retry_count: 0, max_retries: 2, created_at: null,
});
const tasks = [task('ok_node', 'succeeded'), task('bad_node', 'failed')];
const execs: TaskExecution[] = [];

describe('RunNodeTable', () => {
  it('filters to failed nodes only', () => {
    render(
      <RunNodeTable tasks={tasks} executions={execs} edges={[]}
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
      <RunNodeTable tasks={tasks} executions={execs} edges={[]}
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
      <RunNodeTable tasks={depTasks} executions={execs} edges={edges}
        expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    const names = [...container.querySelectorAll('.nodes-node-name')].map((el) => el.textContent);
    expect(names).toEqual(['upstream', 'downstream']);
  });

  it('Failed badge counts failed AND skipped, matching what the filter reveals', () => {
    const mixed = [task('ok_node', 'succeeded'), task('bad_node', 'failed'), task('skip_node', 'skipped')];
    render(
      <RunNodeTable tasks={mixed} executions={execs} edges={[]}
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
      <RunNodeTable tasks={tasks} executions={execs} edges={[]}
        expandedServices={new Set()} onServiceToggle={vi.fn()} />,
    );
    // Collapsed by default (expandedServices is empty): rows are hidden.
    expect(screen.queryByText('bad_node')).not.toBeInTheDocument();
    fireEvent.change(screen.getByPlaceholderText(/filter nodes/i), { target: { value: 'bad' } });
    expect(screen.getByText('bad_node')).toBeInTheDocument();
    expect(screen.queryByText('ok_node')).not.toBeInTheDocument();
  });
});
