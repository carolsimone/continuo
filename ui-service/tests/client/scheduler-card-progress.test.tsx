// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, cleanup } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import SchedulerCard from '../../src/client/SchedulerCard';
import { ScheduleSummary, Task } from '../../src/client/types';

function baseSchedule(overrides: Partial<ScheduleSummary> = {}): ScheduleSummary {
  return {
    schedule_name: 'daily',
    cron_expression: '0 0 * * *',
    description: '',
    timezone: 'UTC',
    is_running: false,
    last_run_at: null,
    last_run_status: 'SUCCEEDED',
    last_run_id: 'run-1',
    ...overrides,
  };
}

// 412 tasks, all succeeded, served in three pages of 200/200/12 — mirrors a
// real run whose task count exceeds the server's page size.
function taskPage(count: number): Task[] {
  return Array.from({ length: count }, (_, i) => ({
    task_id: `t-${i}`,
    service_name: 'svc',
    schema_name: 'schema',
    table_name: 'table',
    job_name: 'job',
    status: 'succeeded',
    retry_count: 0,
    max_retries: 0,
    created_at: null,
  }));
}

// Per-test fetch mock keyed by URL. `/tasks` paginates via ?offset= against a
// fixed 412-row backing set; any other URL (the topology-drift `/graph` poll)
// resolves to an empty body so it doesn't blow up.
function installFetch() {
  const handler = vi.fn((input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input.toString();
    if (url.includes('/tasks')) {
      const offset = Number(new URL(url, 'http://x').searchParams.get('offset') ?? 0);
      const remaining = Math.max(0, 412 - offset);
      return Promise.resolve({
        ok: true,
        json: () =>
          Promise.resolve({ total_count: 412, tasks: taskPage(Math.min(200, remaining)) }),
      } as Response);
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) } as Response);
  });
  vi.stubGlobal('fetch', handler);
  return handler;
}

beforeEach(() => { installFetch(); });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

describe('SchedulerCard progress', () => {
  it('reports the complete task total across pages, not just the first page', async () => {
    render(
      <MemoryRouter>
        <SchedulerCard schedule={baseSchedule()} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText('Completed: 412/412')).toBeInTheDocument();
    });
  });
});
