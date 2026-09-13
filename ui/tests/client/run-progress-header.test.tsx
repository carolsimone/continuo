// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import RunProgressHeader from '../../src/client/RunProgressHeader';
import type { Task } from '../../src/client/types';

const t = (status: string): Task => ({
  task_id: Math.random().toString(), service_name: 'core', schema_name: 'a', table_name: 'x',
  job_name: 'j', status, retry_count: 0, max_retries: 2, created_at: null,
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

  it('counts cancelled separately and toward completion, not as pending', () => {
    render(<RunProgressHeader tasks={[t('succeeded'), t('cancelled')]} />);
    expect(screen.getByTestId('count-cancelled')).toHaveTextContent('1');
    expect(screen.getByTestId('count-pending')).toHaveTextContent('0');
    // a cancelled task is terminal: done = succeeded + cancelled = 2 of 2 = 100%
    expect(screen.getByTestId('run-progress-pct')).toHaveTextContent('100%');
  });
});
