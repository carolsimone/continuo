// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { NodeResultsTable } from './node-results';
import type { NodeValidationResult } from './types';

const node = (over: Partial<NodeValidationResult> = {}): NodeValidationResult => ({
  node_id: 'model.core.orders', status: 'failed', stage: 'validation', ...over,
});

describe('NodeResultsTable', () => {
  it('shows an empty state and no table when there are no results', () => {
    render(<NodeResultsTable perNode={[]} />);
    expect(screen.getByText('No per-node results.')).toBeInTheDocument();
    expect(screen.queryByRole('table')).toBeNull();
  });

  it('groups by stage and renders node/status/duration/log columns without a Fix column when fixCell is omitted', () => {
    render(<NodeResultsTable perNode={[
      node({ stage: 'compile', node_id: 'core', status: 'failed' }),
      node({ stage: 'validation', node_id: 'model.core.orders', status: 'ok', duration_ms: 1234, dbt_log_uri: 's3://b/x.log' }),
    ]} />);
    expect(screen.getByText('Compilation')).toBeInTheDocument();
    expect(screen.getByText('Validation')).toBeInTheDocument();
    expect(screen.getByText('model.core.orders')).toBeInTheDocument();
    expect(screen.getByText('1234 ms')).toBeInTheDocument();
    expect(screen.getByText('view')).toBeInTheDocument();
    // No fixCell was given, so no Fix column header anywhere in the table.
    expect(screen.queryByText('Fix')).toBeNull();
  });

  it('renders a Fix column filled by fixCell when one is given', () => {
    render(<NodeResultsTable
      perNode={[node({ stage: 'validation', node_id: 'model.core.orders' })]}
      fixCell={(stage, n) => <span>fix-for-{stage}-{n.node_id}</span>}
    />);
    expect(screen.getByText('Fix')).toBeInTheDocument();
    expect(screen.getByText('fix-for-validation-model.core.orders')).toBeInTheDocument();
  });

  it('shows a dash for duration and log when a node carries neither', () => {
    render(<NodeResultsTable perNode={[node({ duration_ms: undefined, dbt_log_uri: undefined })]} />);
    const dashes = screen.getAllByText('—');
    expect(dashes.length).toBe(2);
  });
});
