// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import TopologyPanel from '../../src/client/TopologyPanel';
import type { ScheduleGraph } from '../../src/client/types';

const graph: ScheduleGraph = {
  nodes: [
    { node_id: 'core.a.seed', node_type: 'dbt-seed', schedule_name: 's' },
    { node_id: 'finance.a.rev', node_type: 'dbt-model', schedule_name: 's' },
  ],
  edges: [{ from_node_id: 'core.a.seed', to_node_id: 'finance.a.rev' }],
};

describe('TopologyPanel', () => {
  it('renders the node-level graph without throwing', () => {
    const { container } = render(<TopologyPanel graph={graph} />);
    expect(container.querySelector('.react-flow')).toBeTruthy();
  });

  it('renders both nodes at the model level, not collapsed service vertices', () => {
    const { container } = render(<TopologyPanel graph={graph} />);
    expect(container.querySelector('.react-flow__node[data-id="core.a.seed"]')).toBeTruthy();
    expect(container.querySelector('.react-flow__node[data-id="finance.a.rev"]')).toBeTruthy();
  });
});
