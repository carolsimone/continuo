// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import NodeTypeIcon, { nodeTypeFamily } from './NodeTypeIcon';

describe('nodeTypeFamily', () => {
  it('maps every dbt node type to the dbt family', () => {
    expect(nodeTypeFamily('dbt-model')).toBe('dbt');
    expect(nodeTypeFamily('dbt-seed')).toBe('dbt');
    expect(nodeTypeFamily('dbt-snapshot')).toBe('dbt');
  });

  it('maps python-model to python and python-csv to python-csv', () => {
    expect(nodeTypeFamily('python-model')).toBe('python');
    expect(nodeTypeFamily('python-csv')).toBe('python-csv');
  });

  it('maps empty and unknown types to null', () => {
    expect(nodeTypeFamily('')).toBeNull();
    expect(nodeTypeFamily('something-else')).toBeNull();
  });
});

describe('NodeTypeIcon', () => {
  it('renders the dbt mark for a dbt node type', () => {
    const { container } = render(<NodeTypeIcon nodeType="dbt-seed" />);
    const icon = container.querySelector('[data-node-type-icon="dbt"]');
    expect(icon).not.toBeNull();
    expect(icon!.querySelector('path')).not.toBeNull();
  });

  it('renders the python mark for python-model', () => {
    const { container } = render(<NodeTypeIcon nodeType="python-model" />);
    expect(container.querySelector('[data-node-type-icon="python"]')).not.toBeNull();
  });

  it('renders the python mark plus a table badge for python-csv', () => {
    const { container } = render(<NodeTypeIcon nodeType="python-csv" />);
    const icon = container.querySelector('[data-node-type-icon="python-csv"]');
    expect(icon).not.toBeNull();
    // Two glyphs: the python logo and the table badge overlay.
    expect(icon!.querySelectorAll('svg').length).toBe(2);
  });

  it('renders nothing for an empty or unknown node type', () => {
    const empty = render(<NodeTypeIcon nodeType="" />);
    expect(empty.container.firstChild).toBeNull();
    const unknown = render(<NodeTypeIcon nodeType="mystery" />);
    expect(unknown.container.firstChild).toBeNull();
  });

  it('sizes the mark from the size prop', () => {
    const { container } = render(<NodeTypeIcon nodeType="dbt-model" size={18} />);
    const svg = container.querySelector('svg')!;
    expect(svg.getAttribute('width')).toBe('18');
    expect(svg.getAttribute('height')).toBe('18');
  });
});
