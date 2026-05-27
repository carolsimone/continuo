// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, useLocation } from 'react-router-dom';
import Tabs, { useActiveTab } from '../../src/client/Tabs';

function LocationProbe() {
  const loc = useLocation();
  return <div data-testid="location">{loc.pathname + loc.search}</div>;
}

function Harness({ entries, param, defaultSlug }: { entries: string[]; param: string; defaultSlug: string }) {
  return (
    <MemoryRouter initialEntries={entries}>
      <Tabs
        param={param}
        defaultSlug={defaultSlug}
        tabs={[
          { slug: defaultSlug, label: 'Runs', count: 3 },
          { slug: 'topology', label: 'Topology', count: 2 },
        ]}
      />
      <LocationProbe />
    </MemoryRouter>
  );
}

describe('Tabs', () => {
  it('renders the default tab as active when no query param is present', () => {
    render(<Harness entries={['/']} param="tab" defaultSlug="runs" />);
    expect(screen.getByRole('tab', { name: /runs/i })).toHaveClass('tabs__tab--active');
    expect(screen.getByRole('tab', { name: /topology/i })).not.toHaveClass('tabs__tab--active');
  });

  it('reads the active tab from the query param', () => {
    render(<Harness entries={['/?tab=topology']} param="tab" defaultSlug="runs" />);
    expect(screen.getByRole('tab', { name: /topology/i })).toHaveClass('tabs__tab--active');
    expect(screen.getByRole('tab', { name: /runs/i })).not.toHaveClass('tabs__tab--active');
  });

  it('falls back to the default tab when the query param is unknown', () => {
    render(<Harness entries={['/?tab=garbage']} param="tab" defaultSlug="runs" />);
    expect(screen.getByRole('tab', { name: /runs/i })).toHaveClass('tabs__tab--active');
  });

  it('updates the URL when a non-default tab is clicked', async () => {
    const user = userEvent.setup();
    render(<Harness entries={['/']} param="tab" defaultSlug="runs" />);
    await user.click(screen.getByRole('tab', { name: /topology/i }));
    expect(screen.getByTestId('location').textContent).toBe('/?tab=topology');
  });

  it('clears the query param when switching back to the default tab', async () => {
    const user = userEvent.setup();
    render(<Harness entries={['/?tab=topology']} param="tab" defaultSlug="runs" />);
    await user.click(screen.getByRole('tab', { name: /runs/i }));
    expect(screen.getByTestId('location').textContent).toBe('/');
  });

  it('preserves unrelated query params when switching tabs', async () => {
    const user = userEvent.setup();
    render(<Harness entries={['/?filter=foo']} param="tab" defaultSlug="runs" />);
    await user.click(screen.getByRole('tab', { name: /topology/i }));
    expect(screen.getByTestId('location').textContent).toBe('/?filter=foo&tab=topology');
  });

  it('renders the count pill when count is 0 (not just truthy)', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Tabs
          param="tab"
          defaultSlug="runs"
          tabs={[
            { slug: 'runs', label: 'Runs', count: 0 },
            { slug: 'topology', label: 'Topology', count: 5 },
          ]}
        />
      </MemoryRouter>,
    );
    const counts = document.querySelectorAll('.tabs__count');
    expect(counts).toHaveLength(2);
    expect(counts[0].textContent).toBe('0');
    expect(counts[1].textContent).toBe('5');
  });

  it('renders count pills next to each label', () => {
    render(<Harness entries={['/']} param="tab" defaultSlug="runs" />);
    const counts = document.querySelectorAll('.tabs__count');
    expect(counts).toHaveLength(2);
    expect(counts[0].textContent).toBe('3');
    expect(counts[1].textContent).toBe('2');
  });

  it('applies the panel variant class when variant="panel"', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Tabs
          variant="panel"
          param="panel"
          defaultSlug="nodes"
          tabs={[
            { slug: 'nodes', label: 'Nodes' },
            { slug: 'runs', label: 'Past Runs' },
          ]}
        />
      </MemoryRouter>,
    );
    const nav = document.querySelector('nav.tabs');
    expect(nav).not.toBeNull();
    expect(nav).toHaveClass('tabs--panel');
  });
});

describe('useActiveTab', () => {
  function Probe({ param, defaultSlug }: { param: string; defaultSlug: string }) {
    const slug = useActiveTab(param, defaultSlug, ['runs', 'topology']);
    return <div data-testid="active">{slug}</div>;
  }

  it('returns the default when query param is absent', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Probe param="tab" defaultSlug="runs" />
      </MemoryRouter>,
    );
    expect(screen.getByTestId('active').textContent).toBe('runs');
  });

  it('returns the param value when it is a known slug', () => {
    render(
      <MemoryRouter initialEntries={['/?tab=topology']}>
        <Probe param="tab" defaultSlug="runs" />
      </MemoryRouter>,
    );
    expect(screen.getByTestId('active').textContent).toBe('topology');
  });

  it('returns the default when the param value is unknown', () => {
    render(
      <MemoryRouter initialEntries={['/?tab=garbage']}>
        <Probe param="tab" defaultSlug="runs" />
      </MemoryRouter>,
    );
    expect(screen.getByTestId('active').textContent).toBe('runs');
  });
});
