// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import PreviewList, { PREVIEW_GROUP_ABOVE, PreviewFacts, PreviewItem } from '../../src/client/PreviewList';

// items builds n rows alternating between two services, in the order a
// caller hands them over.
function items(n: number): PreviewItem[] {
  return Array.from({ length: n }, (_, i) => ({
    key: `k${i}`,
    service: i % 2 === 0 ? 'core' : 'billing',
    label: `stg.t${String(i).padStart(2, '0')}`,
    title: `${i % 2 === 0 ? 'core' : 'billing'}.stg.t${String(i).padStart(2, '0')}`,
    facts: [],
  }));
}

const colors = new Map([['billing', '#0ea5e9'], ['core', '#f59e0b']]);

function rowIds(): string[] {
  return Array.from(document.querySelectorAll('.preview-row .preview-row__id')).map(el => el.textContent ?? '');
}

describe('PreviewList', () => {
  it('lists up to twelve rows flat, in the order given, none of them a button', () => {
    render(<PreviewList items={items(PREVIEW_GROUP_ABOVE)} colors={colors} />);

    expect(PREVIEW_GROUP_ABOVE).toBe(12);
    expect(document.querySelector('ul.preview-list')).toBeTruthy();
    expect(document.querySelectorAll('li.preview-row')).toHaveLength(12);
    expect(rowIds()[0]).toBe('stg.t00');
    expect(rowIds()[11]).toBe('stg.t11');
    expect(document.querySelector('.preview-group')).toBeNull();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('shows the monospace id with the full id as its title, and the accent dot of its service', () => {
    render(<PreviewList items={items(2)} colors={colors} />);

    const [first, second] = Array.from(document.querySelectorAll<HTMLElement>('.preview-row'));
    const id = first.querySelector('.preview-row__id') as HTMLElement;
    expect(id.textContent).toBe('stg.t00');
    expect(id).toHaveAttribute('title', 'core.stg.t00');
    expect((first.querySelector('.nodes-group-dot') as HTMLElement).style.background).toBe('rgb(245, 158, 11)');
    expect((second.querySelector('.nodes-group-dot') as HTMLElement).style.background).toBe('rgb(14, 165, 233)');
  });

  it('paints a service the palette lacks in neutral slate', () => {
    render(<PreviewList items={[{ key: 'a', service: 'ops', label: 'stg.a', facts: [] }]} colors={colors} />);

    expect((document.querySelector('.nodes-group-dot') as HTMLElement).style.background).toBe('rgb(148, 163, 184)');
  });

  it('separates a row facts with a middle dot and leaves the line out when no fact is known', () => {
    render(
      <PreviewList
        items={[
          { key: 'a', service: 'core', label: 'stg.a', facts: ['+2 downstream', 'last run 9s'] },
          { key: 'b', service: 'core', label: 'stg.b', facts: [] },
        ]}
        colors={colors}
      />,
    );

    const rows = document.querySelectorAll('.preview-row');
    const facts = rows[0].querySelector('.preview-facts') as HTMLElement;
    expect(Array.from(facts.children).map(c => c.textContent)).toEqual(['+2 downstream', '·', 'last run 9s']);
    expect(facts.querySelector('.preview-facts__sep')).toHaveAttribute('aria-hidden', 'true');
    expect(rows[1].querySelector('.preview-facts')).toBeNull();
  });

  it('groups thirteen rows by service, sorted by name and collapsed, with a count pill', () => {
    render(<PreviewList items={items(13)} colors={colors} />);

    const headers = screen.getAllByRole('button');
    expect(headers.map(h => h.querySelector('.nodes-group-name')?.textContent)).toEqual(['billing', 'core']);
    expect(headers.map(h => h.querySelector('.nodes-group-count')?.textContent)).toEqual(['6', '7']);
    expect(headers.map(h => h.getAttribute('aria-expanded'))).toEqual(['false', 'false']);
    expect(headers.every(h => h.classList.contains('nodes-group-header') && h.classList.contains('preview-group__header'))).toBe(true);
    expect((headers[0].querySelector('.nodes-group-dot') as HTMLElement).style.background).toBe('rgb(14, 165, 233)');
    expect(headers[0].querySelector('.pill-sm')).toBeNull();
    expect(document.querySelectorAll('.preview-row')).toHaveLength(0);
  });

  it('opens and closes a group on click, Enter and Space', async () => {
    const user = userEvent.setup();
    render(<PreviewList items={items(13)} colors={colors} />);
    const [billing, core] = screen.getAllByRole('button');

    await user.click(core);
    expect(core).toHaveAttribute('aria-expanded', 'true');
    expect(core.querySelector('.nodes-group-chevron')?.textContent).toBe('▾');
    expect(rowIds()).toEqual(['stg.t00', 'stg.t02', 'stg.t04', 'stg.t06', 'stg.t08', 'stg.t10', 'stg.t12']);
    expect(core.closest('.preview-group')?.querySelector('ul.preview-group__rows')).toBeTruthy();

    billing.focus();
    await user.keyboard('{Enter}');
    expect(document.querySelectorAll('.preview-row')).toHaveLength(13);

    core.focus();
    await user.keyboard(' ');
    expect(core).toHaveAttribute('aria-expanded', 'false');
    expect(document.querySelectorAll('.preview-row')).toHaveLength(6);
  });

  it('groups at a caller-chosen threshold', () => {
    render(<PreviewList items={items(3)} colors={colors} groupAbove={2} />);

    expect(screen.getAllByRole('button')).toHaveLength(2);
  });
});

describe('PreviewFacts', () => {
  it('renders a summary line with the summary modifier', () => {
    render(<PreviewFacts facts={['3 failed or skipped', '35 downstream']} summary />);

    const line = document.querySelector('.preview-facts') as HTMLElement;
    expect(line).toHaveClass('preview-facts--summary');
    expect(Array.from(line.children).map(c => c.textContent)).toEqual(['3 failed or skipped', '·', '35 downstream']);
  });

  it('renders nothing for no facts', () => {
    const { container } = render(<PreviewFacts facts={[]} summary />);

    expect(container.innerHTML).toBe('');
  });
});
