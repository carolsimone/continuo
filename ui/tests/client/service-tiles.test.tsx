// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import ServiceTiles from '../../src/client/ServiceTiles';
import { buildServiceColors } from '../../src/client/service-helpers';

const TAGS = { marketing: '1d61398', core: 'c2d1add', finance: '3f718af', 'service-py': 'docker.io/acme/service-py:acb5507' };

describe('ServiceTiles', () => {
  it('renders one tile per service, sorted by name, with its image tag', () => {
    const { container } = render(<ServiceTiles imageTags={TAGS} changedService="core" />);
    const names = Array.from(container.querySelectorAll('.service-tile .service-tile__name')).map(e => e.textContent);
    expect(names).toEqual(['core', 'finance', 'marketing', 'service-py']);
    const tags = Array.from(container.querySelectorAll('.service-tile .service-tile__tag')).map(e => e.textContent);
    expect(tags).toEqual(['c2d1add', '3f718af', '1d61398', 'docker.io/acme/service-py:acb5507']);
  });

  it('marks only the changed service, and says how many were carried over', () => {
    const { container } = render(<ServiceTiles imageTags={TAGS} changedService="core" />);
    const changed = container.querySelectorAll('.service-tile--changed');
    expect(changed.length).toBe(1);
    expect(changed[0].querySelector('.service-tile__name')?.textContent).toBe('core');
    expect(changed[0].querySelector('.pill-sm')?.textContent).toBe('changed');
    expect(container.querySelectorAll('.service-tile .pill-sm').length).toBe(1);
    expect(container.querySelector('.section-header__title')?.textContent).toBe('Services');
    expect(container.querySelector('.section-header__count')?.textContent).toBe('4');
    expect(container.querySelector('.section-header__sub')?.textContent)
      .toBe('core is new in this release · 3 carried over from prod');
  });

  it('paints each tile with the service accent shared by every other surface', () => {
    const { container } = render(<ServiceTiles imageTags={TAGS} changedService="core" />);
    const colors = buildServiceColors(Object.keys(TAGS));
    for (const tile of Array.from(container.querySelectorAll('.service-tile'))) {
      const name = tile.querySelector('.service-tile__name')!.textContent!;
      const dot = tile.querySelector('.nodes-group-dot') as HTMLElement;
      expect(dot.style.background).toBe(hex2rgb(colors.get(name)!));
    }
  });

  it('names what the changed image is new in, when the page is not a release', () => {
    const { container } = render(<ServiceTiles imageTags={TAGS} changedService="core" subject="verification run" />);
    expect(container.querySelector('.section-header__sub')?.textContent)
      .toBe('core is new in this verification run · 3 carried over from prod');
  });

  it('drops the carried-over count when the changed service is the only one', () => {
    const { container } = render(<ServiceTiles imageTags={{ core: 'c2d1add' }} changedService="core" />);
    expect(container.querySelector('.section-header__sub')?.textContent).toBe('core is new in this release');
  });

  it('renders no sub-line and no marker when the changed service is unknown', () => {
    const { container } = render(<ServiceTiles imageTags={TAGS} />);
    expect(container.querySelector('.section-header__sub')).toBeNull();
    expect(container.querySelector('.service-tile--changed')).toBeNull();
  });

  it('renders nothing at all when no image is recorded', () => {
    const { container } = render(<ServiceTiles imageTags={{}} changedService="core" />);
    expect(container.innerHTML).toBe('');
  });
});

function hex2rgb(hex: string): string {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
}
