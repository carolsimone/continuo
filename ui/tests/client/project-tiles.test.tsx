// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import ProjectTiles from '../../src/client/ProjectTiles';
import { buildProjectColors } from '../../src/client/project-helpers';

const TAGS = { marketing: '1d61398', core: 'c2d1add', finance: '3f718af', 'service-py': 'docker.io/acme/service-py:acb5507' };

describe('ProjectTiles', () => {
  it('renders one tile per project, sorted by name, with its image tag', () => {
    const { container } = render(<ProjectTiles imageTags={TAGS} changedProject="core" />);
    const names = Array.from(container.querySelectorAll('.project-tile .project-tile__name')).map(e => e.textContent);
    expect(names).toEqual(['core', 'finance', 'marketing', 'service-py']);
    const tags = Array.from(container.querySelectorAll('.project-tile .project-tile__tag')).map(e => e.textContent);
    expect(tags).toEqual(['c2d1add', '3f718af', '1d61398', 'docker.io/acme/service-py:acb5507']);
  });

  it('marks only the changed project, and says how many were carried over', () => {
    const { container } = render(<ProjectTiles imageTags={TAGS} changedProject="core" />);
    const changed = container.querySelectorAll('.project-tile--changed');
    expect(changed.length).toBe(1);
    expect(changed[0].querySelector('.project-tile__name')?.textContent).toBe('core');
    expect(changed[0].querySelector('.pill-sm')?.textContent).toBe('changed');
    expect(container.querySelectorAll('.project-tile .pill-sm').length).toBe(1);
    expect(container.querySelector('.section-header__title')?.textContent).toBe('Projects');
    expect(container.querySelector('.section-header__count')?.textContent).toBe('4');
    expect(container.querySelector('.section-header__sub')?.textContent)
      .toBe('core is new in this release · 3 carried over from production');
  });

  it('paints each tile with the accent buildProjectColors assigns it within this set of projects', () => {
    const { container } = render(<ProjectTiles imageTags={TAGS} changedProject="core" />);
    const colors = buildProjectColors(Object.keys(TAGS));
    for (const tile of Array.from(container.querySelectorAll('.project-tile'))) {
      const name = tile.querySelector('.project-tile__name')!.textContent!;
      const dot = tile.querySelector('.nodes-group-dot') as HTMLElement;
      expect(dot.style.background).toBe(hex2rgb(colors.get(name)!));
    }
  });

  it('names what the changed image is new in, when the page is not a release', () => {
    const { container } = render(<ProjectTiles imageTags={TAGS} changedProject="core" subject="release check" />);
    expect(container.querySelector('.section-header__sub')?.textContent)
      .toBe('core is new in this release check · 3 carried over from production');
  });

  it('names where the other images were carried over from, when it is not prod', () => {
    const { container } = render(
      <ProjectTiles imageTags={TAGS} changedProject="core" subject="release check" carriedFrom="rel-9f3e2a1-148" />,
    );
    expect(container.querySelector('.section-header__sub')?.textContent)
      .toBe('core is new in this release check · 3 carried over from rel-9f3e2a1-148');
  });

  it('drops the carried-over count when the changed project is the only one', () => {
    const { container } = render(<ProjectTiles imageTags={{ core: 'c2d1add' }} changedProject="core" />);
    expect(container.querySelector('.section-header__sub')?.textContent).toBe('core is new in this release');
  });

  it('renders no sub-line and no marker when the changed project is unknown', () => {
    const { container } = render(<ProjectTiles imageTags={TAGS} />);
    expect(container.querySelector('.section-header__sub')).toBeNull();
    expect(container.querySelector('.project-tile--changed')).toBeNull();
  });

  it('renders nothing at all when no image is recorded', () => {
    const { container } = render(<ProjectTiles imageTags={{}} changedProject="core" />);
    expect(container.innerHTML).toBe('');
  });
});

function hex2rgb(hex: string): string {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
}
