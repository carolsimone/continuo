// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import PipelineTimeline from '../../src/client/PipelineTimeline';

describe('PipelineTimeline', () => {
  it('renders one step per transition, coloured like the status pill, with its timestamp', () => {
    const { container } = render(<PipelineTimeline transitions={[
      { to: 'received', at: '2026-09-05T14:25:36Z' },
      { to: 'compiling', at: '2026-09-05T14:25:37Z' },
      { to: 'rejected', at: '2026-09-05T14:31:48Z' },
    ]} />);
    expect(container.querySelector('.section-header__title')?.textContent).toBe('Timeline');
    const steps = Array.from(container.querySelectorAll('.release-timeline__step'));
    expect(steps.map(s => s.querySelector('.pill-sm')?.textContent)).toEqual(['received', 'compiling', 'rejected']);
    expect(steps.map(s => s.querySelector('.pill-sm')?.className)).toEqual([
      'pill-sm pill-sm--pending', 'pill-sm pill-sm--running', 'pill-sm pill-sm--failed',
    ]);
    expect(steps.map(s => s.querySelector('.release-timeline__at')?.textContent)).toEqual([
      '2026-09-05 14:25:36', '2026-09-05 14:25:37', '2026-09-05 14:31:48',
    ]);
    expect(container.querySelector('.release-timeline__step--upcoming')).toBeNull();
  });

  it('ghosts the stages an in-flight run has not reached yet, without a timestamp', () => {
    const { container } = render(<PipelineTimeline transitions={[
      { to: 'received', at: '2026-09-05T14:25:36Z' },
      { to: 'compiling', at: '2026-09-05T14:25:37Z' },
    ]} />);
    const ghosts = Array.from(container.querySelectorAll('.release-timeline__step--upcoming'));
    expect(ghosts.map(s => s.querySelector('.pill-sm')?.textContent)).toEqual(['parsing', 'seed_building', 'validating']);
    for (const g of ghosts) expect(g.querySelector('.release-timeline__at')?.textContent?.trim()).toBe('');
    expect(container.querySelectorAll('.release-timeline__step').length).toBe(5);
  });
});
