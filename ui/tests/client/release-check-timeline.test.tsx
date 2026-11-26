// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import ReleaseCheckTimeline from '../../src/client/ReleaseCheckTimeline';

describe('ReleaseCheckTimeline', () => {
  it('renders one step per transition, coloured like the status pill, with its timestamp', () => {
    const { container } = render(<ReleaseCheckTimeline transitions={[
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

  it('ghosts the stages an in-flight release check has not reached yet, without a timestamp', () => {
    const { container } = render(<ReleaseCheckTimeline transitions={[
      { to: 'received', at: '2026-09-05T14:25:36Z' },
      { to: 'compiling', at: '2026-09-05T14:25:37Z' },
    ]} check={{ manifestKind: 'dbt' }} />);
    const ghosts = Array.from(container.querySelectorAll('.release-timeline__step--upcoming'));
    // The stage name is what shows; "(upcoming)" is read out but visually hidden.
    expect(ghosts.map(s => s.querySelector('.pill-sm')?.textContent)).toEqual([
      'parsing (upcoming)', 'seed_building (upcoming)', 'validating (upcoming)',
    ]);
    for (const g of ghosts) {
      expect(g.querySelector('.pill-sm .sr-only')?.textContent).toBe(' (upcoming)');
      expect(g.querySelector('.release-timeline__at')).toBeNull();
    }
    expect(container.querySelectorAll('.release-timeline__step').length).toBe(5);
  });

  it('says under a ghost what it depends on, when the stage may not run', () => {
    const { container } = render(<ReleaseCheckTimeline transitions={[
      { to: 'received', at: '2026-09-05T14:25:36Z' },
      { to: 'compiling', at: '2026-09-05T14:25:37Z' },
    ]} check={{ manifestKind: 'dbt' }} />);
    const ghosts = Array.from(container.querySelectorAll('.release-timeline__step--upcoming'));
    expect(ghosts.map(g => g.querySelector('.release-timeline__hint')?.textContent ?? null))
      .toEqual([null, 'if seeds changed', 'unless nothing changed']);
  });

  it('previews only the stages that apply to the release check\'s kind', () => {
    const { container } = render(<ReleaseCheckTimeline transitions={[
      { to: 'received', at: '2026-09-05T14:25:36Z' },
    ]} check={{ manifestKind: 'python' }} />);
    const ghosts = Array.from(container.querySelectorAll('.release-timeline__step--upcoming .pill-sm'))
      .map(p => p.firstChild?.textContent);
    expect(ghosts).toEqual(['parsing', 'validating']);
  });

  it('renders the section with no steps when nothing has been recorded', () => {
    const { container } = render(<ReleaseCheckTimeline transitions={[]} />);
    expect(container.querySelector('.section-header__title')?.textContent).toBe('Timeline');
    expect(container.querySelectorAll('.release-timeline__step').length).toBe(0);
  });

  describe('parse and contract generation', () => {
    const step = (to: string) => ({ to, at: '2026-09-05T14:25:36Z' });
    const generated = { parse_ms: 2140, contracts_ms: 610, contracts_by_family: { dbt: 45, python: 3 } };

    it('shows parsing and contract generation under the parsing step', () => {
      render(<ReleaseCheckTimeline transitions={[step('parsing'), step('validating')]} check={generated} />);
      expect(screen.getByText('parsing 2.1 s')).toBeInTheDocument();
      expect(screen.getByText('generating contracts 0.6 s · 48 contracts (dbt 45, python 3)')).toBeInTheDocument();
    });

    it('puts both lines in the parsing step and in no other step', () => {
      const { container } = render(
        <ReleaseCheckTimeline transitions={[step('received'), step('compiling'), step('parsing'), step('validating')]} check={generated} />,
      );
      const steps = Array.from(container.querySelectorAll('.release-timeline__step'));
      expect(steps.map(s => s.querySelector('.pill-sm')?.textContent)).toEqual(['received', 'compiling', 'parsing', 'validating']);
      expect(steps.map(s => Array.from(s.querySelectorAll('.release-timeline__line')).map(l => l.textContent))).toEqual([
        [],
        [],
        ['parsing 2.1 s', 'generating contracts 0.6 s · 48\u00a0contracts (dbt 45, python 3)'],
        [],
      ]);
    });

    it('shows no sub-lines for a release without them', () => {
      const { container } = render(
        <ReleaseCheckTimeline transitions={[step('parsing'), step('validating')]} check={{ manifestKind: 'dbt' }} />,
      );
      expect(container.querySelector('.release-timeline__line')).toBeNull();
      expect(screen.queryByText(/^parsing \d/)).not.toBeInTheDocument();
      expect(screen.queryByText(/generating contracts/)).not.toBeInTheDocument();
    });

    it('shows no sub-lines when the release check has not reached parsing, whatever it carries', () => {
      const { container } = render(<ReleaseCheckTimeline transitions={[step('received'), step('compiling')]} check={generated} />);
      expect(container.querySelector('.release-timeline__line')).toBeNull();
    });

    it('does not add them to the ghost of a parsing step the release check has not reached', () => {
      const { container } = render(
        <ReleaseCheckTimeline transitions={[step('received'), step('compiling')]} check={{ manifestKind: 'dbt', ...generated }} />,
      );
      expect(container.querySelector('.release-timeline__step--upcoming .release-timeline__line')).toBeNull();
    });

    it('shows the durations without a count when the parse did not count its contracts', () => {
      render(<ReleaseCheckTimeline transitions={[step('parsing')]} check={{ parse_ms: 2140, contracts_ms: 610 }} />);
      expect(screen.getByText('parsing 2.1 s')).toBeInTheDocument();
      expect(screen.getByText('generating contracts 0.6 s')).toBeInTheDocument();
    });
  });
});
