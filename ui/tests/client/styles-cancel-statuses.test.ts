import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { attemptPillClass, taskPillClass } from '../../src/client/detail-page-helpers';

// A cancelling run, a stopping task and a stopped attempt each have a style of
// their own in every place the client paints a status; none falls to the
// default pill. The class names come from the helpers the components call, so
// a renamed class without a rule fails here.
const css = readFileSync(fileURLToPath(new URL('../../src/client/styles.css', import.meta.url)), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '');

function selectors(): Set<string> {
  const found = new Set<string>();
  for (const m of css.matchAll(/([^{}]+)\{[^{}]*\}/g)) {
    for (const s of m[1].split(',')) found.add(s.trim());
  }
  return found;
}

function declarationOf(selector: string): string {
  for (const m of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (m[1].split(',').map((s) => s.trim()).includes(selector)) return m[2].replace(/\s+/g, ' ').trim();
  }
  throw new Error(`no rule for ${selector}`);
}

describe('cancel statuses in styles.css', () => {
  const sels = selectors();

  it('gives cancelling and stopping a pill, in the neutral-warning palette', () => {
    const cancelling = taskPillClass('cancelling');
    const stopping = taskPillClass('stopping');
    expect(cancelling).toBe('pill-sm--cancelling');
    expect(stopping).toBe('pill-sm--stopping');
    expect(declarationOf(`.${cancelling}`)).toBe('background: #fef3c7; color: #92400e;');
    expect(declarationOf(`.${stopping}`)).toBe('background: #fef3c7; color: #92400e;');
    expect(sels.has('.pill--cancelling')).toBe(true);
  });

  it('gives a stopped attempt a pill that is not the pending default', () => {
    const cls = attemptPillClass('stopped');
    expect(cls).toBe('pill-sm--stopped');
    expect(sels.has(`.${cls}`)).toBe(true);
    expect(declarationOf(`.${cls}`)).not.toBe(declarationOf('.pill-sm--pending'));
  });

  it('paints the run-card badge, the progress bucket and every swimlane element', () => {
    for (const s of [
      '.status-cancelling', '.rp-stopping',
      '.swim-roll.stop', '.swim-node.stop', '.swim-sd.stop', '.swim-legend-key.stop', '.swim-cell.stop',
    ]) {
      expect(sels.has(s), `${s} has no rule`).toBe(true);
    }
  });
});
