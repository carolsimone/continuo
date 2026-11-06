import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// What jsdom cannot see of a run's status bar and counts: one failure among
// thousands of tasks must stay visible, and a task that has not started reads
// as hollow in every place a status is painted, because a filled pending
// segment looks like work already done.
const css = readFileSync(fileURLToPath(new URL('../../src/client/styles.css', import.meta.url)), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '');

function ruleBodies(selector: string): string[] {
  const bodies: string[] = [];
  for (const match of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (match[1].split(',').map(s => s.trim()).includes(selector)) bodies.push(match[2]);
  }
  return bodies;
}

function ruleBody(selector: string): string {
  return ruleBodies(selector).join('\n');
}

describe('styles.css task status bar', () => {
  it('keeps a segment visible however small its share of the run', () => {
    expect(ruleBody('.rp-seg')).toMatch(/min-width:\s*[1-9]/);
  });

  it('animates a segment growing, as the card bar it replaces did', () => {
    expect(ruleBody('.rp-seg')).toMatch(/transition:\s*width/);
  });

  it('leaves a pending segment unpainted, so the track shows through', () => {
    expect(ruleBody('.rp-pending')).toMatch(/background:\s*transparent/);
  });

  it.each(['.rp-dot.rp-pending', '.swim-legend-key.pend', '.swim-cell.pend'])(
    'draws %s as a hollow square',
    (selector) => {
      expect(ruleBody(selector)).toMatch(/background:\s*transparent/);
      expect(ruleBody(selector)).toMatch(/box-shadow:\s*inset/);
    },
  );

  it('colours a failed count, and its number, red', () => {
    expect(ruleBody('.rp-count--alert')).toMatch(/color:\s*#dc2626/);
  });

  it('pulses only the running segment of the bar, not its legend dot, and stops for reduced motion', () => {
    expect(ruleBodies('.rp-seg.rp-running')[0]).toMatch(/animation:\s*rp-pulse/);
    expect(ruleBody('.rp-running')).not.toMatch(/animation/);
    const reduced = /@media \(prefers-reduced-motion: reduce\)\s*\{[^{}]*\.rp-seg\.rp-running\s*\{\s*animation:\s*none/;
    expect(css).toMatch(reduced);
  });
});
