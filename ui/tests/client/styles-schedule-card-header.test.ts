import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// A schedule card's header holds its name, two pills, the cron, when it fires
// next or who paused it, the last run, the Operation select and up to three
// buttons: more than a card holds on a narrow viewport. The header wraps onto
// a second line, its controls moving down together, and each fact keeps to
// one line rather than being squeezed across two.
const css = readFileSync(fileURLToPath(new URL('../../src/client/styles.css', import.meta.url)), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '');

function ruleBody(selector: string): string | null {
  for (const match of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (match[1].split(',').map(s => s.trim()).includes(selector)) return match[2];
  }
  return null;
}

describe('styles.css schedule card header', () => {
  it('wraps instead of squeezing its facts', () => {
    expect(ruleBody('.scheduler-card-header')).toMatch(/flex-wrap:\s*wrap/);
  });

  it('keeps its controls together, right-aligned, when they move to the next line', () => {
    expect(ruleBody('.scheduler-card-actions')).toMatch(/margin-left:\s*auto/);
    expect(ruleBody('.scheduler-card-actions')).toMatch(/display:\s*flex/);
  });

  it.each(['.schedule-cron', '.timestamps'])('keeps %s on one line', (selector) => {
    expect(ruleBody(selector)).toMatch(/white-space:\s*nowrap/);
  });

  it('strikes the cron through while paused', () => {
    expect(ruleBody('.schedule-cron--paused')).toMatch(/text-decoration:\s*line-through/);
  });
});
