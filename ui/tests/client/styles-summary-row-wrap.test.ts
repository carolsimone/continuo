import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// The Runs card's summary row holds one count per task status, nine in all,
// which is wider than a card on a narrow viewport. It wraps onto a second line
// rather than widen the card and scroll the page sideways.
const css = readFileSync(fileURLToPath(new URL('../../src/client/styles.css', import.meta.url)), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '');

function ruleBody(selector: string): string | null {
  for (const match of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (match[1].split(',').map(s => s.trim()).includes(selector)) return match[2];
  }
  return null;
}

describe('styles.css Runs card summary row', () => {
  it('wraps instead of overflowing the card', () => {
    expect(ruleBody('.summary-row')).toMatch(/flex-wrap:\s*wrap/);
  });
});
