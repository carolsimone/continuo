import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// The words line under a dialog field is a live region, rendered before the
// server has read the value so that its first answer is announced. Empty, it
// is a flex item of the field, and the field's gap above it would add height
// to every dialog that has one. It takes the gap back with a negative margin
// rather than `display: none`, which would remove the live region from the
// accessibility tree until it has text.
const css = readFileSync(fileURLToPath(new URL('../../src/client/styles.css', import.meta.url)), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '');

function ruleBody(selector: string): string {
  for (const match of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (match[1].split(',').map(s => s.trim()).includes(selector)) return match[2];
  }
  throw new Error(`no rule for ${selector}`);
}

describe('styles.css dialog field words line', () => {
  it('takes back the field gap while empty, and stays rendered', () => {
    const gap = ruleBody('.dialog-field').match(/gap:\s*(\d+)px/);
    expect(gap).not.toBeNull();
    const empty = ruleBody('.dialog-field__words:empty');
    expect(empty).toMatch(new RegExp(`margin-top:\\s*-${gap![1]}px`));
    expect(empty).not.toMatch(/display:\s*none/);
  });
});
