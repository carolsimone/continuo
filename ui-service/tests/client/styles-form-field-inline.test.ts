import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// Regression guard for the operation-selector alignment bug.
//
// `.form-field` carries a `margin: 8px 0 12px` block margin that suits a filter
// sitting above a list. But the field is reused *inline* as the run/test/build
// operation selector inside action rows (`.page-action-row` on the node page,
// `.scheduler-card-header` on the runs list). There the asymmetric vertical
// margin inflates the flex row (~46px instead of ~27px) and pushes the native
// `<select>` ~1.6px above the sibling `.btn`s' shared centre line, so the
// select and the buttons visibly fail to align on one baseline.
//
// The fix zeroes the margin for the field in those two contexts. This test
// fails if that override is dropped, so the misalignment cannot silently return.
const stylesPath = fileURLToPath(new URL('../../src/client/styles.css', import.meta.url));

/** Return the declaration body of the first rule whose selector list contains `selector`. */
function ruleBodyContaining(css: string, selector: string): string | null {
  for (const match of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    const selectors = match[1];
    if (selectors.includes(selector)) return match[2];
  }
  return null;
}

describe('styles.css inline operation selector', () => {
  for (const context of ['.page-action-row .form-field', '.scheduler-card-header .form-field']) {
    it(`zeroes the .form-field block margin inside ${context.split(' ')[0]}`, () => {
      const css = readFileSync(stylesPath, 'utf8');
      const body = ruleBodyContaining(css, context);

      expect(body, `no rule targets \`${context}\` — the inline operation ` +
        `selector will inherit the block margin and misalign with its buttons`).not.toBeNull();

      expect(
        /margin\s*:\s*0(px)?\b/.test(body ?? ''),
        `\`${context}\` must set \`margin: 0\` so the <select> aligns with the ` +
          `action-row buttons; found:\n  ${(body ?? '').trim()}`,
      ).toBe(true);
    });
  }
});
