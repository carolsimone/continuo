import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// Regression guard for a subtle, silent CSS bug.
//
// The `font` shorthand REQUIRES a font-family, and `inherit` is not a valid
// family token *inside* the shorthand. So `font: 500 12px/1.4 inherit` is an
// invalid declaration that Chrome rejects WHOLESALE — the element then falls
// back to the UA default (~13.3px / line-height: normal / weight 400). On a
// `.btn` that carries an emoji label inside a flex row, `line-height: normal`
// let the glyph balloon the control to ~47px, breaking the design system's
// uniform control height.
//
// The only valid use of `inherit` with the `font` property is the standalone
// `font: inherit` (reset every font sub-property to the inherited value). Any
// `font:` shorthand that lists other tokens AND `inherit` is the broken form.
// Split it into `font-weight` / `font-size` / `line-height` longhands instead
// (font-family inherits by default).
const stylesPath = fileURLToPath(new URL('../../src/client/styles.css', import.meta.url));

describe('styles.css font declarations', () => {
  it('never uses the invalid `font: <tokens> inherit` shorthand', () => {
    const css = readFileSync(stylesPath, 'utf8');

    const offenders: string[] = [];
    for (const match of css.matchAll(/font\s*:\s*([^;{}]+)/g)) {
      const value = match[1].trim();
      // `inherit` is only legal as the sole value of the `font` shorthand.
      if (/\binherit\b/.test(value) && value !== 'inherit') {
        offenders.push(`font: ${value}`);
      }
    }

    expect(
      offenders,
      `Invalid \`font\` shorthand(s) — Chrome drops these entirely; use ` +
        `font-weight/font-size/line-height longhands:\n  ${offenders.join('\n  ')}`,
    ).toEqual([]);
  });
});
