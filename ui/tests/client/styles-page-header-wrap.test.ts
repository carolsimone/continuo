import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// Regression guard for the shared page header on narrow viewports.
//
// The header is one flex row: brand, divider, back link, title, status pill,
// chips, then the right-aligned actions with the user's email and Sign out.
// On a 500px-wide viewport that row is wider than the page, so without
// wrapping it forces horizontal scrolling and pushes Sign out off screen. The
// rules below let the actions drop to a second line, let the title shrink,
// and cap the email; this test fails if any of them is dropped.
const stylesPath = fileURLToPath(new URL('../../src/client/styles.css', import.meta.url));

function ruleBodyContaining(css: string, selector: string): string | null {
  for (const match of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    const selectors = match[1].split(',').map(s => s.trim());
    if (selectors.includes(selector)) return match[2];
  }
  return null;
}

describe('styles.css page header on narrow viewports', () => {
  const css = readFileSync(stylesPath, 'utf8');

  it('lets the header row wrap instead of overflowing the page', () => {
    expect(ruleBodyContaining(css, '.page-header')).toMatch(/flex-wrap:\s*wrap/);
  });

  it('lets the page title shrink and break rather than widen the row', () => {
    const body = ruleBodyContaining(css, '.detail-page-title');
    expect(body).toMatch(/min-width:\s*0/);
    expect(body).toMatch(/overflow-wrap:\s*anywhere/);
  });

  it('caps the signed-in email so it cannot push Sign out off screen', () => {
    const body = ruleBodyContaining(css, '.user-menu__email');
    expect(body).toMatch(/max-width:/);
    expect(body).toMatch(/text-overflow:\s*ellipsis/);
  });
});
