import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// The browser-facing brand assets under ui/public/ are copies of the canonical
// files in docs/logo/. Vite copies public/ into dist/ verbatim, so the UI
// cannot reference docs/logo/ directly; this guard keeps the two copies from
// drifting apart when one side is redesigned.
const uiPublic = (name: string) => fileURLToPath(new URL(`../../public/${name}`, import.meta.url));
const docsLogo = (name: string) => fileURLToPath(new URL(`../../../docs/logo/${name}`, import.meta.url));
const indexHtml = fileURLToPath(new URL('../../index.html', import.meta.url));

describe('brand assets', () => {
  it.each(['favicon.svg', 'mark-light.svg'])('public/%s is byte-identical to docs/logo', name => {
    expect(readFileSync(uiPublic(name), 'utf8')).toBe(readFileSync(docsLogo(name), 'utf8'));
  });

  it('ships the apple touch icon', () => {
    expect(existsSync(uiPublic('apple-touch-icon.png'))).toBe(true);
  });

  it('index.html links the favicon and the apple touch icon', () => {
    const html = readFileSync(indexHtml, 'utf8');
    expect(html).toMatch(/<link rel="icon" type="image\/svg\+xml" href="\/favicon.svg"\s*\/>/);
    expect(html).toMatch(/<link rel="apple-touch-icon" href="\/apple-touch-icon.png"\s*\/>/);
  });
});
