import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// A preview list is the read-only sibling of the snapshot pick list: it shares
// the list, row box and row text rules by selector grouping, never by a copy
// of their declarations, and its rows are not click targets. It sits in a
// .dialog--wide next to option rows and counts, so the list is shorter than a
// pick list and the wide dialog caps its own height: together they keep the
// dialog's footer on a short viewport. An option row lays a checkbox out
// exactly as a radio choice. ui.md documents these rules; every selector it
// names for them must exist in styles.css.
const stylesPath = fileURLToPath(new URL('../../src/client/styles.css', import.meta.url));
const guidelinePath = fileURLToPath(new URL('../../../.claude/design-guideline/ui.md', import.meta.url));

interface Rule {
  selectors: string[];
  body: string;
}

function parseRules(css: string): Rule[] {
  const withoutComments = css.replace(/\/\*[\s\S]*?\*\//g, '');
  return Array.from(withoutComments.matchAll(/([^{}]+)\{([^{}]*)\}/g), m => ({
    selectors: m[1].split(',').map(s => s.trim().replace(/\s+/g, ' ')).filter(Boolean),
    body: m[2],
  }));
}

/** Return the declaration body of the first rule whose selector list contains `selector`. */
function ruleBodyContaining(rules: Rule[], selector: string): string | null {
  return rules.find(r => r.selectors.includes(selector))?.body ?? null;
}

/** Return the selector list of the first rule that contains `selector`. */
function selectorsOfRuleContaining(rules: Rule[], selector: string): string[] {
  return rules.find(r => r.selectors.includes(selector))?.selectors ?? [];
}

const css = readFileSync(stylesPath, 'utf8');
const rules = parseRules(css);

describe('styles.css preview list', () => {
  it.each([
    ['.pick-list', '.preview-list'],
    ['.pick-row', '.preview-row'],
    ['.pick-row__main', '.preview-row__main'],
    ['.pick-row__id', '.preview-row__id'],
    ['.pick-row__meta', '.preview-facts'],
    ['.pick-row__sep', '.preview-facts__sep'],
  ])('declares %s and %s in one shared rule', (pick, preview) => {
    expect(selectorsOfRuleContaining(rules, preview)).toContain(pick);
  });

  it('scrolls the list inside its own bounded height, shorter than a pick list', () => {
    expect(ruleBodyContaining(rules, '.preview-list')).toMatch(/overflow-y\s*:\s*auto/);
    const own = rules.find(r => r.selectors.length === 1 && r.selectors[0] === '.preview-list');
    expect(own?.body).toMatch(/max-height\s*:\s*min\(32vh,\s*280px\)/);
    const pick = rules.find(r => r.selectors.length === 1 && r.selectors[0] === '.pick-list');
    expect(pick?.body).toMatch(/max-height\s*:\s*min\(52vh,\s*420px\)/);
  });

  it('gives a preview row no pointer and no hover: it is not a click target', () => {
    const previewRowRules = rules.filter(r => r.selectors.some(s => s.startsWith('.preview-row')));
    expect(previewRowRules.length).toBeGreaterThan(0);
    for (const rule of previewRowRules) {
      expect(rule.body).not.toMatch(/cursor/);
      expect(rule.selectors.some(s => s.startsWith('.preview-row') && s.includes(':'))).toBe(false);
    }
    expect(ruleBodyContaining(rules, '.pick-row')).not.toMatch(/cursor/);
    const pickOnly = rules.find(r => r.selectors.length === 1 && r.selectors[0] === '.pick-row');
    expect(pickOnly?.body).toMatch(/cursor\s*:\s*pointer/);
  });

  it('lets a preview group header take the pointer and a visible focus ring', () => {
    expect(ruleBodyContaining(rules, '.preview-group__header')).toMatch(/cursor\s*:\s*pointer/);
    expect(ruleBodyContaining(rules, '.preview-group__header:focus-visible')).toMatch(/outline/);
  });
});

describe('styles.css wide dialog', () => {
  it('caps a wide dialog at the viewport height and scrolls inside it', () => {
    const body = ruleBodyContaining(rules, '.dialog--wide') ?? '';
    expect(body).toMatch(/width\s*:\s*480px/);
    expect(body).toMatch(/max-height\s*:\s*calc\(100vh - 32px\)/);
    expect(body).toMatch(/overflow-y\s*:\s*auto/);
  });

  it('has no rerun-specific dialog width class left', () => {
    expect(rules.some(r => r.selectors.includes('.rerun-confirm-dialog'))).toBe(false);
  });
});

describe('styles.css option rows', () => {
  it('places an option row checkbox where its radio goes', () => {
    const selectors = selectorsOfRuleContaining(rules, '.rerun-mode-option > input[type="checkbox"]');
    expect(selectors).toContain('.rerun-mode-option > input[type="radio"]');
    expect(ruleBodyContaining(rules, '.rerun-mode-option > input[type="checkbox"]')).toMatch(/grid-row\s*:\s*1\s*\/\s*span 2/);
  });
});

describe('ui.md and styles.css agree', () => {
  it('names only selectors styles.css declares in the preview list, option row and wide dialog patterns', () => {
    const guideline = readFileSync(guidelinePath, 'utf8');
    const modals = guideline.slice(guideline.indexOf('## Modals'), guideline.indexOf('## Section headers'));
    const start = modals.indexOf('### Preview list');
    expect(start).toBeGreaterThan(-1);
    expect(modals.indexOf('### Option rows')).toBeGreaterThan(start);
    const blocks = Array.from(modals.matchAll(/```css\n([\s\S]*?)```/g), m => m[1]);
    const documented = blocks
      .filter(block => /preview-|rerun-mode-option|dialog--wide/.test(block))
      .flatMap(block => parseRules(block).flatMap(r => r.selectors));
    expect(documented).toEqual(expect.arrayContaining(['.preview-list', '.preview-row', '.dialog--wide',
      '.rerun-mode-option > input[type="checkbox"]']));

    const declared = new Set(rules.flatMap(r => r.selectors));
    expect(documented.filter(selector => !declared.has(selector))).toEqual([]);
  });
});
