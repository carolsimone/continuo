import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// A schedule card holds a name, a status, the cron and its facts, a progress
// line and up to three controls: more than a card holds on a narrow viewport.
// Each of its rows wraps, its controls moving down together, rather than widen
// the card and scroll the page sideways. jsdom loads no stylesheet, so these
// rules are pinned here.
const css = readFileSync(fileURLToPath(new URL('../../src/client/styles.css', import.meta.url)), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '');

function ruleBody(selector: string): string {
  const bodies: string[] = [];
  for (const match of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (match[1].split(',').map(s => s.trim()).includes(selector)) bodies.push(match[2]);
  }
  return bodies.join('\n');
}

describe('styles.css schedule card', () => {
  it.each(['.scheduler-card-header', '.preview-facts', '.scheduler-card-summary'])('wraps %s rather than squeeze it', (selector) => {
    expect(ruleBody(selector)).toMatch(/flex-wrap:\s*wrap/);
  });

  it('keeps its controls together, right-aligned, when they move to the next line', () => {
    expect(ruleBody('.scheduler-card-actions')).toMatch(/margin-left:\s*auto/);
    expect(ruleBody('.scheduler-card-actions')).toMatch(/display:\s*flex/);
  });

  it('lets long cron words wrap inside the card instead of being clipped by it', () => {
    expect(ruleBody('.schedule-cron')).not.toMatch(/white-space:\s*nowrap/);
    expect(ruleBody('.schedule-name')).toMatch(/overflow-wrap:\s*anywhere/);
  });

  it('strikes the cron through while paused', () => {
    expect(ruleBody('.schedule-cron--paused')).toMatch(/text-decoration:\s*line-through/);
  });

  it('sets a raw cron, a value a parser reads, in monospace', () => {
    expect(ruleBody('.schedule-cron--raw')).toMatch(/font-family:[^;]*monospace/);
  });

  it('lets text on the card be selected', () => {
    expect(ruleBody('.scheduler-card')).not.toMatch(/user-select:\s*none/);
    expect(ruleBody('.scheduler-card-header')).not.toMatch(/user-select:\s*none/);
  });

  it('highlights the whole card on hover, as other clickable surfaces do', () => {
    expect(ruleBody('.scheduler-card:hover')).toMatch(/background:\s*#f8f9fa/);
    expect(ruleBody('.scheduler-card:hover')).toMatch(/border-color:\s*#c7d2fe/);
  });

  it('draws the failure rail inside the card, so its content does not shift, and keeps the card shadow', () => {
    const rail = ruleBody('.scheduler-card--failed');
    expect(rail).toMatch(/box-shadow:\s*inset\s+3px\s+0\s+0\s+#dc2626/);
    expect(rail).toMatch(/0 1px 3px/);
    expect(rail).not.toMatch(/border-left/);
  });

  it('declares the failure rail after .detail-card, whose own shadow would otherwise win', () => {
    expect(css.indexOf('.scheduler-card--failed')).toBeGreaterThan(css.indexOf('.detail-card {'));
  });

  it('draws no ring on the group of controls, which only holds the place of a control that left', () => {
    expect(ruleBody('.scheduler-card-actions:focus')).toMatch(/outline:\s*none/);
  });

  it('shows a focus ring on the name', () => {
    expect(ruleBody('.schedule-name:focus-visible')).toMatch(/outline/);
  });
});

describe('styles.css split button', () => {
  it('joins the action to its chooser', () => {
    expect(ruleBody('.btn-group')).toMatch(/display:\s*inline-flex/);
  });

  it('keeps the chooser wide enough to hit, beside an action with no confirmation', () => {
    const width = /width:\s*(\d+)px/.exec(ruleBody('.btn--caret'));
    expect(Number(width?.[1])).toBeGreaterThanOrEqual(28);
  });

  it('spreads the invisible select over the chevron, so a click on it opens the native menu', () => {
    const select = ruleBody('.btn--caret > select');
    expect(select).toMatch(/position:\s*absolute/);
    expect(select).toMatch(/inset:\s*0/);
    expect(select).toMatch(/opacity:\s*0/);
  });

  it('shows the focus ring on the wrapper, since an invisible select hides its own, and the same ring on the action', () => {
    expect(ruleBody('.btn--caret:has(select:focus-visible)')).toMatch(/outline:\s*2px solid #a5b4fc/);
    expect(ruleBody('.btn-group > .btn:focus-visible')).toMatch(/outline:\s*2px solid #a5b4fc/);
  });

  it('fades a disabled chooser and stops its hover', () => {
    const disabled = ruleBody('.btn--caret:where(:has(select:disabled))');
    expect(disabled).toMatch(/opacity:\s*0\.4/);
    expect(disabled).toMatch(/pointer-events:\s*none/);
  });
});
