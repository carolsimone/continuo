import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// The "waiting for node" pill, the status cell that holds it beside the task's
// status and its run-graph placement each have a rule; the pill reads neutral
// like a queued task, not in the warning palette of a cancel.
const css = readFileSync(fileURLToPath(new URL('../../src/client/styles.css', import.meta.url)), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '');

function declarationOf(selector: string): string {
  for (const m of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (m[1].split(',').map((s) => s.trim()).includes(selector)) return m[2].replace(/\s+/g, ' ').trim();
  }
  throw new Error(`no rule for ${selector}`);
}

describe('lease wait styles in styles.css', () => {
  it('gives the waiting pill the queued palette with a dashed border', () => {
    const waiting = declarationOf('.pill-sm--waiting');
    expect(waiting).toContain('background: #f1f5f9; color: #475569;');
    expect(waiting).toContain('border: 1px dashed #94a3b8;');
    expect(declarationOf('.pill-sm--queued')).toBe('background: #f1f5f9; color: #475569;');
  });

  it('lays out the status cell and the run-graph pill', () => {
    expect(declarationOf('.nodes-pills')).toContain('gap: 4px;');
    expect(declarationOf('.swim-wait')).toContain('transform: translate(-100%, -50%);');
  });
});
