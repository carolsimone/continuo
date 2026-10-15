import { Fragment, useState } from 'react';
import type { KeyboardEvent } from 'react';

// PreviewItem is one row of a read-only preview list: an identifier shown in
// the monospace identifier style (title, when set, is the full id shown on
// hover), the service whose accent dot it carries, and its muted facts. A
// fact that is not known is left out of facts, never passed as an empty label.
export interface PreviewItem {
  key: string;
  service: string;
  label: string;
  title?: string;
  facts: string[];
}

// Past this many rows a preview list groups its rows under collapsible
// per-service headers.
export const PREVIEW_GROUP_ABOVE = 12;

// Accent of a service the caller's palette does not hold: a neutral slate,
// never a status hue.
const UNKNOWN_ACCENT = '#94a3b8';

interface FactsProps {
  facts: string[];
  summary?: boolean;
}

// PreviewFacts renders facts on one muted line, separated by a middle dot.
// summary marks the line that breaks down a section header's total. No facts
// render nothing.
export function PreviewFacts({ facts, summary = false }: FactsProps) {
  if (facts.length === 0) return null;
  return (
    <span className={summary ? 'preview-facts preview-facts--summary' : 'preview-facts'}>
      {facts.map((fact, i) => (
        <Fragment key={fact}>
          {i > 0 && <span className="preview-facts__sep" aria-hidden="true">·</span>}
          <span>{fact}</span>
        </Fragment>
      ))}
    </span>
  );
}

function PreviewRow({ item, accent }: { item: PreviewItem; accent: string }) {
  return (
    <li className="preview-row">
      <span className="preview-row__main">
        <span className="nodes-group-dot" style={{ background: accent }} aria-hidden="true" />
        <span className="preview-row__id" title={item.title ?? item.label}>{item.label}</span>
      </span>
      <PreviewFacts facts={item.facts} />
    </li>
  );
}

interface Props {
  items: PreviewItem[];
  colors: Map<string, string>;
  groupAbove?: number;
}

// PreviewList lists items inside a modal without offering them as choices:
// rows are not buttons, and the list scrolls inside its own bounded height.
// Up to groupAbove rows it lists them flat in the order given; past that it
// groups them by service, sorted by name, under collapsible headers that
// start collapsed.
export default function PreviewList({ items, colors, groupAbove = PREVIEW_GROUP_ABOVE }: Props) {
  const [open, setOpen] = useState<Set<string>>(new Set());
  const accent = (service: string) => colors.get(service) ?? UNKNOWN_ACCENT;

  if (items.length <= groupAbove) {
    return (
      <ul className="preview-list">
        {items.map(item => <PreviewRow key={item.key} item={item} accent={accent(item.service)} />)}
      </ul>
    );
  }

  const groups = new Map<string, PreviewItem[]>();
  for (const item of items) {
    const rows = groups.get(item.service);
    if (rows) rows.push(item);
    else groups.set(item.service, [item]);
  }
  const services = [...groups.keys()].sort();
  const toggle = (service: string) => setOpen(prev => {
    const next = new Set(prev);
    if (next.has(service)) next.delete(service);
    else next.add(service);
    return next;
  });

  return (
    <ul className="preview-list">
      {services.map(service => {
        const rows = groups.get(service) ?? [];
        const isOpen = open.has(service);
        return (
          <li key={service} className="preview-group">
            <div
              className="nodes-group-header preview-group__header"
              role="button"
              tabIndex={0}
              aria-expanded={isOpen}
              onClick={() => toggle(service)}
              onKeyDown={(e: KeyboardEvent) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  toggle(service);
                }
              }}
            >
              <span className="nodes-group-chevron" aria-hidden="true">{isOpen ? '▾' : '▸'}</span>
              <span className="nodes-group-dot" style={{ background: accent(service) }} aria-hidden="true" />
              <span className="nodes-group-name">{service}</span>
              <span className="nodes-group-count">{rows.length}</span>
            </div>
            {isOpen && (
              <ul className="preview-group__rows">
                {rows.map(item => <PreviewRow key={item.key} item={item} accent={accent(item.service)} />)}
              </ul>
            )}
          </li>
        );
      })}
    </ul>
  );
}
