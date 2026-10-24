import type { CSSProperties } from 'react';
import { leaseWaitTitle, runPagePath, type LeaseWait } from './detail-page-helpers';

// LeaseWaitPill marks a pending task that waits for another run to finish
// writing its node. While a run holds the node the pill links that run's page;
// while no run holds it (the task is about to be granted the node) it is plain
// text. A click on the link opens the run without reaching the row or node
// beneath it. style positions the pill where a caller lays elements out by
// coordinates (the run graph); it carries no visual styling.
export default function LeaseWaitPill({ wait, className, style }: {
  wait: LeaseWait;
  className?: string;
  style?: CSSProperties;
}) {
  const cls = ['pill-sm', 'pill-sm--waiting', className].filter(Boolean).join(' ');
  const title = leaseWaitTitle(wait);
  if (!wait.holderRunId) {
    return <span className={cls} title={title} style={style}>waiting for node</span>;
  }
  return (
    <a
      className={cls}
      href={runPagePath(wait.holderRunId)}
      title={title}
      style={style}
      onClick={e => e.stopPropagation()}
      onKeyDown={e => e.stopPropagation()}
    >
      waiting for node
    </a>
  );
}
