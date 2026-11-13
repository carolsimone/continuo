import type { ReleaseNotice } from './types';

const plural = (n: number, one: string, many: string) => (n === 1 ? one : many);

// columnChange words what the release does to the seed's columns, or says the
// seed only reloads new rows.
function columnChange(n: ReleaseNotice): string {
  if (n.change === 'rows') return 'reloads new rows';
  const { added, removed, retyped } = n.columns;
  const parts: string[] = [];
  if (added.length > 0) parts.push(`adds the ${plural(added.length, 'column', 'columns')} ${added.join(', ')}`);
  if (removed.length > 0) parts.push(`removes the ${plural(removed.length, 'column', 'columns')} ${removed.join(', ')}`);
  if (retyped.length > 0) parts.push(`changes the type of the ${plural(retyped.length, 'column', 'columns')} ${retyped.join(', ')}`);
  return parts.length > 0 ? parts.join(' and ') : 'changes its columns';
}

// noticeText is the whole warning for one seed reload: the change, what the
// load does and the views it rebuilds, the nodes that refresh on their own
// schedule, the objects outside continuo that read the seed, what the reload
// is predicted to do, and what is not known about it.
function noticeText(n: ReleaseNotice, warehouseCheckExpected: boolean): string {
  const rebuilds = n.rebuilds.length > 0
    ? ` and, in the same run, rebuilds the ${n.rebuilds.length} ${plural(n.rebuilds.length, 'view that reads', 'views that read')} it: ${n.rebuilds.map((r) => r.node_id).join(', ')}`
    : '';
  const sentences = [`Seed reload: ${n.node_id} ${columnChange(n)} (${n.load.reason})${rebuilds}.`];

  for (const r of n.refresh_on_schedule) {
    sentences.push(`${r.node_id} (${r.materialized}) refreshes on its own schedule, ${r.schedule}.`);
  }

  const outside = n.warehouse.outside_continuo;
  if (outside.length > 0) {
    const objects = outside.map((o) => `${o.object} (${o.kind}, owner ${o.owner})`).join(', ');
    sentences.push(`${outside.length} ${plural(outside.length, 'object outside continuo reads', 'objects outside continuo read')} the seed: ${objects}.`);
    if (n.engine_family === 'late_binding') sentences.push('They may go stale; recreate them after the reload.');
  }

  if (n.prediction === 'stops') {
    sentences.push('The reload will stop before the drop until an operator names them in a full refresh of the seed, or removes them and runs the seed again.');
  } else if (n.prediction === 'refused') {
    sentences.push(`The reload is refused: ${n.stop_reason}.`);
  }

  if (n.unvalidated) sentences.push('unvalidated: the release skipped validation.');
  if (warehouseCheckExpected && !n.warehouse.checked) sentences.push('The warehouse was not checked.');
  return sentences.join(' ');
}

// SeedReloadNotices draws one warning strip per seed reload notice of a
// release, and nothing for a release with none. seedBuildRan says the
// release's seed-build stage has finished, the point from which an unchecked
// warehouse is a fact worth stating rather than a stage still to come.
export default function SeedReloadNotices({ notices, seedBuildRan }: {
  notices: ReleaseNotice[] | undefined;
  seedBuildRan: boolean;
}) {
  return (
    <>
      {(notices ?? []).map((n) => (
        <div key={n.node_id} className="info-strip info-strip--warning">
          <span className="info-strip__icon">⚠</span>
          <span>{noticeText(n, seedBuildRan)}</span>
        </div>
      ))}
    </>
  );
}
