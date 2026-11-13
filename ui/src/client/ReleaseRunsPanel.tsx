import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { kindLabel } from './node-helpers';
import { readJson } from './read-json';
import type { ReleaseRun } from './types';

// Cadence of the re-reads below, as in the release page's own poll.
const POLL_INTERVAL_MS = 5000;

// A promotion that changes no seed starts no run, and a promotion that does
// start one only creates it once run-controller has consumed the promotion. So
// an empty list is read again for this many reads in all before it is taken to
// be final.
const MAX_EMPTY_READS = 6;

// Run statuses that will still change.
const ACTIVE_RUN_STATUSES = new Set(['pending', 'running', 'cancelling']);

// ReleaseRunsPanel lists the promote-seed runs a release's promotion started,
// each linking to its run, and draws nothing while the release has none or the
// list cannot be read. It reads the list again every POLL_INTERVAL_MS while a
// run is still active, and while the list is empty for a bounded window.
export default function ReleaseRunsPanel({ releaseId }: { releaseId: string }) {
  const [runs, setRuns] = useState<ReleaseRun[]>([]);

  useEffect(() => {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let reads = 0;
    let last: ReleaseRun[] = [];

    const settled = () => (last.length === 0
      ? reads >= MAX_EMPTY_READS
      : !last.some((r) => ACTIVE_RUN_STATUSES.has(r.status)));

    const tick = () => {
      reads += 1;
      readJson<{ runs?: ReleaseRun[] }>(
        `/api/runs?release_id=${encodeURIComponent(releaseId)}&kind=promote_seed`,
        abort.signal,
      )
        .then((d) => {
          last = d.runs ?? [];
          setRuns(last);
        })
        .catch(() => {})
        .finally(() => {
          if (!abort.signal.aborted && !settled()) timer = setTimeout(tick, POLL_INTERVAL_MS);
        });
    };
    tick();

    return () => {
      abort.abort();
      if (timer) clearTimeout(timer);
    };
  }, [releaseId]);

  if (runs.length === 0) return null;
  return (
    <>
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Seed reload runs</span>
          <span className="section-header__count">{runs.length}</span>
        </div>
      </div>
      <table className="nodes-table">
        <thead><tr><th>Status</th><th>Run</th><th>Started</th></tr></thead>
        <tbody>
          {runs.map((r) => (
            <tr key={r.run_id}>
              <td><span className={`pill-sm pill-sm--${r.status}`}>{r.status}</span></td>
              <td>
                <Link className="nodes-node-name" to={`/run/${encodeURIComponent(r.run_id)}`}>{kindLabel(r.kind)}</Link>
                {' '}<span className="nodes-ts">{r.schedule_name}</span>
              </td>
              <td className="nodes-ts">{r.created_at ? r.created_at.slice(0, 19).replace('T', ' ') : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
