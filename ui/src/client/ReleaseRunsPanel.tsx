import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { kindLabel } from './node-helpers';
import { readJson } from './read-json';
import type { ReleaseRun } from './types';

// ReleaseRunsPanel lists the promote-seed runs a release's promotion started,
// each linking to its run, and draws nothing while the release has none or the
// list cannot be read.
export default function ReleaseRunsPanel({ releaseId }: { releaseId: string }) {
  const [runs, setRuns] = useState<ReleaseRun[]>([]);

  useEffect(() => {
    const abort = new AbortController();
    readJson<{ runs?: ReleaseRun[] }>(
      `/api/runs?release_id=${encodeURIComponent(releaseId)}&kind=promote_seed`,
      abort.signal,
    )
      .then((d) => setRuns(d.runs ?? []))
      .catch(() => {});
    return () => abort.abort();
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
