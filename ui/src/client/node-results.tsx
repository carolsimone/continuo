import { ReactNode, useState } from 'react';
import { NodeValidationResult } from './types';
import { groupByStage, proposalKey, releasePillClass, stageLabel } from './release-helpers';

// LogView renders a node's dbt log: a toggle that fetches and shows the log
// inline on first open, and a link to the full log in a new tab. Shared by
// the release detail page and the verification run detail page — both show
// the same per-node results table.
export function LogView({ uri }: { uri: string }) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const logUrl = `/api/releases/log?key=${encodeURIComponent(uri)}`;

  const toggle = () => {
    if (open) { setOpen(false); return; }
    setOpen(true);
    if (content === null) {
      setErr(null);
      fetch(logUrl)
        .then(r => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
        .then(setContent)
        .catch(e => setErr(e.message));
    }
  };

  return (
    <>
      <button type="button" className="btn btn--secondary" onClick={toggle}>{open ? 'hide' : 'view'}</button>{' '}
      <a className="btn btn--secondary" href={logUrl} target="_blank" rel="noreferrer">open full log ↗</a>
      {open && (err
        ? <div className="info-strip info-strip--error"><span className="info-strip__icon">⚠</span>{err}</div>
        : <pre className="log-block">{content ?? 'loading…'}</pre>)}
    </>
  );
}

// NodeResultsTable renders a run's per-node results grouped by pipeline
// stage: the same table for a candidate release and a fix-verification run.
// fixCell, when given, adds the Fix column the release page fills with each
// node's remediation state; a verification run has no fixes of its own, so
// its page omits it.
export function NodeResultsTable({ perNode, fixCell }: {
  perNode: NodeValidationResult[];
  fixCell?: (stage: string, node: NodeValidationResult) => ReactNode;
}) {
  if (perNode.length === 0) {
    return (
      <>
        <div className="section-header">
          <div className="section-header__main">
            <span className="section-header__title">Per-node results</span>
          </div>
        </div>
        <p className="empty">No per-node results.</p>
      </>
    );
  }
  return (
    <>
      {groupByStage(perNode).map(({ stage, nodes }) => (
        <div key={stage}>
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">{stageLabel(stage)}</span>
              <span className="section-header__count">{nodes.length}</span>
            </div>
          </div>
          <table className="nodes-table">
            <thead>
              <tr><th>Node</th><th>Status</th><th>Duration</th><th>Log</th>{fixCell && <th>Fix</th>}</tr>
            </thead>
            <tbody>
              {nodes.map(n => (
                <tr key={proposalKey(stage, n.node_id)}>
                  <td>
                    <div className="nodes-node-name">{n.node_id}</div>
                    {n.file_path && <div className="nodes-node-subpath">{n.file_path}</div>}
                  </td>
                  <td>
                    <span className={`pill-sm ${releasePillClass(n.status).replace('pill--', 'pill-sm--')}`}>{n.status}</span>
                  </td>
                  <td>{n.duration_ms ? `${n.duration_ms} ms` : '—'}</td>
                  <td>{n.dbt_log_uri ? <LogView uri={n.dbt_log_uri} /> : '—'}</td>
                  {fixCell && <td>{fixCell(stage, n)}</td>}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </>
  );
}
