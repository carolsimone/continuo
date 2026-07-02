import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { ReleaseDetail, NodeValidationResult } from './types';
import { releasePillClass, groupByStage, stageLabel, proposalKey } from './release-helpers';
import { fetchProposals } from './remediation-api';

// Cadence for re-checking whether a remediation proposal has been persisted for a
// failed node. The proposal is produced asynchronously after a release is rejected
// (failure classification → LLM fix proposal), so the page polls until every failed
// node has a proposal, then stops. The cap bounds polling for failed nodes that are
// not healable and will therefore never receive a proposal.
const POLL_INTERVAL_MS = 5000;
const MAX_POLL_MS = 180000;
const MAX_POLLS = Math.floor(MAX_POLL_MS / POLL_INTERVAL_MS);

function LogView({ uri }: { uri: string }) {
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

export default function ReleaseDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [rel, setRel] = useState<ReleaseDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  // node_ids that have at least one remediation proposal for this release
  const [proposedNodeIds, setProposedNodeIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    fetch(`/api/releases/${id}`)
      .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then(setRel)
      .catch(e => setError(e.message));
  }, [id]);

  // Failed nodes are the only ones eligible for a remediation proposal. A stable
  // string key lets the polling effect re-subscribe only when that set changes.
  // Keyed on (stage, node_id) since a node_id alone is ambiguous across stages;
  // the keys themselves contain '|', so joining/splitting uses '\n'.
  const failedKeys = (rel?.per_node_results ?? [])
    .filter(n => n.status === 'failed')
    .map(n => proposalKey(n.stage, n.node_id));
  const failedKey = failedKeys.slice().sort().join('\n');

  // Poll for remediation proposals so the "Proposed fix available →" link surfaces
  // without a manual refresh. Polling runs only while a failed node still lacks a
  // proposal, stops once all are present, and is capped so unhealable failures
  // (which never produce a proposal) do not poll forever. Errors are swallowed so
  // a transient failure never breaks the page; the next tick retries.
  useEffect(() => {
    if (!id) return;
    const failed = failedKey ? failedKey.split('\n') : [];

    let cancelled = false;
    let timer: ReturnType<typeof setInterval> | undefined;
    let polls = 0;
    // Overlapping polls can resolve out of order. Apply only a response newer
    // than the last one already applied, so a slow stale response cannot
    // overwrite proposedNodeIds (and hide the link) after a newer poll has
    // already found every proposal and stopped polling.
    let issued = 0;
    let applied = 0;

    const stop = () => {
      if (timer !== undefined) {
        clearInterval(timer);
        timer = undefined;
      }
    };

    const refresh = () => {
      const seq = ++issued;
      return fetchProposals()
        .then(proposals => {
          if (cancelled || seq <= applied) return;
          applied = seq;
          const ids = new Set(
            proposals.filter(p => p.release_id === id).map(p => proposalKey(p.source, p.node_id)),
          );
          setProposedNodeIds(ids);
          if (failed.every(k => ids.has(k))) stop();
        })
        .catch(() => {});
    };

    // Always fetch once so an already-existing proposal shows immediately.
    refresh();

    // Poll only while a failed node could still receive a proposal.
    if (failed.length > 0) {
      timer = setInterval(() => {
        polls += 1;
        if (polls > MAX_POLLS) { stop(); return; }
        refresh();
      }, POLL_INTERVAL_MS);
    }

    return () => { cancelled = true; stop(); };
  }, [id, failedKey]);

  if (error) {
    return (
      <div className="page">
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{error}
        </div>
      </div>
    );
  }
  if (!rel) return <div className="page"><p className="empty">Loading…</p></div>;

  const perNode: NodeValidationResult[] = rel.per_node_results ?? [];

  return (
    <div className="page">
      <header className="page-header">
        <button type="button" className="detail-back-link" onClick={() => navigate('/?tab=releases')}>← Back</button>
        <div className="detail-page-title">{rel.release_id}</div>
        <span className={`pill ${releasePillClass(rel.status)}`}>{rel.status}</span>
      </header>

      {rel.reject_reason && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{rel.reject_reason}
        </div>
      )}

      <main className="page-content page-content--readable">
        <p>Image tags: {Object.entries(rel.image_tags || {}).map(([s, t]) => `${s}=${t}`).join(', ') || '—'}</p>

        <div className="section-header">
          <div className="section-header__main">
            <span className="section-header__title">Timeline</span>
          </div>
        </div>
        <ul>
          {rel.transitions.map((t, i) => (
            <li key={i}>{t.to} · {t.at.slice(0, 19).replace('T', ' ')}</li>
          ))}
        </ul>

        {perNode.length === 0 ? (
          <>
            <div className="section-header">
              <div className="section-header__main">
                <span className="section-header__title">Per-node results</span>
              </div>
            </div>
            <p className="empty">No per-node results.</p>
          </>
        ) : (
          groupByStage(perNode).map(({ stage, nodes }) => (
            <div key={stage}>
              <div className="section-header">
                <div className="section-header__main">
                  <span className="section-header__title">{stageLabel(stage)}</span>
                  <span className="section-header__count">{nodes.length}</span>
                </div>
              </div>
              <table className="nodes-table">
                <thead>
                  <tr><th>Node</th><th>Status</th><th>Duration</th><th>Log</th><th>Fix</th></tr>
                </thead>
                <tbody>
                  {nodes.map(n => (
                    <tr key={proposalKey(stage, n.node_id)}>
                      <td>
                        <div className="nodes-node-name">{n.node_id}</div>
                        {n.file_path && <div className="nodes-node-subpath">{n.file_path}</div>}
                      </td>
                      <td>
                        <span className={`pill-sm ${releasePillClass(n.status).replace('pill--', 'pill-sm--')}`}>
                          {n.status}
                        </span>
                      </td>
                      <td>{n.duration_ms ? `${n.duration_ms} ms` : '—'}</td>
                      <td>{n.dbt_log_uri ? <LogView uri={n.dbt_log_uri} /> : '—'}</td>
                      <td>
                        {proposedNodeIds.has(proposalKey(stage, n.node_id)) && (
                          <Link to="/?tab=remediation" className="btn btn--secondary">
                            Proposed fix available →
                          </Link>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))
        )}
      </main>
    </div>
  );
}
