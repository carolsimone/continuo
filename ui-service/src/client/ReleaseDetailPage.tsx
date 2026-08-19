import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router';
import { ReleaseDetail, NodeValidationResult } from './types';
import { releasePillClass, groupByStage, stageLabel, proposalKey, reasonLabel } from './release-helpers';
import { fetchProposals } from './remediation-api';

// Cadence for re-checking whether a remediation proposal has been persisted for a
// failed node. The proposal is produced asynchronously after a release is rejected
// (failure classification → LLM fix proposal), so the page polls until every failed
// node has a proposal, then stops. The cap bounds polling for failed nodes that are
// not healable and will therefore never receive a proposal.
const POLL_INTERVAL_MS = 5000;
const MAX_POLL_MS = 180000;
const MAX_POLLS = Math.floor(MAX_POLL_MS / POLL_INTERVAL_MS);

// Release statuses that will never change again. Per-node validation results land
// incrementally while the release is still progressing (e.g. 'validating'), so the
// detail page polls until the release reaches one of these, then stops.
const TERMINAL_RELEASE_STATUSES = new Set(['promoted', 'validated', 'rejected', 'superseded']);

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
  // Set only when a poll fails after we already have last-good data (as opposed
  // to the initial load, which uses `error` above). Non-blocking: the page keeps
  // showing `rel` and this just surfaces that a retry is pending.
  const [pollError, setPollError] = useState<string | null>(null);
  // Per (stage, node_id) FIX-cell state, bucketed from the node's remediation
  // proposals: 'proposed' when a fix is ready to review, 'generating' while a fix
  // is still in flight. Nodes with only terminal-but-blank outcomes
  // (skipped/failed/escalated) or none carry no entry and render nothing.
  const [fixState, setFixState] = useState<Map<string, 'proposed' | 'generating'>>(new Map());

  // Poll the release detail while it is still progressing, so per-node validation
  // results (projected incrementally by the backend) render live without a manual
  // refresh. Self-schedules the next fetch only while non-terminal, rather than a
  // fixed interval, so it stops as soon as a terminal status is observed.
  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // Local to this effect run, updated only by its own successful ticks, so a
    // catch handler can tell an initial-load failure (no data yet) apart from a
    // mid-poll failure (we already have something to keep showing) without
    // racing the async setRel/setPollError state updates.
    let lastGood: ReleaseDetail | null = null;

    const tick = () => {
      fetch(`/api/releases/${id}`)
        .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
        .then((data: ReleaseDetail) => {
          if (cancelled) return;
          lastGood = data;
          setRel(data);
          setPollError(null);
          if (!TERMINAL_RELEASE_STATUSES.has(data.status)) timer = setTimeout(tick, POLL_INTERVAL_MS);
        })
        .catch(e => {
          if (cancelled) return;
          if (lastGood) {
            // Transient failure after a successful load: keep showing the
            // last-good view, surface a non-blocking indicator, and keep
            // retrying rather than stopping live updates.
            setPollError(e.message);
            if (!TERMINAL_RELEASE_STATUSES.has(lastGood.status)) timer = setTimeout(tick, POLL_INTERVAL_MS);
          } else {
            setError(e.message);
          }
        });
    };
    tick();

    return () => { cancelled = true; if (timer) clearTimeout(timer); };
  }, [id]);

  // Failed nodes are the only ones eligible for a remediation proposal. A stable
  // string key lets the polling effect re-subscribe only when that set changes.
  // Keyed on (stage, node_id) since a node_id alone is ambiguous across stages;
  // the keys themselves contain '|', so joining/splitting uses '\n'.
  const failedKeys = (rel?.per_node_results ?? [])
    .filter(n => n.status === 'failed')
    .map(n => proposalKey(n.stage, n.node_id));
  const failedKey = failedKeys.slice().sort().join('\n');

  // Proposals are produced only after a release is rejected (failure
  // classification → LLM fix proposal runs off release.rejected:v1), so a node
  // can already appear 'failed' in per_node_results while the release is still
  // 'validating' (live updates land mid-run) without any proposal existing yet.
  // Gating on rejected — and keying the effect on it — means the poll starts
  // fresh exactly at the live transition to rejected, instead of having already
  // started (and possibly capped out via MAX_POLLS) before there was anything
  // to find.
  const isRejected = rel?.status === 'rejected';

  // Poll for remediation proposals so the FIX cell surfaces the "Generating fix…"
  // chip and then the "Proposed fix available →" link without a manual refresh.
  // Polling runs only while a failed node has not yet reached a ready proposal,
  // stops once every failed node has one, and is capped so unhealable failures
  // (which never produce a ready proposal) do not poll forever. Errors are
  // swallowed so a transient failure never breaks the page; the next tick retries.
  useEffect(() => {
    if (!id || !isRejected) return;
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
          // Bucket each failed node by its proposals. A ready 'proposed' fix
          // dominates an in-flight 'generating' one (a later attempt can supersede
          // an earlier in-flight row); terminal-but-blank statuses
          // (skipped/failed/escalated) leave the node without an entry.
          const byKey = new Map<string, 'proposed' | 'generating'>();
          for (const p of proposals.filter(p => p.release_id === id)) {
            const k = proposalKey(p.source, p.node_id);
            if (p.status === 'proposed') byKey.set(k, 'proposed');
            else if (p.status === 'generating' && byKey.get(k) !== 'proposed') byKey.set(k, 'generating');
          }
          setFixState(byKey);
          // Stop only when every failed node has a ready fix; keep polling through
          // the generating→proposed swap and until the cap for unhealable nodes.
          if (failed.every(k => byKey.get(k) === 'proposed')) stop();
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
  }, [id, isRejected, failedKey]);

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
        {rel.shadow && (
          <span className="pill-sm pill-sm--verification">fix verification run</span>
        )}
        <span className={`pill ${releasePillClass(rel.status)}`}>{rel.status}</span>
      </header>

      {rel.reject_reason && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>
          {reasonLabel(rel.reject_reason)}
          {rel.reject_detail ? ` — ${rel.reject_detail}` : ''}
        </div>
      )}

      {pollError && (
        <div className="info-strip info-strip--warning">
          <span className="info-strip__icon">⚠</span>Live updates temporarily unavailable — retrying…
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
                        {fixState.get(proposalKey(stage, n.node_id)) === 'proposed' ? (
                          <Link to="/?tab=remediation" className="btn btn--secondary">
                            Proposed fix available →
                          </Link>
                        ) : fixState.get(proposalKey(stage, n.node_id)) === 'generating' ? (
                          <span
                            className="btn btn--secondary is-disabled"
                            aria-disabled="true"
                            aria-busy="true"
                          >
                            Generating fix…
                          </span>
                        ) : null}
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
