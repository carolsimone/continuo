import { useEffect, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import PageHeader from './PageHeader';
import { VerificationRunDetail } from './types';
import { reasonLabel, releasePillClass, verificationRunPhase } from './release-helpers';
import { NodeResultsTable } from './node-results';
import ServiceTiles from './ServiceTiles';
import PipelineTimeline from './PipelineTimeline';

const POLL_INTERVAL_MS = 5000;
const TERMINAL = new Set(['passed', 'failed']);

// VerificationDetailPage shows one fix-verification run: which release and
// attempt it verifies, where it is in the pipeline, and its per-node
// results and logs — the same table a release shows, without the Fix
// column, because a verification is the fix being judged.
export default function VerificationDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [run, setRun] = useState<VerificationRunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let lastGood: VerificationRunDetail | null = null;
    const tick = () => {
      fetch(`/api/verifications/${id}`)
        .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
        .then((data: VerificationRunDetail) => {
          if (cancelled) return;
          lastGood = data;
          setRun(data);
          setPollError(null);
          if (!TERMINAL.has(data.status)) timer = setTimeout(tick, POLL_INTERVAL_MS);
        })
        .catch(e => {
          if (cancelled) return;
          if (lastGood) {
            setPollError(e.message);
            if (!TERMINAL.has(lastGood.status)) timer = setTimeout(tick, POLL_INTERVAL_MS);
          } else {
            setError(e.message);
          }
        });
    };
    tick();
    return () => { cancelled = true; if (timer) clearTimeout(timer); };
  }, [id]);

  if (error) {
    return (
      <div className="page">
        <div className="info-strip info-strip--error"><span className="info-strip__icon">⚠</span>{error}</div>
      </div>
    );
  }
  if (!run) return <div className="page"><p className="empty">Loading…</p></div>;

  const phase = verificationRunPhase(run.status);
  const fmt = (ts: string) => (ts ? ts.slice(0, 19).replace('T', ' ') : '—');

  return (
    <div className="page">
      <PageHeader>
        <button type="button" className="detail-back-link" onClick={() => navigate('/?tab=remediation')}>← Back</button>
        <div className="detail-page-title">{run.run_id}</div>
        <span className={`pill ${releasePillClass(phase)}`}>{phase === 'running' ? run.status : phase}</span>
      </PageHeader>

      <p className="page-sub">
        Verification run for release <Link to={`/releases/${run.verifies_release_id}`}>{run.verifies_release_id}</Link>
        <span className="page-sub__sep">·</span>
        attempt {run.attempt}
        <span className="page-sub__sep">·</span>
        service {run.changed_service}
        <span className="page-sub__sep">·</span>
        {run.manifest_kind}
      </p>

      {run.fail_reason && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>
          {reasonLabel(run.fail_reason)}{run.fail_detail ? ` — ${run.fail_detail}` : ''}
        </div>
      )}
      {pollError && (
        <div className="info-strip info-strip--warning">
          <span className="info-strip__icon">⚠</span>Live updates temporarily unavailable — retrying…
        </div>
      )}

      <main className="page-content page-content--readable">
        <p>Submitted {fmt(run.created_at)} · started {fmt(run.activated_at)} · finished {fmt(run.finished_at)}</p>
        <ServiceTiles
          imageTags={run.image_tags || {}}
          changedService={run.changed_service}
          subject="verification run"
          carriedFrom={run.verifies_release_id}
        />
        <PipelineTimeline transitions={run.transitions} run={{ manifestKind: run.manifest_kind }} />

        <NodeResultsTable perNode={run.per_node_results ?? []} />
      </main>
    </div>
  );
}
