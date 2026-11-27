import { useEffect, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import PageHeader from './PageHeader';
import { ReleaseCheckDetail } from './types';
import { reasonLabel, releasePillClass, releaseCheckPhase, tryLabel } from './release-helpers';
import { NodeResultsTable } from './node-results';
import ProjectTiles from './ProjectTiles';
import ReleaseCheckTimeline from './ReleaseCheckTimeline';

const POLL_INTERVAL_MS = 5000;
const TERMINAL = new Set(['passed', 'failed']);

// ReleaseCheckDetailPage shows one fix's release check: which release and
// try it verifies, which stage it has reached, and its per-node results
// and logs — the same table a release shows, without the Fix column, because
// the release check is the fix being judged.
export default function ReleaseCheckDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [check, setCheck] = useState<ReleaseCheckDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let lastGood: ReleaseCheckDetail | null = null;
    const tick = () => {
      fetch(`/api/release-checks/${id}`)
        .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
        .then((data: ReleaseCheckDetail) => {
          if (cancelled) return;
          lastGood = data;
          setCheck(data);
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
  if (!check) return <div className="page"><p className="empty">Loading…</p></div>;

  const phase = releaseCheckPhase(check.status);
  const fmt = (ts: string) => (ts ? ts.slice(0, 19).replace('T', ' ') : '—');

  return (
    <div className="page">
      <PageHeader>
        <button type="button" className="detail-back-link" onClick={() => navigate('/?tab=remediation')}>← Back</button>
        <div className="detail-page-title">{check.check_id}</div>
        <span className={`pill ${releasePillClass(phase)}`}>{phase === 'running' ? check.status : phase}</span>
      </PageHeader>

      <p className="page-sub">
        Release check for release <Link to={`/releases/${check.verifies_release_id}`}>{check.verifies_release_id}</Link>
        <span className="page-sub__sep">·</span>
        {tryLabel(check.remediation_round, check.try)}
        <span className="page-sub__sep">·</span>
        project {check.changed_project}
        <span className="page-sub__sep">·</span>
        {check.manifest_kind}
      </p>

      {check.fail_reason && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>
          {reasonLabel(check.fail_reason)}{check.fail_detail ? ` — ${check.fail_detail}` : ''}
        </div>
      )}
      {pollError && (
        <div className="info-strip info-strip--warning">
          <span className="info-strip__icon">⚠</span>Live updates temporarily unavailable — retrying…
        </div>
      )}

      <main className="page-content page-content--readable">
        <p>Submitted {fmt(check.created_at)} · started {fmt(check.activated_at)} · finished {fmt(check.finished_at)}</p>
        <ProjectTiles
          imageTags={check.image_tags || {}}
          changedProject={check.changed_project}
          subject="release check"
          carriedFrom={check.verifies_release_id}
        />
        <ReleaseCheckTimeline
          transitions={check.transitions}
          check={{
            manifestKind: check.manifest_kind,
            parse_ms: check.parse_ms,
            contracts_ms: check.contracts_ms,
            contracts_by_family: check.contracts_by_family,
          }}
        />

        <NodeResultsTable perNode={check.per_node_results ?? []} />
      </main>
    </div>
  );
}
