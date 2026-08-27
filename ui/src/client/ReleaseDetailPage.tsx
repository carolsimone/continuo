import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router';
import { ReleaseDetail, NodeValidationResult, ProposalDTO } from './types';
import { releasePillClass, groupByStage, stageLabel, proposalKey, reasonLabel } from './release-helpers';
import { fetchProposals } from './remediation-api';

// A remediation round is capped at this many attempts; the release-controller
// enforces the same limit and answers 409 rounds_exhausted past it.
const MAX_REMEDIATION_ROUNDS = 3;

// PR states that mean a fix is already out for human review — "Try again"
// stays hidden while one of these is open, since a retry would be redundant.
const OPEN_PR_STATES = ['opening', 'open', 'merged'];

// Messages for the release-controller's 409/502/500 refusal reasons, other
// than proposal_open (handled separately because it also carries a PR link).
const REFUSAL_TEXT: Record<string, string> = {
  rounds_exhausted: `Retried ${MAX_REMEDIATION_ROUNDS} times — push a new commit to start over.`,
  not_retryable: 'This release was rejected before retries existed — push a new commit.',
  not_healable: 'This rejection is not something the agent can fix.',
  not_rejected: 'Only a rejected release can be retried.',
  retry_in_progress: 'A retry is already in progress — wait for the new round to start.',
  internal: 'Retry failed on the server — try again in a moment.',
  proposal_reader_unavailable: 'The remediation service is unreachable — try again in a moment.',
};

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

// What the FIX cell can say about a failed node, ordered by how far its fix has
// progressed: still being written, then being run through a shadow release to
// see whether it holds, then ready for a human to review. A node can carry
// several attempts at once — a later one can supersede an earlier in-flight
// one — so the cell shows the furthest-along of them. Every other proposal
// status is terminal-but-blank (skipped/failed/escalated) and leaves the cell
// empty.
type FixState = 'generating' | 'verifying' | 'proposed';

const FIX_STATE_RANK: Record<FixState, number> = { generating: 1, verifying: 2, proposed: 3 };

function isFixState(status: string): status is FixState {
  return status in FIX_STATE_RANK;
}

// hasActiveFix reports whether any failed node has a generating/verifying/
// proposed fix among the given proposals — the same furthest-along ranking
// FixCell uses, restricted to whatever list is passed in (all proposals, or
// only the current remediation round's), so the dead-end check below can be
// scoped to a single round without disturbing how the table renders. A
// 'proposed' attempt whose PR was closed without merging is excluded: that PR
// is a dead end for the attempt, not something still open for a human to act
// on, so it must not keep the node counted as having an active fix.
function hasActiveFix(list: ProposalDTO[], failedKeys: string[]): boolean {
  const byKey = new Map<string, FixState>();
  for (const p of list) {
    if (!isFixState(p.status)) continue;
    if (p.status === 'proposed' && p.pr_state === 'rejected') continue;
    const k = proposalKey(p.source, p.node_id);
    const current = byKey.get(k);
    if (!current || FIX_STATE_RANK[p.status] > FIX_STATE_RANK[current]) byKey.set(k, p.status);
  }
  return failedKeys.some(k => byKey.has(k));
}

// anyOpenPR reports whether any proposal in the list has a PR out for review —
// checked across every round, not just the current one, since a fix from an
// earlier round is still the thing to look at instead of retrying.
function anyOpenPR(list: ProposalDTO[]): boolean {
  return list.some(p => OPEN_PR_STATES.includes(p.pr_state ?? ''));
}

// FixCell renders one node's remediation state: a link to the proposal once a
// fix is ready to review, a non-actionable chip while one is still being
// produced or verified, and — when no attempt is in flight — a note
// explaining why the latest attempt stopped (skipped/failed/escalated), so a
// node whose fix dead-ended says why instead of showing an empty cell.
function FixCell({ state, note }: { state?: FixState; note?: string }) {
  if (state === 'proposed') {
    return (
      <Link to="/?tab=remediation" className="btn btn--secondary">
        Proposed fix available →
      </Link>
    );
  }
  if (state) {
    return (
      <span className="btn btn--secondary is-disabled" aria-disabled="true" aria-busy="true">
        {state === 'verifying' ? 'Verifying fix…' : 'Generating fix…'}
      </span>
    );
  }
  if (note) return <span className="nodes-reason">{note}</span>;
  return null;
}

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
  // proposals — see FixState. Nodes with only terminal-but-blank outcomes
  // (skipped/failed/escalated) or none carry no entry and render nothing here.
  const [fixState, setFixState] = useState<Map<string, FixState>>(new Map());
  // Per (stage, node_id) explanation for a node whose latest attempt landed on
  // a terminal-but-blank status, so the FIX cell says why instead of leaving
  // that cell empty. Only ever read where fixState has no entry for the key.
  const [fixNote, setFixNote] = useState<Map<string, string>>(new Map());
  // The last polled proposal list for this release, kept alongside fixState so
  // the dead-end check below can recompute in-flight/PR state per remediation
  // round (fixState itself is a rendering-only summary across every round).
  const [proposals, setProposals] = useState<ProposalDTO[]>([]);
  const [retrying, setRetrying] = useState(false);
  const [retryError, setRetryError] = useState<string | null>(null);
  // Bumped by retry() to re-arm the proposal-polling effect below after a
  // successful retry, without changing the failed-node set it is also keyed
  // on (a retry does not change which nodes failed).
  const [pollNonce, setPollNonce] = useState(0);

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

  // This remediation round's proposals — a proposal with no remediation_round
  // (older rows, or a round-1 attempt) counts as round 1. Scoping the
  // in-flight check to this set is what keeps a freshly-bumped round from
  // reading as a dead end off the *previous* round's now-terminal proposals.
  const currentRoundProposals = proposals.filter(p => (p.remediation_round ?? 1) === rel?.remediation_round);

  // A rejected release is a dead end for remediation — nothing left for the
  // agent to do without human input — once it has at least one failed node,
  // the current round has produced at least one proposal (the window between
  // "round bumped" and "the new round's first proposal landed" is never a
  // dead end), no failed node has an in-flight/proposed fix in the current
  // round, and no proposal from any round already has a PR out for review.
  // A shadow release is never a dead end — it verifies a fix, it does not
  // start one. "Try again" (below) only ever shows in this state.
  const deadEnd = rel?.status === 'rejected'
    && !rel.shadow
    && failedKeys.length > 0
    && currentRoundProposals.length > 0
    && !hasActiveFix(currentRoundProposals, failedKeys)
    && !anyOpenPR(proposals);

  // Poll for remediation proposals so the FIX cell surfaces the "Generating fix…"
  // chip and then the "Proposed fix available →" link without a manual refresh.
  // Polling runs only while a failed node has not yet reached a ready proposal,
  // stops once every failed node has one, and is capped so unhealable failures
  // (which never produce a ready proposal) do not poll forever. The cap is
  // suspended while any failed node's fix is 'verifying': that verdict comes
  // from a whole release the backend runs behind a global queue, which routinely
  // outlasts the cap, and it always arrives — the backend ends the attempt
  // itself if the release never answers. Errors are swallowed so a transient
  // failure never breaks the page; the next tick retries.
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
    // Whether the last applied response showed a fix a shadow release is still
    // judging. Such a verdict is coming — the backend runs a whole release to
    // produce it, behind a global release queue, and ends the attempt itself if
    // it never arrives — so the cap below, which exists for failures that will
    // never be healed at all, must not apply while one is outstanding.
    // Otherwise a healthy transition to 'proposed' lands after polling stopped
    // and the page sits on "Verifying fix…" until someone reloads it.
    let verifying = false;

    const stop = () => {
      if (timer !== undefined) {
        clearInterval(timer);
        timer = undefined;
      }
    };

    const refresh = () => {
      const seq = ++issued;
      return fetchProposals()
        .then(fetched => {
          if (cancelled || seq <= applied) return;
          applied = seq;
          const releaseProposals = fetched.filter(p => p.release_id === id);
          setProposals(releaseProposals);
          // Bucket each failed node by its proposals, keeping the furthest-along
          // state any of its attempts reached; terminal-but-blank statuses
          // (skipped/failed/escalated) leave the node without an entry here —
          // see fixNote below for those.
          const byKey = new Map<string, FixState>();
          // Latest attempt per node, so fixNote explains why remediation
          // stopped rather than why an earlier attempt did.
          const latestByKey = new Map<string, ProposalDTO>();
          for (const p of releaseProposals) {
            const k = proposalKey(p.source, p.node_id);
            if (isFixState(p.status)) {
              const current = byKey.get(k);
              if (!current || FIX_STATE_RANK[p.status] > FIX_STATE_RANK[current]) byKey.set(k, p.status);
            }
            const latest = latestByKey.get(k);
            if (!latest || p.attempt > latest.attempt) latestByKey.set(k, p);
          }
          setFixState(byKey);
          const noteByKey = new Map<string, string>();
          for (const [k, p] of latestByKey) {
            if (p.status === 'escalated') noteByKey.set(k, 'Attempt budget spent.');
            else if (p.status === 'failed') noteByKey.set(k, p.rationale || 'The model could not produce a safe fix.');
            else if (p.status === 'skipped') noteByKey.set(k, `${p.rationale || 'No source to fix at this commit.'} Fix it in the repository.`);
          }
          setFixNote(noteByKey);
          verifying = failed.some(k => byKey.get(k) === 'verifying');
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
        if (polls > MAX_POLLS && !verifying) { stop(); return; }
        refresh();
      }, POLL_INTERVAL_MS);
    }

    return () => { cancelled = true; stop(); };
  }, [id, isRejected, failedKey, pollNonce]);

  // restartProposalPolling re-arms the effect above after a successful retry,
  // so a fresh remediation round's proposals are picked up without a reload.
  const restartProposalPolling = () => setPollNonce(n => n + 1);

  // retry asks release-controller to start another remediation round. A 202
  // advances the round shown in the header and restarts proposal polling; a
  // 409 means the request was refused, and the reason is turned into the text
  // shown under the button.
  const retry = async () => {
    setRetrying(true);
    setRetryError(null);
    try {
      const r = await fetch(`/api/releases/${id}/retry-remediation`, { method: 'POST' });
      const body = await r.json().catch(() => ({}));
      if (r.status === 202) {
        setRel(prev => (prev ? { ...prev, remediation_round: body.remediation_round } : prev));
        restartProposalPolling();
        return;
      }
      if (body.error === 'proposal_open') {
        setRetryError(
          body.pr_url
            ? `A fix is already proposed: ${body.pr_url}`
            : 'A fix is already proposed — review it on the Remediation tab.',
        );
        return;
      }
      setRetryError(REFUSAL_TEXT[body.error] ?? `Retry refused (${body.error ?? r.status}).`);
    } catch (e: any) {
      setRetryError(e.message);
    } finally {
      setRetrying(false);
    }
  };

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
        <span className={`pill ${releasePillClass(rel.status)}`}>
          {rel.status}{rel.remediation_round > 1 ? ` · round ${rel.remediation_round}` : ''}
        </span>
      </header>

      {rel.reject_reason && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>
          {reasonLabel(rel.reject_reason)}
          {rel.reject_detail ? ` — ${rel.reject_detail}` : ''}
        </div>
      )}

      {rel.status === 'rejected' && deadEnd && rel.remediation_round < MAX_REMEDIATION_ROUNDS && (
        <button type="button" className="btn btn--primary" disabled={retrying} onClick={retry}>
          Try again (round {rel.remediation_round} of {MAX_REMEDIATION_ROUNDS})
        </button>
      )}
      {rel.status === 'rejected' && deadEnd && rel.remediation_round >= MAX_REMEDIATION_ROUNDS && (
        <div className="info-strip info-strip--neutral">
          <span className="info-strip__icon">ⓘ</span>
          Retried {MAX_REMEDIATION_ROUNDS} times — push a new commit to start over.
        </div>
      )}
      {retryError && (
        <div className="info-strip info-strip--error" role="alert">
          <span className="info-strip__icon">⚠</span>{retryError}
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
                        <FixCell
                          state={fixState.get(proposalKey(stage, n.node_id))}
                          note={fixNote.get(proposalKey(stage, n.node_id))}
                        />
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
