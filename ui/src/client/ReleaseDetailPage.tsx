import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router';
import { ReleaseDetail, NodeValidationResult, ProposalDTO, VerificationRunSummary } from './types';
import {
  releasePillClass, proposalKey, reasonLabel,
  proposalNodeIds, proposalStatusForNode, proposalReasonForNode,
  proposalPullRequests, proposalPrServices, proposalPrStateForService,
  verificationPhase, verificationRunPhase, effectiveRound,
} from './release-helpers';
import { fetchProposals } from './remediation-api';
import { NodeResultsTable } from './node-results';

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
const TERMINAL_RELEASE_STATUSES = new Set(['promoted', 'rejected', 'superseded']);

// What the FIX cell can say about a failed node, ordered by how far its fix has
// progressed: still being written, then being run through a verification run to
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

// Per-node statuses that mean a fix attempt has nothing left to report: a
// fix ready for review, or one of the terminal-but-blank outcomes rendered
// as a fixNote (see refresh() below). 'generating'/'verifying' are the only
// non-terminal statuses a node can carry.
const TERMINAL_NODE_STATUSES = new Set(['proposed', 'skipped', 'failed', 'escalated']);

// proposalIsDeadEnd mirrors release-controller's isDeadEnd: a batched attempt
// opens one pull request per owning service, so it is a dead end only once
// every owning service's pull request has been rejected. An owning service
// with no pull request yet, or one in any non-rejected state, means a fix
// could still land. A legacy (unsplit) proposal has one owning service group
// (''), whose pull request is synthesized from the singular pr_* fields.
function proposalIsDeadEnd(p: ProposalDTO): boolean {
  return proposalPrServices(p).every((service) => proposalPrStateForService(p, service) === 'rejected');
}

// hasActiveFix reports whether any failed node has a generating/verifying/
// proposed fix among the given proposals — the same furthest-along ranking
// FixCell uses, restricted to whatever list is passed in (all proposals, or
// only the current remediation round's), so the dead-end check below can be
// scoped to a single round without disturbing how the table renders. A
// 'proposed' attempt every one of whose owning-service PRs was closed
// without merging is excluded: that attempt is a dead end for every node it
// resolves, not just its representative one, so it must not keep those nodes
// counted as having an active fix. A single owning service still open (or not
// yet PR'd) keeps the whole attempt counted as active.
function hasActiveFix(list: ProposalDTO[], failedKeys: string[]): boolean {
  const byKey = new Map<string, FixState>();
  for (const p of list) {
    if (p.status === 'proposed' && proposalIsDeadEnd(p)) continue;
    for (const nid of proposalNodeIds(p)) {
      const status = proposalStatusForNode(p, nid);
      if (!isFixState(status)) continue;
      const k = proposalKey(p.source, nid);
      const current = byKey.get(k);
      if (!current || FIX_STATE_RANK[status] > FIX_STATE_RANK[current]) byKey.set(k, status);
    }
  }
  return failedKeys.some(k => byKey.has(k));
}

// anyOpenPR reports whether any proposal in the list has a pull request out
// for review — checked across every round, not just the current one, since a
// fix from an earlier round is still the thing to look at instead of
// retrying. A batched attempt's pull requests are weighed per owning service
// (proposalPullRequests), so one service's still-open PR counts even when a
// sibling service's PR was rejected; a legacy (unsplit) proposal falls back
// to its singular pr_state, so behavior for those is unchanged.
function anyOpenPR(list: ProposalDTO[]): boolean {
  return list.some(p => proposalPullRequests(p).some(pr => OPEN_PR_STATES.includes(pr.pr_state ?? '')));
}

// verifyLabel picks the wording of the in-flight verification chip. A
// verification run puts the fix through the whole validation pipeline behind
// a global one-at-a-time queue, so it can sit waiting its turn before any
// work starts — which reads as "stuck" under a flat "Verifying fix…".
// Splitting the chip on the phase agent-remediation recorded for the run
// (queued vs running) lets a long wait read as "waiting its turn" instead.
function verifyLabel(phase?: 'queued' | 'running'): string {
  return phase === 'queued' ? 'Queued for verification' : 'Verifying fix…';
}

// FixCell renders one node's remediation state: a link to the proposal once a
// fix is ready to review, a non-actionable chip while one is still being
// produced or verified, and — when no attempt is in flight — a note
// explaining why the latest attempt stopped (skipped/failed/escalated), so a
// node whose fix dead-ended says why instead of showing an empty cell. For a
// fix being verified, verifyPhase splits the chip into queued vs running (see
// verifyLabel).
function FixCell({ state, verifyPhase, note }: {
  state?: FixState;
  verifyPhase?: 'queued' | 'running';
  note?: string;
}) {
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
        {state === 'verifying' ? verifyLabel(verifyPhase) : 'Generating fix…'}
      </span>
    );
  }
  if (note) return <span className="nodes-reason">{note}</span>;
  return null;
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
  // Per (stage, node_id) verification phase for a node currently shown as
  // 'verifying', read off the proposal's own verification summary
  // (verificationPhase) — whether the run judging its fix is still queued
  // behind the global one-at-a-time pipeline or already running. Populated
  // only for verifying nodes; empty for every other node.
  const [verifyPhaseByKey, setVerifyPhaseByKey] = useState<Map<string, 'queued' | 'running'>>(new Map());
  // The verification runs recorded for this release, newest first, rendered
  // in the "Verification runs" section below.
  const [verificationRuns, setVerificationRuns] = useState<VerificationRunSummary[]>([]);
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
  const currentRoundProposals = proposals.filter(p => effectiveRound(p) === rel?.remediation_round);

  // A rejected release is a dead end for remediation — nothing left for the
  // agent to do without human input — once it has at least one failed node,
  // the current round has produced at least one proposal (the window between
  // "round bumped" and "the new round's first proposal landed" is never a
  // dead end), no failed node has an in-flight/proposed fix in the current
  // round, and no proposal from any round already has a PR out for review.
  // "Try again" (below) only ever shows in this state.
  const deadEnd = rel?.status === 'rejected'
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
    // Whether the last applied response showed a fix a verification run is
    // still judging. Such a verdict is coming — the backend runs a whole
    // release-shaped pipeline to produce it, behind a global queue, and ends
    // the attempt itself if it never arrives — so the cap below, which exists
    // for failures that will never be healed at all, must not apply while one
    // is outstanding. Otherwise a healthy transition to 'proposed' lands
    // after polling stopped and the page sits on "Verifying fix…" until
    // someone reloads it.
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
          // Refresh the "Verification runs" section on the same tick as the
          // proposal poll, so a newly-started run shows up without a manual
          // reload. Best-effort: a failed fetch leaves the section showing
          // its last-known rows rather than clearing them.
          fetch(`/api/releases/${id}/verifications`)
            .then(r => (r.ok ? r.json() : { runs: [] }))
            .then(d => { if (!cancelled) setVerificationRuns(d.runs ?? []); })
            .catch(() => {});
          // Bucket each failed node by its proposals, keeping the furthest-along
          // state any of its attempts reached; terminal-but-blank statuses
          // (skipped/failed/escalated) leave the node without an entry here —
          // see fixNote below for those. A batched proposal is unpacked into
          // every node it resolves, each read against its own per-node
          // outcome rather than the proposal's overall status.
          const byKey = new Map<string, FixState>();
          // Latest attempt per node (proposal + which resolved node it was),
          // so fixNote explains why remediation stopped for that node rather
          // than why an earlier attempt, or a different node in the same
          // batched attempt, did.
          const latestByKey = new Map<string, { proposal: ProposalDTO; nodeId: string }>();
          for (const p of releaseProposals) {
            for (const nid of proposalNodeIds(p)) {
              const k = proposalKey(p.source, nid);
              const status = proposalStatusForNode(p, nid);
              if (isFixState(status)) {
                const current = byKey.get(k);
                if (!current || FIX_STATE_RANK[status] > FIX_STATE_RANK[current]) byKey.set(k, status);
              }
              const latest = latestByKey.get(k);
              if (!latest || p.attempt > latest.proposal.attempt) latestByKey.set(k, { proposal: p, nodeId: nid });
            }
          }
          setFixState(byKey);
          // Verification phase for each node whose furthest-along state is
          // 'verifying', read off the proposal's own verification summary
          // (verificationPhase) rather than a separate poll — so the verify
          // chip can split queued-vs-running. A node settled on any other
          // state carries no entry. When more than one attempt for a node is
          // still verifying, 'running' wins over 'queued'.
          const phaseByKey = new Map<string, 'queued' | 'running'>();
          for (const p of releaseProposals) {
            const phase = verificationPhase(p);
            if (!phase) continue;
            for (const nid of proposalNodeIds(p)) {
              const k = proposalKey(p.source, nid);
              if (byKey.get(k) !== 'verifying') continue;
              if (phase === 'running' || !phaseByKey.has(k)) phaseByKey.set(k, phase);
            }
          }
          setVerifyPhaseByKey(phaseByKey);
          const noteByKey = new Map<string, string>();
          for (const [k, { proposal: p, nodeId: nid }] of latestByKey) {
            const status = proposalStatusForNode(p, nid);
            const reason = proposalReasonForNode(p, nid);
            if (status === 'escalated') noteByKey.set(k, 'Attempt budget spent.');
            else if (status === 'failed') noteByKey.set(k, reason || 'The model could not produce a safe fix.');
            else if (status === 'skipped') noteByKey.set(k, `${reason || 'No source to fix at this commit.'} Fix it in the repository.`);
          }
          setFixNote(noteByKey);
          verifying = failed.some(k => byKey.get(k) === 'verifying');
          // Stop once every failed node has settled *for the release's current
          // remediation round*: its latest current-round attempt landed on
          // 'proposed', or on a terminal-but-blank outcome ('skipped'/
          // 'failed'/'escalated'). A node still 'generating'/'verifying', or
          // with no current-round proposal at all yet, is not settled and
          // keeps the poll alive (up to the cap below, suspended by
          // `verifying`). Scoping to the current round (rather than reusing
          // byKey/noteByKey above, which intentionally span every round so
          // the FIX cell keeps showing the furthest-along attempt) matters
          // right after "Try again": the round bumps before its first
          // proposal is written, and the previous round's now-stale terminal
          // outcome must not read as settled during that gap — otherwise
          // polling would stop before ever seeing the retry's fix.
          const currentRound = rel?.remediation_round;
          const currentRoundProposals = currentRound === undefined
            ? releaseProposals
            : releaseProposals.filter(p => effectiveRound(p) === currentRound);
          const latestCurrentRoundByKey = new Map<string, { proposal: ProposalDTO; nodeId: string }>();
          for (const p of currentRoundProposals) {
            for (const nid of proposalNodeIds(p)) {
              const k = proposalKey(p.source, nid);
              const latest = latestCurrentRoundByKey.get(k);
              if (!latest || p.attempt > latest.proposal.attempt) latestCurrentRoundByKey.set(k, { proposal: p, nodeId: nid });
            }
          }
          const settled = (k: string) => {
            const entry = latestCurrentRoundByKey.get(k);
            if (!entry) return false;
            return TERMINAL_NODE_STATUSES.has(proposalStatusForNode(entry.proposal, entry.nodeId));
          };
          if (failed.every(settled)) stop();
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
        <div className="action-banner">
          <div className="action-banner__body">
            <div className="action-banner__title">No fix was produced for this release</div>
            <div className="action-banner__sub">
              The last remediation attempt couldn’t open a pull request. Run another round to try again.
            </div>
          </div>
          <button type="button" className="btn btn--cta" disabled={retrying} onClick={retry}>
            <svg className="btn__ico" width="14" height="14" viewBox="0 0 24 24" fill="none"
              stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M21 12a9 9 0 1 1-2.64-6.36" />
              <path d="M21 3v6h-6" />
            </svg>
            Try again (round {rel.remediation_round} of {MAX_REMEDIATION_ROUNDS})
          </button>
        </div>
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

        <NodeResultsTable
          perNode={perNode}
          fixCell={(stage, n) => {
            const key = proposalKey(stage, n.node_id);
            const st = fixState.get(key);
            return <FixCell state={st} verifyPhase={st === 'verifying' ? verifyPhaseByKey.get(key) : undefined} note={fixNote.get(key)} />;
          }}
        />
        {verificationRuns.length > 0 && (
          <>
            <div className="section-header">
              <div className="section-header__main">
                <span className="section-header__title">Verification runs</span>
                <span className="section-header__count">{verificationRuns.length}</span>
              </div>
            </div>
            <table className="nodes-table">
              <thead><tr><th>Run</th><th>Attempt</th><th>Service</th><th>Status</th><th>Started</th></tr></thead>
              <tbody>
                {verificationRuns.map(v => {
                  const phase = verificationRunPhase(v.status);
                  return (
                    <tr key={v.run_id}>
                      <td><Link className="nodes-node-name" to={`/verifications/${v.run_id}`}>{v.run_id}</Link></td>
                      <td>{v.attempt}</td>
                      <td>{v.service}</td>
                      <td><span className={`pill-sm ${releasePillClass(phase).replace('pill--', 'pill-sm--')}`}>{phase}</span></td>
                      <td className="nodes-ts">{v.activated_at ? v.activated_at.slice(0, 19).replace('T', ' ') : '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </>
        )}
      </main>
    </div>
  );
}
