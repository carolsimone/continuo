import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router';
import PageHeader from './PageHeader';
import { ApproverDTO, ReleaseDetail, NodeValidationResult, FixDTO, ReleaseCheckSummary } from './types';
import {
  releasePillClass, fixKey, reasonLabel,
  fixNodeIds, fixStatusForNode, fixReasonForNode, fixCodeForNode, fixApproverForNode,
  fixPullRequests, fixPrProjects, fixPrStateForProject,
  tryReleaseCheckPhase, releaseCheckPhase, effectiveRound, shortSha, tryBefore, tryLabel,
} from './release-helpers';
import ErrorCell from './ErrorCell';
import { HANDOVER_TEXT } from './handover-text';
import { fetchFixes } from './remediation-api';
import { NodeResultsTable } from './node-results';
import ProjectTiles from './ProjectTiles';
import ReleaseCheckTimeline from './ReleaseCheckTimeline';
import SeedReloadNotices from './SeedReloadNotices';
import ReleaseRunsPanel from './ReleaseRunsPanel';

// A release gets at most this many remediation rounds; the release-controller
// enforces the same limit and answers 409 rounds_exhausted past it.
const MAX_REMEDIATION_ROUNDS = 3;

// PR states that mean a fix is already out for human review — "Try again"
// stays hidden while one of these is open, since a retry would be redundant.
const OPEN_PR_STATES = ['opening', 'open', 'merged'];

// Messages for the release-controller's 409/502/500 refusal reasons, other
// than fix_open (handled separately because it also carries a PR link).
const REFUSAL_TEXT: Record<string, string> = {
  rounds_exhausted: `Retried ${MAX_REMEDIATION_ROUNDS} times — push a new commit to start over.`,
  not_retryable: 'This release was rejected before retries existed — push a new commit.',
  not_healable: 'This rejection is not something the agent can fix.',
  not_rejected: 'Only a rejected release can be retried.',
  retry_in_progress: 'A retry is already in progress — wait for the new round to start.',
  internal: 'Retry failed on the server — try again in a moment.',
  fix_reader_unavailable: 'agent-remediation is unreachable — try again in a moment.',
};

// Cadence for re-checking whether a remediation fix has been persisted for a
// failed node. The fix is produced asynchronously after a release is rejected
// (failure classification → LLM fix), so the page polls until every failed
// node has a fix, then stops. The cap bounds polling for failed nodes that are
// not healable and will therefore never receive a fix.
const POLL_INTERVAL_MS = 5000;
const MAX_POLL_MS = 180000;
const MAX_POLLS = Math.floor(MAX_POLL_MS / POLL_INTERVAL_MS);

// Release statuses that will never change again. Per-node validation results land
// incrementally while the release is still progressing (e.g. 'validating'), so the
// detail page polls until the release reaches one of these, then stops.
const TERMINAL_RELEASE_STATUSES = new Set(['promoted', 'rejected', 'superseded']);

// What the FIX cell can say about a failed node, ordered by how far its fix has
// progressed: still being written, then being run through a release check to
// see whether it holds, then ready for a human to review. A node can carry
// several tries at once — a later one can supersede an earlier in-flight
// one — so the cell shows the furthest-along of them. Every other fix
// status is terminal-but-blank (skipped/failed/escalated) and leaves the cell
// empty.
type FixState = 'generating' | 'verifying' | 'proposed';

const FIX_STATE_RANK: Record<FixState, number> = { generating: 1, verifying: 2, proposed: 3 };

function isFixState(status: string): status is FixState {
  return status in FIX_STATE_RANK;
}

// Per-node statuses that mean a try has nothing left to report: a
// fix ready for review, or one of the terminal-but-blank outcomes rendered
// as a fixNote (see refresh() below). 'generating'/'verifying' are the only
// non-terminal statuses a node can carry.
const TERMINAL_NODE_STATUSES = new Set(['proposed', 'skipped', 'failed', 'escalated']);

// fixIsDeadEnd mirrors release-controller's isDeadEnd: a batched try
// opens one pull request per owning project, so it is a dead end only once
// every owning project's pull request has been rejected. An owning project
// with no pull request yet, or one in any non-rejected state, means a fix
// could still land. A legacy (unsplit) fix has one owning project group
// (''), whose pull request is synthesized from the singular pr_* fields.
function fixIsDeadEnd(p: FixDTO): boolean {
  return fixPrProjects(p).every((project) => fixPrStateForProject(p, project) === 'rejected');
}

// hasActiveFix reports whether any failed node has a generating/verifying/
// proposed fix among the given fixes — the same furthest-along ranking
// FixCell uses, restricted to whatever list is passed in (all fixes, or
// only the current remediation round's), so the dead-end check below can be
// scoped to a single round without disturbing how the table renders. A
// 'proposed' try every one of whose owning-project PRs was closed
// without merging is excluded: that try is a dead end for every node it
// resolves, not just its representative one, so it must not keep those nodes
// counted as having an active fix. A single owning project still open (or not
// yet PR'd) keeps the whole try counted as active.
function hasActiveFix(list: FixDTO[], failedKeys: string[]): boolean {
  const byKey = new Map<string, FixState>();
  for (const p of list) {
    if (p.status === 'proposed' && fixIsDeadEnd(p)) continue;
    for (const nid of fixNodeIds(p)) {
      const status = fixStatusForNode(p, nid);
      if (!isFixState(status)) continue;
      const k = fixKey(p.source, nid);
      const current = byKey.get(k);
      if (!current || FIX_STATE_RANK[status] > FIX_STATE_RANK[current]) byKey.set(k, status);
    }
  }
  return failedKeys.some(k => byKey.has(k));
}

// anyOpenPR reports whether any fix in the list has a pull request out
// for review — checked across every round, not just the current one, since a
// fix from an earlier round is still the thing to look at instead of
// retrying. A batched try's pull requests are weighed per owning project
// (fixPullRequests), so one project's still-open PR counts even when a
// sibling project's PR was rejected; a legacy (unsplit) fix falls back
// to its singular pr_state, so behavior for those is unchanged.
function anyOpenPR(list: FixDTO[]): boolean {
  return list.some(p => fixPullRequests(p).some(pr => OPEN_PR_STATES.includes(pr.pr_state ?? '')));
}

// verifyLabel picks the wording of the chip of a fix in 'verifying'. A fix's
// release check puts it through every stage a release passes, behind a
// global one-at-a-time release queue, so it can sit waiting its turn before
// any work starts — which reads as "stuck" under a flat "Checking the fix…".
// Splitting the chip on the phase agent-remediation recorded for the release
// check (queued vs running) lets a long wait read as "waiting its turn"
// instead.
function verifyLabel(phase?: 'queued' | 'running'): string {
  return phase === 'queued' ? 'Queued for its release check' : 'Checking the fix…';
}

// FixNote explains why remediation stopped for one node. text is the short
// sentence shown in full; message, when present, is the node's own (possibly
// long) error text, which the cell truncates behind a `more` toggle.
interface FixNote {
  text: string;
  message?: string;
}

// OwnerNote says who owns a node a fix was proposed for or that was handed to a
// human, and so whom the pull request asks to review. An empty owner is a node
// with no owner, for which nobody is asked.
function OwnerNote({ approver }: { approver: ApproverDTO }) {
  const owner = approver.owner.trim();
  return (
    <div className="nodes-reason nodes-owner">
      {owner ? `Owner: ${owner}` : 'No owner, so no reviewer is asked.'}
    </div>
  );
}

// FixCell renders one node's remediation state: a link to the fix once a
// fix is ready to review, a non-actionable chip while one is still being
// produced or verified, and — when no try is in flight — a note
// explaining why the latest try stopped (skipped/failed/escalated), so a
// node whose fix dead-ended says why instead of showing an empty cell. For a
// fix being verified, verifyPhase splits the chip into queued vs running (see
// verifyLabel). A fix ready to review and a node handed to a human also show
// the node's owner when the try recorded one.
function FixCell({ state, verifyPhase, note, owner }: {
  state?: FixState;
  verifyPhase?: 'queued' | 'running';
  note?: FixNote;
  owner?: ApproverDTO;
}) {
  if (state === 'proposed') {
    return (
      <>
        <Link to="/?tab=remediation" className="btn btn--secondary">
          Proposed fix available →
        </Link>
        {owner && <OwnerNote approver={owner} />}
      </>
    );
  }
  if (state) {
    return (
      <span className="btn btn--secondary is-disabled" aria-disabled="true" aria-busy="true">
        {state === 'verifying' ? verifyLabel(verifyPhase) : 'Generating fix…'}
      </span>
    );
  }
  if (note) {
    return (
      <>
        <span className="nodes-reason">{note.text}</span>
        {note.message && <ErrorCell message={note.message} />}
        {owner && <OwnerNote approver={owner} />}
      </>
    );
  }
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
  // fixes — see FixState. Nodes with only terminal-but-blank outcomes
  // (skipped/failed/escalated) or none carry no entry and render nothing here.
  const [fixState, setFixState] = useState<Map<string, FixState>>(new Map());
  // Per (stage, node_id) explanation for a node whose latest try landed on
  // a terminal-but-blank status, so the FIX cell says why instead of leaving
  // that cell empty. Only ever read where fixState has no entry for the key.
  const [fixNote, setFixNote] = useState<Map<string, FixNote>>(new Map());
  // Per (stage, node_id) owner of a node whose fix is ready to review or that
  // was handed to a human, as the try that decides the cell recorded it.
  // A node has no entry when that try recorded no approver for it (a row
  // with no remediation id records none), so the cell then says nothing about
  // owners.
  const [fixOwner, setFixOwner] = useState<Map<string, ApproverDTO>>(new Map());
  // The last polled fix list for this release, kept alongside fixState so
  // the dead-end check below can recompute in-flight/PR state per remediation
  // round (fixState itself is a rendering-only summary across every round).
  const [fixes, setFixes] = useState<FixDTO[]>([]);
  // Per (stage, node_id) release check phase for a node currently shown as
  // 'verifying', read off the fix's own release check summary
  // (tryReleaseCheckPhase) — whether the release check judging its fix
  // is still queued behind the global one-at-a-time release queue or already
  // running. Populated
  // only for verifying nodes; empty for every other node.
  const [verifyPhaseByKey, setVerifyPhaseByKey] = useState<Map<string, 'queued' | 'running'>>(new Map());
  // The release checks recorded for this release, newest first, rendered
  // in the "Release checks" section below.
  const [releaseChecks, setReleaseChecks] = useState<ReleaseCheckSummary[]>([]);
  const [retrying, setRetrying] = useState(false);
  const [retryError, setRetryError] = useState<string | null>(null);
  // Bumped by retry() to re-arm the fix-polling effect below after a
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

  // Failed nodes are the only ones eligible for a remediation fix. A stable
  // string key lets the polling effect re-subscribe only when that set changes.
  // Keyed on (stage, node_id) since a node_id alone is ambiguous across stages;
  // the keys themselves contain '|', so joining/splitting uses '\n'.
  const failedKeys = (rel?.per_node_results ?? [])
    .filter(n => n.status === 'failed')
    .map(n => fixKey(n.stage, n.node_id));
  const failedKey = failedKeys.slice().sort().join('\n');

  // Fixes are produced only after a release is rejected (failure
  // classification → LLM fix runs off release.rejected:v1), so a node
  // can already appear 'failed' in per_node_results while the release is still
  // 'validating' (live updates land mid-run) without any fix existing yet.
  // Gating on rejected — and keying the effect on it — means the poll starts
  // fresh exactly at the live transition to rejected, instead of having already
  // started (and possibly capped out via MAX_POLLS) before there was anything
  // to find.
  const isRejected = rel?.status === 'rejected';

  // This remediation round's fixes — a fix with no remediation_round
  // (older rows, or a round-1 try) counts as round 1. Scoping the
  // in-flight check to this set is what keeps a freshly-bumped round from
  // reading as a dead end off the *previous* round's now-terminal fixes.
  const currentRoundFixes = fixes.filter(p => effectiveRound(p) === rel?.remediation_round);

  // A rejected release is a dead end for remediation — nothing left for the
  // agent to do without human input — once it has at least one failed node,
  // the current round has produced at least one fix (the window between
  // "round bumped" and "the new round's first fix landed" is never a
  // dead end), no failed node has an in-flight/proposed fix in the current
  // round, and no fix from any round already has a PR out for review.
  // "Try again" (below) only ever shows in this state.
  const deadEnd = rel?.status === 'rejected'
    && failedKeys.length > 0
    && currentRoundFixes.length > 0
    && !hasActiveFix(currentRoundFixes, failedKeys)
    && !anyOpenPR(fixes);

  // Poll for remediation fixes so the FIX cell surfaces the "Generating fix…"
  // chip and then the "Proposed fix available →" link without a manual refresh.
  // Polling runs only while a failed node has not yet reached a ready fix,
  // stops once every failed node has one, and is capped so unhealable failures
  // (which never produce a ready fix) do not poll forever. The cap is
  // suspended while any failed node's fix is 'verifying': that verdict comes
  // from a whole release the backend runs behind a global queue, which routinely
  // outlasts the cap, and it always arrives — the backend ends the try
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
    // already found every fix and stopped polling.
    let issued = 0;
    let applied = 0;
    // Whether the last applied response showed a fix a release check is
    // still judging. Such a verdict is coming — the backend runs every stage
    // a release passes to produce it, behind a global queue, and ends
    // the try itself if it never arrives — so the cap below, which exists
    // for failures that will never be healed at all, must not apply while one
    // is outstanding. Otherwise a healthy transition to 'proposed' lands
    // after polling stopped and the page sits on "Checking the fix…" until
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
      return fetchFixes()
        .then(fetched => {
          if (cancelled || seq <= applied) return;
          applied = seq;
          const releaseFixes = fetched.filter(p => p.release_id === id);
          setFixes(releaseFixes);
          // Refresh the "Release checks" section on the same tick as the
          // fix poll, so a newly-started release check shows up without
          // a manual reload. Best-effort: a failed fetch leaves the section
          // showing its last-known rows rather than clearing them.
          fetch(`/api/releases/${id}/release-checks`)
            .then(r => (r.ok ? r.json() : { checks: [] }))
            .then(d => { if (!cancelled) setReleaseChecks(d.checks ?? []); })
            .catch(() => {});
          // Bucket each failed node by its fixes, keeping the furthest-along
          // state any of its tries reached; terminal-but-blank statuses
          // (skipped/failed/escalated) leave the node without an entry here —
          // see fixNote below for those. A batched fix is unpacked into
          // every node it resolves, each read against its own per-node
          // outcome rather than the fix's overall status.
          const byKey = new Map<string, FixState>();
          // Latest try per node (fix + which resolved node it was),
          // so fixNote explains why remediation stopped for that node rather
          // than why an earlier try, or a different node in the same
          // batched try, did.
          const latestByKey = new Map<string, { fix: FixDTO; nodeId: string }>();
          // Latest try that proposed a fix for each node, whose approver
          // is the owner the cell shows beside the link to that fix.
          const proposedByKey = new Map<string, { fix: FixDTO; nodeId: string }>();
          for (const p of releaseFixes) {
            for (const nid of fixNodeIds(p)) {
              const k = fixKey(p.source, nid);
              const status = fixStatusForNode(p, nid);
              if (isFixState(status)) {
                const current = byKey.get(k);
                if (!current || FIX_STATE_RANK[status] > FIX_STATE_RANK[current]) byKey.set(k, status);
              }
              if (status === 'proposed') {
                const proposed = proposedByKey.get(k);
                if (!proposed || tryBefore(proposed.fix, p)) proposedByKey.set(k, { fix: p, nodeId: nid });
              }
              const latest = latestByKey.get(k);
              if (!latest || tryBefore(latest.fix, p)) latestByKey.set(k, { fix: p, nodeId: nid });
            }
          }
          setFixState(byKey);
          // Release check phase for each node whose furthest-along state is
          // 'verifying', read off the fix's own release check summary
          // (tryReleaseCheckPhase) rather than a separate poll — so the verify
          // chip can split queued-vs-running. A node settled on any other
          // state carries no entry. When more than one try for a node is
          // still verifying, 'running' wins over 'queued'.
          const phaseByKey = new Map<string, 'queued' | 'running'>();
          for (const p of releaseFixes) {
            const phase = tryReleaseCheckPhase(p);
            if (!phase) continue;
            for (const nid of fixNodeIds(p)) {
              const k = fixKey(p.source, nid);
              if (byKey.get(k) !== 'verifying') continue;
              if (phase === 'running' || !phaseByKey.has(k)) phaseByKey.set(k, phase);
            }
          }
          setVerifyPhaseByKey(phaseByKey);
          const noteByKey = new Map<string, FixNote>();
          const ownerByKey = new Map<string, ApproverDTO>();
          for (const [k, { fix, nodeId }] of proposedByKey) {
            const approver = fixApproverForNode(fix, nodeId);
            if (approver) ownerByKey.set(k, approver);
          }
          for (const [k, { fix: p, nodeId: nid }] of latestByKey) {
            // A node outcome with a handover code says why the node went to a
            // human: the code's text, and the node's own message beneath it.
            // It reads the outcome's own reason, never the fix's
            // rationale, which on a batched try describes other nodes'
            // fixes.
            const code = fixCodeForNode(p, nid);
            if (code) {
              const message = p.node_outcomes?.[nid]?.reason?.trim();
              noteByKey.set(k, { text: HANDOVER_TEXT[code], message: message || undefined });
              // A node still being written or verified by another try
              // shows that chip instead, with no owner beside it.
              const approver = fixApproverForNode(p, nid);
              if (approver && !byKey.has(k)) ownerByKey.set(k, approver);
              continue;
            }
            const status = fixStatusForNode(p, nid);
            const reason = fixReasonForNode(p, nid);
            if (status === 'escalated') noteByKey.set(k, { text: "The round's tries are spent." });
            else if (status === 'failed') noteByKey.set(k, { text: reason || 'The model could not produce a safe fix.' });
            else if (status === 'skipped') noteByKey.set(k, { text: `Skipped try: ${reason || 'No source to fix at this commit.'} Fix it in the repository.` });
          }
          setFixNote(noteByKey);
          setFixOwner(ownerByKey);
          verifying = failed.some(k => byKey.get(k) === 'verifying');
          // Stop once every failed node has settled *for the release's current
          // remediation round*: its latest current-round try landed on
          // 'proposed', or on a terminal-but-blank outcome ('skipped'/
          // 'failed'/'escalated'). A node still 'generating'/'verifying', or
          // with no current-round fix at all yet, is not settled and
          // keeps the poll alive (up to the cap below, suspended by
          // `verifying`). Scoping to the current round (rather than reusing
          // byKey/noteByKey above, which intentionally span every round so
          // the FIX cell keeps showing the furthest-along try) matters
          // right after "Try again": the round bumps before its first
          // fix is written, and the previous round's now-stale terminal
          // outcome must not read as settled during that gap — otherwise
          // polling would stop before ever seeing the retry's fix.
          const currentRound = rel?.remediation_round;
          const currentRoundFixes = currentRound === undefined
            ? releaseFixes
            : releaseFixes.filter(p => effectiveRound(p) === currentRound);
          const latestCurrentRoundByKey = new Map<string, { fix: FixDTO; nodeId: string }>();
          for (const p of currentRoundFixes) {
            for (const nid of fixNodeIds(p)) {
              const k = fixKey(p.source, nid);
              const latest = latestCurrentRoundByKey.get(k);
              if (!latest || tryBefore(latest.fix, p)) latestCurrentRoundByKey.set(k, { fix: p, nodeId: nid });
            }
          }
          const settled = (k: string) => {
            const entry = latestCurrentRoundByKey.get(k);
            if (!entry) return false;
            return TERMINAL_NODE_STATUSES.has(fixStatusForNode(entry.fix, entry.nodeId));
          };
          if (failed.every(settled)) stop();
        })
        .catch(() => {});
    };

    // Always fetch once so an already-existing fix shows immediately.
    refresh();

    // Poll only while a failed node could still receive a fix.
    if (failed.length > 0) {
      timer = setInterval(() => {
        polls += 1;
        if (polls > MAX_POLLS && !verifying) { stop(); return; }
        refresh();
      }, POLL_INTERVAL_MS);
    }

    return () => { cancelled = true; stop(); };
  }, [id, isRejected, failedKey, pollNonce]);

  // restartFixPolling re-arms the effect above after a successful retry,
  // so a fresh remediation round's fixes are picked up without a reload.
  const restartFixPolling = () => setPollNonce(n => n + 1);

  // retry asks release-controller to start another remediation round. A 202
  // advances the round shown in the header and restarts fix polling; a
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
        restartFixPolling();
        return;
      }
      if (body.error === 'fix_open') {
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
  // The seed-build stage has finished once the release entered it and has
  // since moved on (or been rejected from it).
  const seedBuildRan = rel.status !== 'seed_building' && rel.transitions.some(t => t.to === 'seed_building');

  return (
    <div className="page">
      <PageHeader>
        <button type="button" className="detail-back-link" onClick={() => navigate('/?tab=releases')}>← Back</button>
        <div className="detail-page-title">{rel.release_id}</div>
        <span className={`pill ${releasePillClass(rel.status)}`}>
          {rel.status}{rel.remediation_round > 1 ? ` · round ${rel.remediation_round}` : ''}
        </span>
      </PageHeader>

      {rel.changed_project && (
        <p className="page-sub">
          Changes <strong>{rel.changed_project}</strong>
          {rel.commit_url && (
            <>
              <span className="page-sub__sep">·</span>
              <a href={rel.commit_url} target="_blank" rel="noopener noreferrer">
                {rel.repo} @ {shortSha(rel.commit_sha ?? '')} ↗
              </a>
            </>
          )}
        </p>
      )}

      {rel.reject_reason && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>
          {reasonLabel(rel.reject_reason)}
          {rel.reject_detail ? ` — ${rel.reject_detail}` : ''}
        </div>
      )}

      <SeedReloadNotices notices={rel.notices} seedBuildRan={seedBuildRan} />

      {rel.status === 'rejected' && deadEnd && rel.remediation_round < MAX_REMEDIATION_ROUNDS && (
        <div className="action-banner">
          <div className="action-banner__body">
            <div className="action-banner__title">No fix was produced for this release</div>
            <div className="action-banner__sub">
              The last try couldn’t open a pull request. Run another round to try again.
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
        <ProjectTiles imageTags={rel.image_tags || {}} changedProject={rel.changed_project} />
        <ReleaseCheckTimeline
          transitions={rel.transitions}
          check={{
            manifestKind: rel.manifest_kind,
            bootstrap: rel.bootstrap,
            parse_ms: rel.parse_ms,
            contracts_ms: rel.contracts_ms,
            contracts_by_family: rel.contracts_by_family,
          }}
        />
        {rel.status === 'promoted' && <ReleaseRunsPanel releaseId={rel.release_id} />}

        <NodeResultsTable
          perNode={perNode}
          fixCell={(stage, n) => {
            const key = fixKey(stage, n.node_id);
            const st = fixState.get(key);
            return (
              <FixCell
                state={st}
                verifyPhase={st === 'verifying' ? verifyPhaseByKey.get(key) : undefined}
                note={fixNote.get(key)}
                owner={fixOwner.get(key)}
              />
            );
          }}
        />
        {releaseChecks.length > 0 && (
          <>
            <div className="section-header">
              <div className="section-header__main">
                <span className="section-header__title">Release checks</span>
                <span className="section-header__count">{releaseChecks.length}</span>
              </div>
            </div>
            <table className="nodes-table">
              <thead><tr><th>Release check</th><th>Try</th><th>Project</th><th>Status</th><th>Started</th></tr></thead>
              <tbody>
                {releaseChecks.map(c => {
                  const phase = releaseCheckPhase(c.status);
                  return (
                    <tr key={c.check_id}>
                      <td><Link className="nodes-node-name" to={`/release-checks/${c.check_id}`}>{c.check_id}</Link></td>
                      <td>{tryLabel(c.remediation_round, c.try)}</td>
                      <td>{c.project}</td>
                      <td><span className={`pill-sm ${releasePillClass(phase).replace('pill--', 'pill-sm--')}`}>{phase}</span></td>
                      <td className="nodes-ts">{c.activated_at ? c.activated_at.slice(0, 19).replace('T', ' ') : '—'}</td>
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
