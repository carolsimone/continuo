import { Fragment, useCallback, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router';
import { ProposalDTO, PullRequestDTO } from './types';
import { fetchProposals } from './remediation-api';
import { fetchNodeProjects } from './node-api';
import type { CreatePullRequestResponse } from './remediation-api';
import {
  proposalNodeIds, proposalPrProjects, proposalPullRequests, proposalPillClass,
  effectiveRound, groupProposals, ProposalGroup,
} from './release-helpers';
import { useCurrentUser } from './auth/AuthContext';
import CreatePrModal from './CreatePrModal';

// DiffView mirrors the LogView component in ReleaseDetailPage: toggle view/hide,
// fetch content from /api/releases/log?key=<uri>, and link out to the full source.
// The first edit of a proposal opens by default — the diff is what a
// reviewer judges first — and the rest stay one click away.
function DiffView({ uri, defaultOpen = false }: { uri: string; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen);
  const [content, setContent] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const logUrl = `/api/releases/log?key=${encodeURIComponent(uri)}`;

  const load = () => {
    setErr(null);
    fetch(logUrl)
      .then(r => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then(setContent)
      .catch(e => setErr(e.message));
  };
  useEffect(() => { if (defaultOpen) load(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const toggle = () => {
    if (open) { setOpen(false); return; }
    setOpen(true);
    if (content === null) load();
  };

  return (
    <>
      <button type="button" className="btn btn--secondary" onClick={toggle}>
        {open ? 'hide' : 'view'}
      </button>{' '}
      <a className="btn btn--secondary" href={logUrl} target="_blank" rel="noreferrer">
        open full ↗
      </a>
      {open && (err
        ? <div className="info-strip info-strip--error"><span className="info-strip__icon">⚠</span>{err}</div>
        : <pre className="log-block">{content ?? 'loading…'}</pre>)}
    </>
  );
}

// sourceLabel says whether the attempt found the real source file it needed
// to change: only a resolved source can become a pull request.
function sourceLabel(resolved: boolean): string {
  return resolved ? 'resolved' : 'unresolved';
}

// releaseCheckPhaseLabel is the wording for one release check's recorded
// phase. The default branch covers both 'running' and '' (a release check the
// reconciler has not read yet) — an unread release check is already underway
// from the operator's point of view, so both read as the same in-progress
// wording.
function releaseCheckPhaseLabel(phase: string): string {
  switch (phase) {
    case 'queued':  return 'Queued for its release check';
    case 'running': return 'Checking the fix…';
    case 'passed':  return 'passed';
    case 'failed':  return 'failed';
    default:        return 'Checking the fix…';
  }
}

// StatusPill renders an attempt's lifecycle status in the same small pill the
// Releases list uses for a release status, coloured by proposalPillClass so
// the two surfaces share one vocabulary. A status still in flight (a fix
// being generated or verified) is marked busy for assistive tech.
function StatusPill({ status }: { status: string }) {
  const busy = status === 'verifying' || status === 'generating';
  return (
    <span
      className={`pill-sm ${proposalPillClass(status).replace('pill--', 'pill-sm--')}`}
      aria-busy={busy ? 'true' : undefined}
    >
      {status}
    </span>
  );
}

// ReleaseChecks lists one attempt's release checks — one per edited
// project — as compact labelled lines, "<project> · <kind> · <phase> · since
// <activated_at>", each linking to that release check's page. A legacy
// attempt with only the singular release_check_id shows the one link; an
// attempt judged without a release check renders nothing.
function ReleaseChecks({ proposal }: { proposal: ProposalDTO }) {
  const checks = proposal.release_checks && proposal.release_checks.length > 0
    ? proposal.release_checks.map((c) => ({
        key: c.check_id || c.project,
        checkId: c.check_id,
        text: `${c.project} · ${c.kind} · ${releaseCheckPhaseLabel(c.phase)}${c.activated_at ? ` · since ${c.activated_at}` : ''}`,
      }))
    : proposal.release_check_id
      ? [{ key: proposal.release_check_id, checkId: proposal.release_check_id, text: `release check ${proposal.release_check_id}` }]
      : [];
  if (checks.length === 0) return null;
  return (
    <div className="remediation-subrow__lines">
      <span className="remediation-subrow__label">release check</span>
      {checks.map((check) => (
        <div className="remediation-subrow__line" key={check.key}>
          <span>{check.text}</span>
          <Link to={`/release-checks/${check.checkId}`} className="btn btn--secondary">
            open release check →
          </Link>
        </div>
      ))}
    </div>
  );
}

// hasReleaseChecks mirrors ReleaseChecks' empty case so the row that
// hosts it is only rendered when there is something to show.
function hasReleaseChecks(p: ProposalDTO): boolean {
  return (p.release_checks?.length ?? 0) > 0 || Boolean(p.release_check_id);
}

// prStateBadge renders a pull request's state as a chip: merged green,
// rejected red, open (awaiting review) indigo, opening and failed grey — so
// the PR column reads uniformly whatever state each request is in.
function prStateBadge(prState: string) {
  return <span className={`pr-chip pr-chip--${prState}`}>{prState}</span>;
}

// prStateBadgeLabeled renders one pull request's state chip, prefixed by
// its owning project in small muted type when the proposal is split across
// several — the legacy (project '') group keeps the bare chip, unwrapped so
// its own text content is still just the state word.
function prStateBadgeLabeled(pr: PullRequestDTO) {
  const badge = prStateBadge(pr.pr_state);
  if (!pr.project) return badge;
  return (
    <span className="pr-chip-labeled">
      <span className="pr-chip-labeled__project">{pr.project}</span> {badge}
    </span>
  );
}

// prStateChips renders every recorded per-project pull-request state for one
// proposal as a run of chips, laid out by the .remediation-prs row they
// sit in.
function prStateChips(proposal: ProposalDTO) {
  return proposalPullRequests(proposal)
    .filter((pr) => pr.pr_state)
    .map((pr) => (
      <Fragment key={pr.project || 'legacy'}> {prStateBadgeLabeled(pr)}</Fragment>
    ));
}

// isActionable is the predicate for a proposal that still needs a human
// decision: it has a source to fix, and at least one of its owning-project
// groups is in a retryable claim state — the same precondition the server's
// BeginPullRequest CAS enforces per project ('' or 'failed'). A project
// group whose pr_state is 'opening' already has an in-flight or
// previously-recorded claim, and 'open'/'merged'/'rejected' are settled —
// offering another Create PR for those would just fail the claim. Such a
// proposal renders its detail card expanded in place instead of waiting
// for a click, as long as any group still needs a PR.
function isActionable(p: ProposalDTO): boolean {
  if (!(p.status === 'proposed' && p.source_resolved)) return false;
  const pullRequests = proposalPullRequests(p);
  return proposalPrProjects(p).some((project) => {
    const state = pullRequests.find((pr) => pr.project === project)?.pr_state ?? '';
    return state === '' || state === 'failed';
  });
}

function ProposalDetailCard({
  proposal,
  isOperator,
  onCreatePr,
}: {
  proposal: ProposalDTO;
  isOperator: boolean;
  onCreatePr: () => void;
}) {
  return (
    <div className="detail-card remediation-detail">
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">{proposalNodeIds(proposal).join(', ')}</span>
        </div>
        <div className="section-header__sub">
          release {proposal.release_id} · confidence {proposal.confidence}
        </div>
      </div>

      <div className="detail-card__body">
        {/* The diff is what a reviewer judges first, so it leads the card;
            the first edit opens by default and the rest stay one click away. */}
        {proposal.edits && proposal.edits.length > 0
          ? proposal.edits.map((edit, index) => (
              <div className="detail-card__row" key={edit.path}>
                <span className="detail-card__edit-path">{edit.path}</span>{' '}
                {edit.diff_uri && <DiffView uri={edit.diff_uri} defaultOpen={index === 0} />}
              </div>
            ))
          : proposal.diff_uri && (
              <div className="detail-card__row">
                <DiffView uri={proposal.diff_uri} defaultOpen />
              </div>
            )}

        {/* Why the release rejected this fix. It is the whole reason a fix
            put through a release check reached 'failed', so a card that
            omitted it would leave the operator with the word alone. */}
        {proposal.verify_error && (
          <div className="info-strip info-strip--error detail-card__row">
            <span className="info-strip__icon">⚠</span>
            Release check failed: {proposal.verify_error}
          </div>
        )}

        <pre className="detail-card__rationale">{proposal.rationale}</pre>

        {!proposal.source_resolved && (
          <div className="info-strip info-strip--warning detail-card__row">
            <span className="info-strip__icon">⚠</span>
            No real-source fix — a PR cannot be opened for this proposal.
          </div>
        )}

        {proposalPullRequests(proposal)
          .filter((pr) => pr.pr_url)
          .map((pr) => (
            <a
              key={pr.project || 'legacy'}
              className="btn btn--secondary"
              href={pr.pr_url}
              target="_blank"
              rel="noreferrer"
            >
              {pr.project ? `open PR (${pr.project}) ↗` : 'open PR ↗'}
            </a>
          ))}

        {isOperator && isActionable(proposal) && (
          proposal.repo ? (
            <button
              type="button"
              className="btn btn--secondary"
              onClick={onCreatePr}
            >
              Create PR
            </button>
          ) : (
            // Nowhere to open a pull request: agent-remediation refuses the
            // claim for a proposal with no repository, so the button is not
            // offered and the reason is shown in its place.
            <div className="info-strip info-strip--warning detail-card__row">
              <span className="info-strip__icon">⚠</span>
              No repository recorded for this proposal; a pull request cannot be opened.
            </div>
          )
        )}
      </div>
    </div>
  );
}

// AttemptRows renders one group's remediation attempts, newest first, inside
// the group's contained block: each a compact row (attempt · confidence ·
// source · status pill), its release checks as a labelled sub-row
// beneath, and — for the actionable attempt (auto) or a manually selected
// one — its full detail card with diffs and Create PR. The column labels
// are a muted sub-label of the block, not a peer of the group table's
// header.
function AttemptRows({
  group,
  isOperator,
  selectedId,
  onToggleAttempt,
  onCreatePr,
  detailRef,
}: {
  group: ProposalGroup;
  isOperator: boolean;
  selectedId: string | null;
  onToggleAttempt: (p: ProposalDTO) => void;
  onCreatePr: (p: ProposalDTO) => void;
  detailRef: (el: HTMLTableRowElement | null) => void;
}) {
  return (
    <table className="nodes-table remediation-attempts__table">
      <thead>
        <tr>
          <th>Attempt</th>
          <th>Confidence</th>
          <th>Source</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {group.attempts.map((p) => {
          const autoExpanded = isActionable(p);
          const isSelected = !autoExpanded && selectedId === p.id;
          const showCard = autoExpanded || isSelected;
          const showRuns = hasReleaseChecks(p);
          const toggle = () => onToggleAttempt(p);
          const compactRowClass = [
            autoExpanded ? 'nodes-row--static' : '',
            isSelected ? 'nodes-row--selected' : '',
            showCard || showRuns ? 'nodes-row--no-border' : '',
          ].filter(Boolean).join(' ');

          return (
            <Fragment key={p.id}>
              <tr
                className={compactRowClass}
                onClick={autoExpanded ? undefined : toggle}
                role={autoExpanded ? undefined : 'button'}
                tabIndex={autoExpanded ? undefined : 0}
                aria-expanded={autoExpanded ? undefined : isSelected}
                onKeyDown={autoExpanded ? undefined : (e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    toggle();
                  }
                }}
              >
                <td className="nodes-attempt">#{p.attempt}</td>
                <td>{p.confidence}</td>
                <td className="nodes-reason">{sourceLabel(p.source_resolved)}</td>
                <td><div className="remediation-prs"><StatusPill status={p.status} />{prStateChips(p)}</div></td>
              </tr>

              {showRuns && (
                <tr className={`remediation-subrow nodes-row--static${showCard ? ' nodes-row--no-border' : ''}`}>
                  <td colSpan={4}><ReleaseChecks proposal={p} /></td>
                </tr>
              )}

              {showCard && (
                <tr
                  className="nodes-row--static"
                  ref={el => { if (isSelected) detailRef(el); }}
                >
                  <td colSpan={4}>
                    <ProposalDetailCard
                      proposal={p}
                      isOperator={isOperator}
                      onCreatePr={() => onCreatePr(p)}
                    />
                  </td>
                </tr>
              )}
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );
}

export default function RemediationPanel() {
  const currentUser = useCurrentUser();
  const [proposals, setProposals] = useState<ProposalDTO[]>([]);
  const [projects, setProjects] = useState<string[]>([]);
  const [project, setProject] = useState('');
  const [openGroups, setOpenGroups] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<ProposalDTO | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [createPrProposalId, setCreatePrProposalId] = useState<string | null>(null);
  const detailRef = useRef<HTMLTableRowElement | null>(null);
  // Monotonic id of the most recently issued proposals fetch. Changing the
  // Project filter fires a new fetch before earlier ones settle; only the
  // latest request may write state, so a slower earlier response (or a stale
  // failure) can never overwrite the current filter's results.
  const proposalsReqSeq = useRef(0);

  // loadProposals is the single path that populates the list from the
  // server, used both on filter change and to refresh authoritative state
  // after an action (e.g. creating a PR) — never a bespoke fetch. It honours
  // the current project filter and ignores any response that a newer request
  // has superseded.
  const loadProposals = useCallback(() => {
    const seq = ++proposalsReqSeq.current;
    fetchProposals(project ? { project } : {})
      .then(data => {
        if (seq !== proposalsReqSeq.current) return;
        setProposals(data);
        setError(null);
      })
      .catch(e => {
        if (seq !== proposalsReqSeq.current) return;
        setError(e.message);
      });
  }, [project]);

  useEffect(() => { loadProposals(); }, [loadProposals]);

  // The Project filter's options are the projects of the live release's
  // nodes (fetchNodeProjects), fetched once on mount. A failure leaves the
  // list working with only the "All projects" option rather than surfacing
  // as the panel error.
  useEffect(() => {
    fetchNodeProjects().then(setProjects).catch(() => setProjects([]));
  }, []);

  // Scroll a manually-opened attempt card into view — an auto-expanded card
  // needs no scroll, it is already in place at its row.
  useEffect(() => {
    if (selected) {
      detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }, [selected]);

  const createPrProposal = proposals.find(p => p.id === createPrProposalId) ?? null;
  const groups = groupProposals(proposals);

  const toggleGroup = (key: string) => setOpenGroups(prev => {
    const next = new Set(prev);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    return next;
  });

  const toggleAttempt = (p: ProposalDTO) => setSelected(prev => (prev?.id === p.id ? null : p));

  return (
    <>
      <div className="form-field">
        <label htmlFor="remediation-project">Project</label>
        <select
          id="remediation-project"
          value={project}
          onChange={e => setProject(e.target.value)}
        >
          <option value="">All projects</option>
          {projects.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Proposals</span>
          <span className="section-header__count">{proposals.length}</span>
        </div>
      </div>

      {error && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{error}
        </div>
      )}

      {groups.length === 0 && !error && (
        <div className="info-strip info-strip--neutral">
          <span className="info-strip__icon">○</span>No proposals yet.
        </div>
      )}

      {groups.length > 0 && (
        <table className="nodes-table remediation-table">
          <thead>
            <tr>
              <th aria-hidden="true"></th>
              <th>Release</th>
              <th>Round</th>
              <th>Projects</th>
              <th>Nodes</th>
              <th>Latest status</th>
              <th>Attempts</th>
              <th>PR</th>
            </tr>
          </thead>
          <tbody>
            {groups.map(g => {
              const autoExpanded = g.attempts.some(isActionable);
              const isOpen = autoExpanded || openGroups.has(g.key);
              const toggle = () => toggleGroup(g.key);
              const rowClass = [
                'remediation-group',
                autoExpanded ? 'nodes-row--static' : '',
                isOpen ? 'remediation-group--open nodes-row--no-border' : '',
              ].filter(Boolean).join(' ');

              return (
                <Fragment key={g.key}>
                  <tr
                    className={rowClass}
                    onClick={autoExpanded ? undefined : toggle}
                    role={autoExpanded ? undefined : 'button'}
                    tabIndex={autoExpanded ? undefined : 0}
                    aria-expanded={autoExpanded ? undefined : isOpen}
                    onKeyDown={autoExpanded ? undefined : (e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        toggle();
                      }
                    }}
                  >
                    <td className="remediation-group__toggle" aria-hidden="true">
                      <span className={`remediation-chevron${isOpen ? ' remediation-chevron--open' : ''}`}>▸</span>
                    </td>
                    <td className="nodes-node-name">{g.releaseId}</td>
                    <td>{g.round}</td>
                    <td className="remediation-group__projects">
                      {g.projects.length > 0 ? g.projects.join(', ') : <span className="nodes-dash">—</span>}
                    </td>
                    <td className="remediation-group__nodes">{g.nodeIds.join(', ')}</td>
                    <td><StatusPill status={g.latest.status} /></td>
                    <td><span className="remediation-group__count">{g.attempts.length}</span></td>
                    <td className="remediation-group__prs">
                      {g.latestPrProposal
                        ? <div className="remediation-prs">{prStateChips(g.latestPrProposal)}</div>
                        : <span className="nodes-dash">—</span>}
                    </td>
                  </tr>
                  {isOpen && (
                    <tr className="remediation-group__body nodes-row--static">
                      <td colSpan={8}>
                        <div className="remediation-attempts">
                          <AttemptRows
                            group={g}
                            isOperator={currentUser?.role === 'operator'}
                            selectedId={selected?.id ?? null}
                            onToggleAttempt={toggleAttempt}
                            onCreatePr={(p) => setCreatePrProposalId(p.id)}
                            detailRef={(el) => { detailRef.current = el; }}
                          />
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      )}

      {createPrProposal && (
        <CreatePrModal
          proposal={createPrProposal}
          onClose={() => setCreatePrProposalId(null)}
          onCreated={(result: CreatePullRequestResponse) => {
            // Every GitHub PR in result.pull_requests is confirmed — show its
            // link right away. Recording it against the proposal is
            // best-effort server side, so 'open' is not guaranteed yet;
            // 'opening' is the one state the claim step already guarantees.
            // A project the server didn't touch this call (already settled,
            // or one that failed and is still listed as an error) keeps its
            // prior entry untouched. A refetch right after replaces all of
            // this with whatever the server actually recorded.
            const priorPRs = proposalPullRequests(createPrProposal);
            const newByProject = new Map(result.pull_requests.map(r => [r.project, r]));
            const mergedPRs: PullRequestDTO[] = priorPRs.map(pr => {
              const created = newByProject.get(pr.project);
              return created ? { ...pr, pr_url: created.pr_url, pr_number: created.pr_number, pr_state: 'opening' } : pr;
            });
            for (const created of result.pull_requests) {
              if (!mergedPRs.some(pr => pr.project === created.project)) {
                mergedPRs.push({
                  project: created.project,
                  repo: '',
                  branch: '',
                  pr_url: created.pr_url,
                  pr_number: created.pr_number,
                  pr_state: 'opening',
                  pr_opened_at: '',
                  pr_opened_by: '',
                  pr_closed_at: '',
                });
              }
            }
            const first = mergedPRs[0];
            const updated: ProposalDTO = {
              ...createPrProposal,
              pull_requests: mergedPRs,
              pr_url: first?.pr_url ?? createPrProposal.pr_url,
              pr_number: first?.pr_number ?? createPrProposal.pr_number,
              pr_state: first?.pr_state ?? createPrProposal.pr_state,
            };
            setProposals(prev => prev.map(p => (p.id === updated.id ? updated : p)));
            // The proposal may have just left the retryable-claim state
            // (isActionable goes false when every project settled), so its
            // group might no longer auto-expand. Pin the group open and
            // select the attempt so its row stays open and shows the new
            // link(s) instead of collapsing.
            setOpenGroups(prev => new Set(prev).add(`${updated.release_id} ${effectiveRound(updated)}`));
            setSelected(updated);
            loadProposals();
          }}
        />
      )}
    </>
  );
}
