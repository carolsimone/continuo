import { Fragment, useCallback, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router';
import { ProposalDTO, PullRequestDTO } from './types';
import { fetchProposals, fetchNodeServices } from './remediation-api';
import type { CreatePullRequestResponse } from './remediation-api';
import {
  proposalNodeIds, proposalPrServices, proposalPullRequests,
  effectiveRound, groupProposals, ProposalGroup,
} from './release-helpers';
import { useCurrentUser } from './auth/AuthContext';
import CreatePrModal from './CreatePrModal';

// DiffView mirrors the LogView component in ReleaseDetailPage: toggle view/hide,
// fetch content from /api/releases/log?key=<uri>, and link out to the full source.
function DiffView({ uri }: { uri: string }) {
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

function sourceLabel(resolved: boolean): string {
  return resolved ? 'yes' : 'no';
}

// verificationPhaseLabel is the wording for one run's recorded phase. The
// default branch covers both 'running' and '' (a run the reconciler has not
// observed yet) — an unread run is already underway from the operator's
// point of view, so both read as the same in-progress wording.
function verificationPhaseLabel(phase: string): string {
  switch (phase) {
    case 'queued':  return 'Queued for verification';
    case 'running': return 'Verifying fix…';
    case 'passed':  return 'passed';
    case 'failed':  return 'failed';
    default:        return 'Verifying fix…';
  }
}

// verificationLines renders one attempt's verification runs — one per edited
// service — as "<service> · <kind> · <phase> · since <activated_at> · open
// run →", each linking to that run's page. A legacy attempt with only the
// singular verification_run_id shows the one link; an attempt judged without a
// run shows nothing.
function verificationLines(proposal: ProposalDTO) {
  if (proposal.verifications && proposal.verifications.length > 0) {
    return proposal.verifications.map((v) => (
      <div className="detail-card__row" key={v.run_id || v.service}>
        <span className="nodes-reason">
          {v.service} · {v.kind} · {verificationPhaseLabel(v.phase)}
          {v.activated_at ? ` · since ${v.activated_at}` : ''}
        </span>{' '}
        <Link to={`/verifications/${v.run_id}`} className="btn btn--secondary">
          open run →
        </Link>
      </div>
    ));
  }
  if (proposal.verification_run_id) {
    return (
      <div className="detail-card__row">
        <Link to={`/verifications/${proposal.verification_run_id}`} className="btn btn--secondary">
          verification run {proposal.verification_run_id} →
        </Link>
      </div>
    );
  }
  return null;
}

// statusChip renders a proposal's status. 'verifying' is the one status whose
// raw word does not say what is happening — the fix is written and a
// verification run is putting it through the full validation pipeline to
// decide whether it holds — so it reads as that wait, in the same
// non-actionable busy chip the release page shows for an in-flight fix.
// Every other status is already a plain statement of where the attempt ended.
function statusChip(status: string) {
  if (status === 'verifying') {
    return (
      <span className="btn btn--secondary is-disabled" aria-disabled="true" aria-busy="true">
        Verifying fix…
      </span>
    );
  }
  return <>{status}</>;
}

// prStateBadge renders terminal PR outcomes as colored chips; non-terminal
// pr_state values stay plain text.
function prStateBadge(prState: string) {
  if (prState === 'merged' || prState === 'rejected') {
    return <span className={`pr-chip pr-chip--${prState}`}>{prState}</span>;
  }
  return <>{prState}</>;
}

// prStateBadgeLabeled renders one pull request's state chip, labeled with
// its owning service when the proposal is split across several — the
// legacy (service '') group keeps today's unlabeled chip exactly as before,
// unwrapped so its own text content is still just the bare state word.
function prStateBadgeLabeled(pr: PullRequestDTO) {
  const badge = prStateBadge(pr.pr_state);
  if (!pr.service) return badge;
  return <span className="pr-chip-labeled">{badge} ({pr.service})</span>;
}

// prStateChips renders every recorded per-service pull-request state for one
// proposal, each preceded by a separator so it reads after the status word.
function prStateChips(proposal: ProposalDTO) {
  return proposalPullRequests(proposal)
    .filter((pr) => pr.pr_state)
    .map((pr) => (
      <Fragment key={pr.service || 'legacy'}> · {prStateBadgeLabeled(pr)}</Fragment>
    ));
}

// isActionable is the predicate for a proposal that still needs a human
// decision: it has a source to fix, and at least one of its owning-service
// groups is in a retryable claim state — the same precondition the server's
// BeginPullRequest CAS enforces per service ('' or 'failed'). A service
// group whose pr_state is 'opening' already has an in-flight or
// previously-recorded claim, and 'open'/'merged'/'rejected' are settled —
// offering another Create PR for those would just fail the claim. Such a
// proposal renders its detail card expanded in place instead of waiting
// for a click, as long as any group still needs a PR.
function isActionable(p: ProposalDTO): boolean {
  if (!(p.status === 'proposed' && p.source_resolved)) return false;
  const pullRequests = proposalPullRequests(p);
  return proposalPrServices(p).some((service) => {
    const state = pullRequests.find((pr) => pr.service === service)?.pr_state ?? '';
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
        <p className="detail-card__rationale">
          {proposal.rationale}
        </p>

        {/* Why the release rejected this fix. It is the whole reason a fix
            put through a verification run reached 'failed', so a card that
            omitted it would leave the operator with the word alone. */}
        {proposal.verify_error && (
          <div className="info-strip info-strip--error detail-card__row">
            <span className="info-strip__icon">⚠</span>
            Verification failed: {proposal.verify_error}
          </div>
        )}

        {proposal.edits && proposal.edits.length > 0
          ? proposal.edits.map((edit) => (
              <div className="detail-card__row" key={edit.path}>
                <span className="detail-card__edit-path">{edit.path}</span>{' '}
                {edit.diff_uri && <DiffView uri={edit.diff_uri} />}
              </div>
            ))
          : proposal.diff_uri && (
              <div className="detail-card__row">
                <DiffView uri={proposal.diff_uri} />
              </div>
            )}

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
              key={pr.service || 'legacy'}
              className="btn btn--secondary"
              href={pr.pr_url}
              target="_blank"
              rel="noreferrer"
            >
              {pr.service ? `open PR (${pr.service}) ↗` : 'open PR ↗'}
            </a>
          ))}

        {isOperator && isActionable(proposal) && (
          <button
            type="button"
            className="btn btn--secondary"
            onClick={onCreatePr}
          >
            Create PR
          </button>
        )}
      </div>
    </div>
  );
}

// AttemptRows renders one group's remediation attempts, newest first: each a
// compact row (attempt · confidence · source · status), its verification runs
// listed beneath, and — for the actionable attempt (auto) or a manually
// selected one — its full detail card with diffs and Create PR.
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
    <table className="nodes-table remediation-attempts">
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
          const verifs = verificationLines(p);
          const toggle = () => onToggleAttempt(p);
          const compactRowClass = [
            autoExpanded ? 'nodes-row--static' : '',
            isSelected ? 'nodes-row--selected' : '',
            showCard || verifs ? 'nodes-row--no-border' : '',
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
                <td>{p.attempt}</td>
                <td>{p.confidence}</td>
                <td>{sourceLabel(p.source_resolved)}</td>
                <td>{statusChip(p.status)}{prStateChips(p)}</td>
              </tr>

              {verifs && (
                <tr className={showCard ? 'nodes-row--static nodes-row--no-border' : 'nodes-row--static'}>
                  <td colSpan={4}>{verifs}</td>
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
  const [services, setServices] = useState<string[]>([]);
  const [service, setService] = useState('');
  const [openGroups, setOpenGroups] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<ProposalDTO | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [createPrProposalId, setCreatePrProposalId] = useState<string | null>(null);
  const detailRef = useRef<HTMLTableRowElement | null>(null);

  // loadProposals is the single path that populates the list from the
  // server, used both on filter change and to refresh authoritative state
  // after an action (e.g. creating a PR) — never a bespoke fetch. It honours
  // the current service filter.
  const loadProposals = useCallback(() => {
    fetchProposals(service ? { service } : {})
      .then(data => {
        setProposals(data);
        setError(null);
      })
      .catch(e => setError(e.message));
  }, [service]);

  useEffect(() => { loadProposals(); }, [loadProposals]);

  // The Service filter's options come from the node catalog, fetched once on
  // mount. A failure leaves the list working with only the "All services"
  // option rather than surfacing as the panel error.
  useEffect(() => {
    fetchNodeServices().then(setServices).catch(() => setServices([]));
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
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Proposals</span>
          <span className="section-header__count">{proposals.length}</span>
        </div>
        <div className="section-header__sub">
          <label htmlFor="remediation-service">Service</label>{' '}
          <select
            id="remediation-service"
            value={service}
            onChange={e => setService(e.target.value)}
          >
            <option value="">All services</option>
            {services.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
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
        <table className="nodes-table">
          <thead>
            <tr>
              <th aria-hidden="true"></th>
              <th>Release</th>
              <th>Round</th>
              <th>Services</th>
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
                autoExpanded ? 'nodes-row--static' : '',
                isOpen ? 'nodes-row--no-border' : '',
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
                    <td aria-hidden="true">{isOpen ? '▾' : '▸'}</td>
                    <td>{g.releaseId}</td>
                    <td>{g.round}</td>
                    <td>{g.services.length > 0 ? g.services.join(', ') : '—'}</td>
                    <td>{g.nodeIds.join(', ')}</td>
                    <td>{statusChip(g.latest.status)}</td>
                    <td>{g.attempts.length}</td>
                    <td>
                      {g.latestPrProposal
                        ? proposalPullRequests(g.latestPrProposal)
                            .filter((pr) => pr.pr_state)
                            .map((pr) => (
                              <Fragment key={pr.service || 'legacy'}>{prStateBadgeLabeled(pr)} </Fragment>
                            ))
                        : '—'}
                    </td>
                  </tr>
                  {isOpen && (
                    <tr className="nodes-row--static">
                      <td colSpan={8}>
                        <AttemptRows
                          group={g}
                          isOperator={currentUser?.role === 'operator'}
                          selectedId={selected?.id ?? null}
                          onToggleAttempt={toggleAttempt}
                          onCreatePr={(p) => setCreatePrProposalId(p.id)}
                          detailRef={(el) => { detailRef.current = el; }}
                        />
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
            // A service the server didn't touch this call (already settled,
            // or one that failed and is still listed as an error) keeps its
            // prior entry untouched. A refetch right after replaces all of
            // this with whatever the server actually recorded.
            const priorPRs = proposalPullRequests(createPrProposal);
            const newByService = new Map(result.pull_requests.map(r => [r.service, r]));
            const mergedPRs: PullRequestDTO[] = priorPRs.map(pr => {
              const created = newByService.get(pr.service);
              return created ? { ...pr, pr_url: created.pr_url, pr_number: created.pr_number, pr_state: 'opening' } : pr;
            });
            for (const created of result.pull_requests) {
              if (!mergedPRs.some(pr => pr.service === created.service)) {
                mergedPRs.push({
                  service: created.service,
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
            // (isActionable goes false when every service settled), so its
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
