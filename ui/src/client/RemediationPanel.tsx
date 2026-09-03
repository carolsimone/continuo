import { Fragment, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router';
import { ProposalDTO, PullRequestDTO } from './types';
import { fetchProposals } from './remediation-api';
import type { CreatePullRequestResponse } from './remediation-api';
import { proposalNodeIds, proposalPrServices, proposalPullRequests } from './release-helpers';
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
            verified by a release reached 'failed', so a card that omitted it
            would leave the operator with the word alone. */}
        {proposal.verify_error && (
          <div className="info-strip info-strip--error detail-card__row">
            <span className="info-strip__icon">⚠</span>
            Verification failed: {proposal.verify_error}
          </div>
        )}

        {/* The runs that judged this fix — one per edited service — with where
            each stands, so the operator can tell "waiting its turn" from
            "running" without opening the run. */}
        {proposal.verifications && proposal.verifications.length > 0
          ? proposal.verifications.map((v) => (
              <div className="detail-card__row" key={v.run_id}>
                <Link to={`/verifications/${v.run_id}`} className="btn btn--secondary">
                  verification run {v.run_id} →
                </Link>
                {' '}
                <span className="nodes-reason">
                  {v.service} · {v.kind} · {verificationPhaseLabel(v.phase)}
                </span>
              </div>
            ))
          : proposal.verification_run_id && (
              <div className="detail-card__row">
                <Link to={`/verifications/${proposal.verification_run_id}`} className="btn btn--secondary">
                  verification run {proposal.verification_run_id} →
                </Link>
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

export default function RemediationPanel() {
  const currentUser = useCurrentUser();
  const [proposals, setProposals] = useState<ProposalDTO[]>([]);
  const [selected, setSelected] = useState<ProposalDTO | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [createPrProposalId, setCreatePrProposalId] = useState<string | null>(null);
  const detailRef = useRef<HTMLTableRowElement | null>(null);

  // loadProposals is the single path that populates the list from the
  // server, used both on mount and to refresh authoritative state after an
  // action (e.g. creating a PR) — never a bespoke fetch.
  const loadProposals = () => {
    fetchProposals()
      .then(data => {
        setProposals(data);
        setError(null);
      })
      .catch(e => setError(e.message));
  };

  useEffect(loadProposals, []);

  // Scroll a manually-opened detail card into view — an auto-expanded card
  // needs no scroll, it is already in place at its row.
  useEffect(() => {
    if (selected) {
      detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }, [selected]);

  const createPrProposal = proposals.find(p => p.id === createPrProposalId) ?? null;

  return (
    <>
      {error && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{error}
        </div>
      )}

      {proposals.length === 0 && !error && (
        <div className="info-strip info-strip--neutral">
          <span className="info-strip__icon">○</span>No proposals yet.
        </div>
      )}

      {proposals.length > 0 && (
        <>
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">Proposals</span>
              <span className="section-header__count">{proposals.length}</span>
            </div>
          </div>
          <table className="nodes-table">
            <thead>
              <tr>
                <th>Node</th>
                <th>Release</th>
                <th>Confidence</th>
                <th>Source</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {proposals.map(p => {
                const autoExpanded = isActionable(p);
                const isSelected = !autoExpanded && selected?.id === p.id;
                const showCard = autoExpanded || isSelected;
                const toggle = () => setSelected(prev => (prev?.id === p.id ? null : p));
                // Whichever row sits directly above its own card — auto-expanded
                // or manually selected — drops its border so it doesn't draw a
                // hairline against that card. The card row itself keeps its own
                // border, which is what separates it from the next proposal.
                const compactRowClass = [
                  autoExpanded ? 'nodes-row--static' : '',
                  isSelected ? 'nodes-row--selected' : '',
                  showCard ? 'nodes-row--no-border' : '',
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
                      <td>{proposalNodeIds(p).join(', ')}</td>
                      <td>{p.release_id}</td>
                      <td>{p.confidence}</td>
                      <td>{sourceLabel(p.source_resolved)}</td>
                      <td>
                        {statusChip(p.status)}
                        {proposalPullRequests(p)
                          .filter((pr) => pr.pr_state)
                          .map((pr) => (
                            <Fragment key={pr.service || 'legacy'}> · {prStateBadgeLabeled(pr)}</Fragment>
                          ))}
                      </td>
                    </tr>
                    {showCard && (
                      <tr
                        className="nodes-row--static"
                        ref={el => { if (isSelected) detailRef.current = el; }}
                      >
                        <td colSpan={5}>
                          <ProposalDetailCard
                            proposal={p}
                            isOperator={currentUser?.role === 'operator'}
                            onCreatePr={() => setCreatePrProposalId(p.id)}
                          />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </>
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
            // (isActionable goes false when every service settled), so it
            // might no longer auto-expand. Select it manually so its row
            // stays open and shows the new link(s) instead of collapsing.
            setSelected(updated);
            loadProposals();
          }}
        />
      )}
    </>
  );
}
