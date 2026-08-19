import { Fragment, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router';
import { ProposalDTO } from './types';
import { fetchProposals } from './remediation-api';
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

// statusChip renders a proposal's status. 'verifying' is the one status whose
// raw word does not say what is happening — the fix is written and a shadow
// release is running it through the full validation pipeline to decide whether
// it holds — so it reads as that wait, in the same non-actionable busy chip the
// release page shows for an in-flight fix. Every other status is already a
// plain statement of where the attempt ended.
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

// isActionable is the predicate for a proposal that still needs a human
// decision: it has a source to fix, and its pr_state is a retryable claim
// state — the same precondition the server's BeginPullRequest CAS enforces
// ('' or 'failed'). A proposal whose pr_state is 'opening' already has an
// in-flight or previously-recorded claim — offering another Create PR
// would just 409. Such a proposal renders its detail card expanded in
// place instead of waiting for a click.
function isActionable(p: ProposalDTO): boolean {
  return p.status === 'proposed' && p.source_resolved
    && (p.pr_state === '' || p.pr_state === 'failed');
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
          <span className="section-header__title">{proposal.node_id}</span>
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

        {/* The release that judged this fix. Without the link it is named on a
            different screen with nothing connecting the two. */}
        {proposal.shadow_release_id && (
          <div className="detail-card__row">
            <Link
              to={`/releases/${proposal.shadow_release_id}`}
              className="btn btn--secondary"
            >
              verification release {proposal.shadow_release_id} →
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

        {proposal.pr_url && (
          <a
            className="btn btn--secondary"
            href={proposal.pr_url}
            target="_blank"
            rel="noreferrer"
          >
            open PR ↗
          </a>
        )}

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
                      <td>{p.node_id}</td>
                      <td>{p.release_id}</td>
                      <td>{p.confidence}</td>
                      <td>{sourceLabel(p.source_resolved)}</td>
                      <td>{statusChip(p.status)}{p.pr_state ? <> · {prStateBadge(p.pr_state)}</> : null}</td>
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
          onCreated={(prUrl) => {
            // The GitHub PR itself is confirmed — show the link right away.
            // Recording the PR against the proposal is best-effort server
            // side, so 'open' is not guaranteed yet; 'opening' is the one
            // state the claim step already guarantees, and it keeps
            // isActionable false so this row doesn't offer another Create
            // PR while the true state loads. A refetch right after replaces
            // this with whatever the server actually recorded.
            const updated = { ...createPrProposal, pr_url: prUrl, pr_state: 'opening' };
            setProposals(prev => prev.map(p => (p.id === updated.id ? updated : p)));
            // The proposal just left the retryable-claim state (isActionable
            // goes false), so it no longer auto-expands. Select it manually
            // so its row stays open and shows the new "open PR ↗" link
            // instead of collapsing.
            setSelected(updated);
            setCreatePrProposalId(null);
            loadProposals();
          }}
        />
      )}
    </>
  );
}
