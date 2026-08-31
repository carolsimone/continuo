import { useEffect, useState } from 'react';
import { ProposalDTO } from './types';
import { createPullRequest, CreatePullRequestResponse } from './remediation-api';
import { proposalNodeIds } from './release-helpers';

interface Props {
  proposal: ProposalDTO;
  onClose: () => void;
  onCreated: (result: CreatePullRequestResponse) => void;
}

type ButtonState = 'idle' | 'loading' | 'success';

export default function CreatePrModal({ proposal, onClose, onCreated }: Props) {
  const [buttonState, setButtonState] = useState<ButtonState>('idle');
  const [error, setError] = useState<string | null>(null);
  // result holds the last response this modal received, so a partial
  // success (some services opened, some failed) can stay on screen instead
  // of being lost the instant the request settles.
  const [result, setResult] = useState<CreatePullRequestResponse | null>(null);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const handleConfirm = async () => {
    setButtonState('loading');
    setError(null);
    try {
      const res = await createPullRequest(proposal.id);
      setResult(res);
      onCreated(res);
      if (res.errors.length === 0) {
        // Every owning service succeeded — behave exactly as before: show
        // success, hand the link(s) to the caller, and close.
        setButtonState('success');
        onClose();
        setTimeout(() => setButtonState('idle'), 3000);
      } else {
        // A partial success: some services opened, others didn't. Keep the
        // modal open so the operator can see which is which — closing here
        // would silently drop the services that still need attention.
        setButtonState('idle');
      }
    } catch (err: unknown) {
      const apiErr = err as { status?: number; message?: string };
      setButtonState('idle');
      setError(apiErr.message ?? 'Request failed — please try again');
    }
  };

  const btnLabel = buttonState === 'loading' ? 'Creating…' : buttonState === 'success' ? 'Created' : 'Create PR';
  const btnClass = [
    'btn',
    'btn--primary',
    buttonState === 'loading' ? 'is-loading' : '',
    buttonState === 'success' ? 'is-success' : '',
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-pr-dialog-title"
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id="create-pr-dialog-title">Create Pull Request</h2>

        <p>
          <strong>{proposalNodeIds(proposal).join(', ')}</strong> · release {proposal.release_id}
        </p>
        <p>This opens a GitHub PR applying the proposed fix; it will not be merged.</p>

        {result && result.pull_requests.length > 0 && (
          <ul>
            {result.pull_requests.map(pr => (
              <li key={pr.service || 'legacy'}>
                <a href={pr.pr_url} target="_blank" rel="noreferrer">
                  {pr.service ? `${pr.service}: open PR ↗` : 'open PR ↗'}
                </a>
              </li>
            ))}
          </ul>
        )}

        {result && result.errors.length > 0 && (
          <div className="info-strip info-strip--error">
            <span className="info-strip__icon">⚠</span>
            {result.errors.map(e => (e.service ? `${e.service}: ${e.error}` : e.error)).join('; ')}
          </div>
        )}

        {error && (
          <div className="info-strip info-strip--error">
            <span className="info-strip__icon">⚠</span>
            {error}
          </div>
        )}

        <div className="dialog-actions">
          <button
            type="button"
            className="btn btn--secondary"
            onClick={onClose}
          >
            Cancel
          </button>
          <button
            type="button"
            className={btnClass}
            disabled={buttonState === 'loading'}
            onClick={handleConfirm}
          >
            {btnLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
