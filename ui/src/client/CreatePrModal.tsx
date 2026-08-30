import { useEffect, useState } from 'react';
import { ProposalDTO } from './types';
import { createPullRequest } from './remediation-api';
import { proposalNodeIds } from './release-helpers';

interface Props {
  proposal: ProposalDTO;
  onClose: () => void;
  onCreated: (prUrl: string) => void;
}

type ButtonState = 'idle' | 'loading' | 'success';

export default function CreatePrModal({ proposal, onClose, onCreated }: Props) {
  const [buttonState, setButtonState] = useState<ButtonState>('idle');
  const [error, setError] = useState<string | null>(null);

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
      const result = await createPullRequest(proposal.id);
      setButtonState('success');
      onCreated(result.pr_url);
      onClose();
      setTimeout(() => setButtonState('idle'), 3000);
    } catch (err: unknown) {
      const apiErr = err as { status?: number; pr_url?: string; message?: string };
      if (apiErr.status === 409 && apiErr.pr_url) {
        // PR already exists — treat as success
        setButtonState('success');
        onCreated(apiErr.pr_url);
        onClose();
        setTimeout(() => setButtonState('idle'), 3000);
      } else {
        setButtonState('idle');
        setError(apiErr.message ?? 'Request failed — please try again');
      }
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
