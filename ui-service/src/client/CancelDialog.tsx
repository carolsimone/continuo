import { useEffect, useState } from 'react';

interface CancelConfig {
  cancellation_reasons: string[];
}

interface Props {
  scheduleName: string;
  onClose: () => void;
}

export default function CancelDialog({ scheduleName, onClose }: Props) {
  const [config, setConfig] = useState<CancelConfig | null>(null);
  const [configError, setConfigError] = useState(false);
  const [reason, setReason] = useState('');
  const [otherText, setOtherText] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/config')
      .then(r => {
        if (!r.ok) throw new Error('config unavailable');
        return r.json();
      })
      .then((data: CancelConfig) => setConfig(data))
      .catch(() => setConfigError(true));
  }, []);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const isOther = reason === 'Other';
  const effectiveReason = isOther ? otherText.trim() : reason;
  const canSubmit =
    !submitting &&
    reason !== '' &&
    (!isOther || otherText.trim() !== '');

  const handleSubmit = async () => {
    setSubmitting(true);
    setSubmitError(null);
    try {
      const res = await fetch(`/api/schedules/${encodeURIComponent(scheduleName)}/cancel`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cancellation_reason: effectiveReason }),
      });
      if (res.ok) {
        onClose();
      } else {
        const body = await res.json().catch(() => ({}));
        setSubmitError(body.error ?? 'Request failed — please try again');
      }
    } catch {
      setSubmitError('Request failed — please try again');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={`cancel-dialog-title-${scheduleName}`}
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id={`cancel-dialog-title-${scheduleName}`}>Cancel run</h2>
        {configError ? (
          <p className="info-strip info-strip--error">Could not load configuration.</p>
        ) : !config ? (
          <p className="dialog-loading">Loading…</p>
        ) : (
          <>
            <label className="dialog-label">
              Reason
              <select
                className="dialog-select"
                value={reason}
                onChange={e => { setReason(e.target.value); setOtherText(''); }}
                autoFocus
              >
                <option value="">Select…</option>
                {config.cancellation_reasons.map(r => (
                  <option key={r} value={r}>{r}</option>
                ))}
              </select>
            </label>
            {isOther && (
              <label className="dialog-label">
                Describe the reason
                <textarea
                  className="dialog-textarea"
                  value={otherText}
                  onChange={e => setOtherText(e.target.value)}
                  placeholder="Enter reason…"
                  rows={3}
                />
              </label>
            )}
            {submitError && <p className="info-strip info-strip--error">{submitError}</p>}
            <div className="dialog-actions">
              <button
                type="button"
                className="btn btn--secondary"
                onClick={onClose}
              >
                Close
              </button>
              <button
                type="button"
                className="btn btn--danger"
                disabled={!canSubmit}
                onClick={handleSubmit}
              >
                {submitting ? 'Cancelling…' : 'Cancel run'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
