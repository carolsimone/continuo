import { useEffect, useState } from 'react';
import PreviewList, { PreviewFacts, PreviewItem } from './PreviewList';
import { formatCount, formatDuration } from './node-helpers';
import { useActionRequestId } from './request-id';
import type { ChangedUpstreamNode, RebasePreview } from './types';

const REQUEST_FAILED = 'Request failed — please try again';
// The code POST /api/runs/:id/rebase answers with when the live release
// is no longer the one the dialog previewed.
const RELEASE_MOVED_CODE = 'release_moved';
const TITLE_ID = 'rebase-dialog-title';

interface Props {
  // runId is the failed or cancelled run to rebase.
  runId: string;
  driftLabel: string | null;
  // projectColors is the page's accent palette, so a project is painted here
  // as everywhere else on the page.
  projectColors: Map<string, string>;
  onClose: () => void;
  // onSubmitted runs once the rebase has been accepted.
  onSubmitted: () => void;
}

interface PreviewState {
  data: RebasePreview | null;
  // changedNodes are the changed upstream nodes the last successful preview
  // named; a failed preview keeps them, so the option to include them stays
  // offered.
  changedNodes: ChangedUpstreamNode[];
  loading: boolean;
  error: string | null;
}

type ButtonState = 'idle' | 'loading' | 'success';

// isRebasePreview reports whether a 200 answer has the preview's shape, so a
// body of another shape is a failed preview rather than a broken render.
function isRebasePreview(body: unknown): body is RebasePreview {
  const p = body as Partial<RebasePreview> | null;
  return typeof p?.release_id === 'string'
    && typeof p.will_run === 'number'
    && typeof p.total_nodes === 'number'
    && typeof p.inherited === 'number'
    && Array.isArray(p.changed_nodes);
}

// formatSeconds formats a duration in seconds; under a second reads "<1s".
function formatSeconds(seconds: number): string {
  return seconds < 1 ? '<1s' : formatDuration(Math.round(seconds));
}

// reasonFacts breaks the nodes a rebase runs down by why they run, leaving out
// a reason no node runs for.
function reasonFacts(p: RebasePreview): string[] {
  const reasons: Array<[number, string]> = [
    [p.failed_or_skipped, 'failed or skipped'],
    [p.new_nodes, 'new'],
    [p.changed_upstream, 'changed upstream'],
    [p.downstream, 'downstream'],
  ];
  return reasons.filter(([n]) => n > 0).map(([n, label]) => `${formatCount(n)} ${label}`);
}

// changedItem lists a changed upstream node under its schema.table id with the
// facts known about it: how many nodes it feeds and, when it has succeeded
// before, how long its latest success took.
function changedItem(n: ChangedUpstreamNode): PreviewItem {
  const facts: string[] = [];
  if (n.downstream > 0) facts.push(`+${formatCount(n.downstream)} downstream`);
  if (n.last_success_seconds !== null && n.last_success_seconds > 0) {
    facts.push(`last run ${formatSeconds(n.last_success_seconds)}`);
  }
  const id = `${n.schema_name}.${n.table_name}`;
  return { key: `${n.project_name}.${id}`, project: n.project_name, label: id, title: `${n.project_name}.${id}`, facts };
}

// RebaseDialog confirms a rebase of a failed or cancelled run onto the live
// release. It previews what the rebase would run as it opens, and again
// whenever "Include changed upstream nodes" changes, and rebases on exactly
// the previewed release. A refusal stays in the dialog; a moved live release
// previews again and says so.
export default function RebaseDialog({ runId, driftLabel, projectColors, onClose, onSubmitted }: Props) {
  const [includeChanged, setIncludeChanged] = useState(true);
  const [preview, setPreview] = useState<PreviewState>({ data: null, changedNodes: [], loading: false, error: null });
  // One request id for the dialog: every submit of the same request sends it,
  // so a submit repeated after a timeout returns the run the first one made.
  // Asking for something else (the other changed-upstream option) renews it.
  const { id: requestId, renew: renewRequestId } = useActionRequestId();
  const [reloadKey, setReloadKey] = useState(0);
  const [releaseMoved, setReleaseMoved] = useState(false);
  const [buttonState, setButtonState] = useState<ButtonState>('idle');
  const [submitError, setSubmitError] = useState<string | null>(null);
  const submitting = buttonState === 'loading';

  // The preview loads as the dialog opens, whenever the changed-upstream
  // option changes, and after a moved release refused a rebase. Each load aborts the one before it, and an answer that arrives
  // after its load was superseded is dropped. The last preview stays on
  // screen until the next one replaces it; a failed load drops it but keeps
  // the changed upstream nodes it named.
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    setPreview(p => ({ ...p, loading: true, error: null }));
    fetch(
      `/api/runs/${encodeURIComponent(runId)}/rebase-preview?include_changed_upstream=${includeChanged}`,
      { signal },
    )
      .then(async res => {
        const body = await res.json().catch(() => ({}));
        if (signal.aborted) return;
        if (res.ok && isRebasePreview(body)) {
          setPreview({ data: body, changedNodes: body.changed_nodes, loading: false, error: null });
        } else {
          const error = (!res.ok && body?.error) || REQUEST_FAILED;
          setPreview(p => ({ ...p, data: null, loading: false, error }));
        }
      })
      .catch(() => {
        if (!signal.aborted) setPreview(p => ({ ...p, data: null, loading: false, error: REQUEST_FAILED }));
      });
    return () => controller.abort();
  }, [includeChanged, runId, reloadKey]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !submitting) onClose();
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [onClose, submitting]);

  // chooseIncludeChanged previews the other selection, so a moved-release
  // warning shown for the previous one goes.
  const chooseIncludeChanged = (next: boolean) => {
    if (next !== includeChanged) renewRequestId();
    setIncludeChanged(next);
    setReleaseMoved(false);
  };

  const previewReady = preview.data !== null && !preview.loading && preview.error === null;
  const canSubmit = buttonState === 'idle' && previewReady;

  const submit = async () => {
    setButtonState('loading');
    setSubmitError(null);
    setReleaseMoved(false);
    try {
      const res = await fetch(`/api/runs/${encodeURIComponent(runId)}/rebase`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          request_id: requestId,
          include_changed_upstream: includeChanged,
          expected_release_id: preview.data?.release_id ?? '',
        }),
      });
      if (res.ok) {
        setButtonState('success');
        onSubmitted();
        return;
      }
      const body = await res.json().catch(() => ({}));
      setButtonState('idle');
      if (res.status === 409 && body?.code === RELEASE_MOVED_CODE) {
        setReleaseMoved(true);
        setReloadKey(k => k + 1);
        return;
      }
      setSubmitError(body?.error ?? REQUEST_FAILED);
    } catch {
      setButtonState('idle');
      setSubmitError(REQUEST_FAILED);
    }
  };

  const data = preview.data;
  const changedItems = preview.changedNodes.map(changedItem);
  const reasons = data ? reasonFacts(data) : [];

  const rebaseClass = [
    'btn', 'btn--primary',
    buttonState === 'loading' ? 'is-loading' : '',
    buttonState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');
  const rebaseLabel = buttonState === 'loading' ? 'Rebasing…' : buttonState === 'success' ? 'Rebased' : 'Rebase';

  return (
    <div className="dialog-overlay" onClick={() => { if (!submitting) onClose(); }}>
      <div
        className="dialog dialog--wide"
        role="dialog"
        aria-modal="true"
        aria-labelledby={TITLE_ID}
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id={TITLE_ID}>Rebase onto live release</h2>
        <p className="dialog-subtitle">Restarts this run from its failure on the live release.</p>

        {driftLabel && (
          <div className="info-strip info-strip--warning">
            <span className="info-strip__icon">⚠</span>
            <span>{driftLabel}</span>
          </div>
        )}

        {data && (
          <section className="preview-section">
            <div className="section-header">
              <div className="section-header__main">
                <span className="section-header__title">Will run</span>
                <span className="section-header__count">{formatCount(data.will_run)}</span>
              </div>
              <div className="section-header__sub">
                {`of ${formatCount(data.total_nodes)} nodes · ${formatCount(data.inherited)} inherited`}
              </div>
            </div>
            <PreviewFacts facts={reasons} summary />
          </section>
        )}

        {changedItems.length > 0 && (
          <>
            <label className="rerun-mode-option">
              <input
                type="checkbox"
                checked={includeChanged}
                disabled={submitting}
                onChange={e => chooseIncludeChanged(e.target.checked)}
              />
              <span className="rerun-mode-option__title">Include changed upstream nodes</span>
              <span className="rerun-mode-option__desc">
                Unchecked, only the failed part re-runs; a failure caused by one of these changes fails again
              </span>
            </label>
            <section className="preview-section">
              <div className="section-header">
                <div className="section-header__main">
                  <span className="section-header__title">Changed upstream</span>
                  <span className="section-header__count">{formatCount(changedItems.length)}</span>
                </div>
                {!includeChanged && <div className="section-header__sub">Left out of this rebase</div>}
              </div>
              <PreviewList items={changedItems} colors={projectColors} />
            </section>
          </>
        )}

        {releaseMoved && (
          <div className="info-strip info-strip--warning">
            <span className="info-strip__icon">⚠</span>
            <span>The live release changed; review the updated selection</span>
          </div>
        )}
        {preview.loading && (
          <div className="info-strip info-strip--neutral">
            <span>Loading what will run on the live release…</span>
          </div>
        )}
        {preview.error && (
          <div className="info-strip info-strip--error">
            <span className="info-strip__icon">⚠</span>
            <span>{preview.error}</span>
          </div>
        )}
        {submitError && (
          <div className="info-strip info-strip--error">
            <span className="info-strip__icon">⚠</span>
            <span>{submitError}</span>
          </div>
        )}

        <div className="dialog-actions">
          <button type="button" className="btn btn--secondary" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button type="button" className={rebaseClass} onClick={submit} disabled={!canSubmit}>
            {rebaseLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
