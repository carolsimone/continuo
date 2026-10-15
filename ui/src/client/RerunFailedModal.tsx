import { useEffect, useState } from 'react';
import PreviewList, { PreviewFacts, PreviewItem } from './PreviewList';
import { formatDuration } from './node-helpers';
import type { ChangedUpstreamNode, RebasePreview } from './types';

// 'this' reruns the failed nodes on the run's own snapshot; 'latest' rebases
// the run onto the live release, restarting it from its failure.
export type RerunFailedMode = 'this' | 'latest';

const REQUEST_FAILED = 'Request failed — please try again';
// The code POST /api/schedulers/:id/rebase answers with when the live release
// is no longer the one the dialog previewed.
const RELEASE_MOVED_CODE = 'release_moved';
const TITLE_ID = 'rerun-failed-dialog-title';

interface Props {
  // runId is the failed or cancelled run to rerun or rebase.
  runId: string;
  driftLabel: string | null;
  // serviceColors is the page's accent palette, so a service is painted here
  // as everywhere else on the page.
  serviceColors: Map<string, string>;
  onClose: () => void;
  // onSubmitted runs once the rerun or the rebase has been accepted.
  onSubmitted: (mode: RerunFailedMode) => void;
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

function formatCount(n: number): string {
  return n.toLocaleString('en-US');
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
  return { key: `${n.service_name}.${id}`, service: n.service_name, label: id, title: `${n.service_name}.${id}`, facts };
}

// RerunFailedModal asks how to rerun a failed or cancelled run's failed nodes
// and posts the choice itself. "This snapshot" reruns them on the run's own
// snapshot; "Latest snapshot" previews what a rebase onto the live release
// would run — again whenever "Include changed upstream nodes" changes — and
// rebases on exactly the previewed release. A refusal stays in the dialog; a
// moved live release previews again and says so.
export default function RerunFailedModal({ runId, driftLabel, serviceColors, onClose, onSubmitted }: Props) {
  const [mode, setMode] = useState<RerunFailedMode>('this');
  const [includeChanged, setIncludeChanged] = useState(true);
  const [preview, setPreview] = useState<PreviewState>({ data: null, changedNodes: [], loading: false, error: null });
  const [reloadKey, setReloadKey] = useState(0);
  const [releaseMoved, setReleaseMoved] = useState(false);
  const [buttonState, setButtonState] = useState<ButtonState>('idle');
  const [submitError, setSubmitError] = useState<string | null>(null);
  const submitting = buttonState === 'loading';

  // The preview loads when "Latest snapshot" is chosen, whenever the
  // changed-upstream option changes, and after a moved release refused a
  // rebase. Each load aborts the one before it, and an answer that arrives
  // after its load was superseded is dropped. The last preview stays on
  // screen until the next one replaces it; a failed load drops it but keeps
  // the changed upstream nodes it named.
  useEffect(() => {
    if (mode !== 'latest') return;
    const controller = new AbortController();
    const { signal } = controller;
    setPreview(p => ({ ...p, loading: true, error: null }));
    fetch(
      `/api/schedulers/${encodeURIComponent(runId)}/rebase-preview?include_changed_upstream=${includeChanged}`,
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
  }, [mode, includeChanged, runId, reloadKey]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !submitting) onClose();
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [onClose, submitting]);

  const chooseMode = (next: RerunFailedMode) => {
    setMode(next);
    setReleaseMoved(false);
    setSubmitError(null);
  };

  // chooseIncludeChanged previews the other selection, so a moved-release
  // warning shown for the previous one goes.
  const chooseIncludeChanged = (next: boolean) => {
    setIncludeChanged(next);
    setReleaseMoved(false);
  };

  const previewReady = preview.data !== null && !preview.loading && preview.error === null;
  const canSubmit = buttonState === 'idle' && (mode === 'this' || previewReady);

  const submit = async () => {
    const submitted = mode;
    setButtonState('loading');
    setSubmitError(null);
    setReleaseMoved(false);
    try {
      const res = submitted === 'this'
        ? await fetch(`/api/schedulers/${encodeURIComponent(runId)}/rerun`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
        })
        : await fetch(`/api/schedulers/${encodeURIComponent(runId)}/rebase`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            include_changed_upstream: includeChanged,
            expected_release_id: preview.data?.release_id ?? '',
          }),
        });
      if (res.ok) {
        setButtonState('success');
        onSubmitted(submitted);
        return;
      }
      const body = await res.json().catch(() => ({}));
      setButtonState('idle');
      if (submitted === 'latest' && res.status === 409 && body?.code === RELEASE_MOVED_CODE) {
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

  const data = mode === 'latest' ? preview.data : null;
  const changedItems = mode === 'latest' ? preview.changedNodes.map(changedItem) : [];
  const reasons = data ? reasonFacts(data) : [];

  const rerunClass = [
    'btn', 'btn--primary',
    buttonState === 'loading' ? 'is-loading' : '',
    buttonState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');
  const rerunLabel = buttonState === 'loading' ? 'Rerunning…' : buttonState === 'success' ? 'Reran' : 'Rerun';

  return (
    <div className="dialog-overlay" onClick={() => { if (!submitting) onClose(); }}>
      <div
        className="dialog dialog--wide"
        role="dialog"
        aria-modal="true"
        aria-labelledby={TITLE_ID}
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id={TITLE_ID}>Rerun failed nodes</h2>

        {driftLabel && (
          <div className="info-strip info-strip--warning">
            <span className="info-strip__icon">⚠</span>
            <span>{driftLabel}</span>
          </div>
        )}

        <label className="rerun-mode-option">
          <input
            type="radio"
            name="rerun-mode"
            value="this"
            checked={mode === 'this'}
            disabled={submitting}
            onChange={() => chooseMode('this')}
          />
          <span className="rerun-mode-option__title">This snapshot</span>
          <span className="rerun-mode-option__desc">
            Reuse the image + manifest pinned to this run. Reproduces the failure exactly.
          </span>
        </label>

        <label className="rerun-mode-option">
          <input
            type="radio"
            name="rerun-mode"
            value="latest"
            checked={mode === 'latest'}
            disabled={submitting}
            onChange={() => chooseMode('latest')}
          />
          <span className="rerun-mode-option__title">Latest snapshot</span>
          <span className="rerun-mode-option__desc">
            Use the current image + manifest. Restarts from the failure; new nodes run too.
          </span>
        </label>

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
                {!includeChanged && <div className="section-header__sub">Left out of this rerun</div>}
              </div>
              <PreviewList items={changedItems} colors={serviceColors} />
            </section>
          </>
        )}

        {mode === 'latest' && releaseMoved && (
          <div className="info-strip info-strip--warning">
            <span className="info-strip__icon">⚠</span>
            <span>The live release changed; review the updated selection</span>
          </div>
        )}
        {mode === 'latest' && preview.loading && (
          <div className="info-strip info-strip--neutral">
            <span>Loading what will run on the latest snapshot…</span>
          </div>
        )}
        {mode === 'latest' && preview.error && (
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
          <button type="button" className={rerunClass} onClick={submit} disabled={!canSubmit}>
            {rerunLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
