import { useEffect, useId, useRef, useState } from 'react';
import { formatDayTime, formatFireDate, formatFireTime } from './scheduler-card-helpers';
import type { CronPreview, ScheduleSummary } from './types';

// How long the dialog waits after the last change of the cron or the
// timezone before it previews them.
const PREVIEW_DEBOUNCE_MS = 300;
// How long a confirmed save or removal shows its success before the dialog
// closes.
const CLOSE_AFTER_SUCCESS_MS = 600;
const REQUEST_FAILED = 'Request failed — please try again';
// The code PUT /api/schedules/:name/cron answers a 409 with when another
// edit saved first.
const STALE_VERSION_CODE = 'stale_version';
const CRON_FIELDS_HINT = 'minute · hour · day of month · month · day of week';

type Mode = 'edit' | 'add';
type Action = 'save' | 'remove';
type ActionState = { action: Action; phase: 'loading' | 'success' } | null;

// A preview answers for one cron in one timezone: the cron in words and its
// next fires, or the parser's refusal, or a failed request.
type PreviewResult =
  | { kind: 'ok'; words: string; next: string[]; timezone: string }
  | { kind: 'invalid'; message: string }
  | { kind: 'failed'; message: string };

interface Preview {
  key: string;
  result: PreviewResult;
}

const previewKey = (cron: string, timezone: string) => `${cron}\n${timezone}`;

function isCronPreview(body: unknown): body is CronPreview {
  const p = body as Partial<CronPreview> | null;
  return typeof p?.cron_words === 'string' && Array.isArray(p.next_fire_times);
}

function browserTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
}

// timeZoneOptions lists the IANA zones this browser knows, with UTC and the
// zone the dialog opened with, sorted.
function timeZoneOptions(current: string): string[] {
  const intl = Intl as typeof Intl & { supportedValuesOf?: (key: 'timeZone') => string[] };
  const zones = new Set(intl.supportedValuesOf?.('timeZone') ?? []);
  zones.add('UTC');
  if (current) zones.add(current);
  return [...zones].sort();
}

interface Props {
  // The schedule as the card showed it when the dialog opened. The dialog
  // keeps that reading, its version above all, while the card's poll goes on.
  schedule: ScheduleSummary;
  // edit changes, pauses or removes the cron of a schedule that has one; add
  // gives a cron to a schedule that runs only when triggered.
  mode: Mode;
  onClose: () => void;
}

// CronDialog sets when a schedule runs on its own. It previews the cron and
// timezone with the parser that fires them as the operator types, so the
// words and the next runs it shows are what will fire, and it saves only a
// cron whose preview came back. A save carries the version the dialog opened
// with: an edit someone else saved meanwhile is refused, and the dialog says
// who made it.
export default function CronDialog({ schedule, mode, onClose }: Props) {
  const [opened] = useState(schedule);
  const name = opened.schedule_name;
  const isEdit = mode === 'edit';
  const [cron, setCron] = useState(isEdit ? opened.cron : '');
  const [timezone, setTimezone] = useState(isEdit && opened.timezone ? opened.timezone : browserTimeZone());
  const [description, setDescription] = useState(isEdit ? opened.description : '');
  const [paused, setPaused] = useState(isEdit && opened.paused);
  const [zones] = useState(() => timeZoneOptions(timezone));
  const [preview, setPreview] = useState<Preview | null>(null);
  const [state, setState] = useState<ActionState>(null);
  const [staleBy, setStaleBy] = useState<{ who: string | null } | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  // Set once the operator changes the cron or the timezone: the first
  // preview, of what the dialog opened with, does not wait.
  const edited = useRef(false);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dialogRef = useRef<HTMLDivElement>(null);
  const cronRef = useRef<HTMLInputElement>(null);
  // Whether the focus was inside the dialog when the last request started.
  const focusWasInside = useRef(false);
  const ids = useId();
  const busy = state !== null;

  const cronText = cron.trim();
  const currentKey = previewKey(cronText, timezone);

  // Previews the cron and timezone PREVIEW_DEBOUNCE_MS after their last
  // change. A change cancels the wait and aborts the request before it, and
  // an answer that arrives after its request was superseded is dropped, so a
  // slow answer for an older cron never replaces a newer one. The last
  // preview stays on screen until the next one replaces it. An empty cron is
  // not previewed.
  useEffect(() => {
    if (cronText === '') {
      setPreview(null);
      return;
    }
    const controller = new AbortController();
    const { signal } = controller;
    const key = previewKey(cronText, timezone);
    const zone = timezone;
    const timer = setTimeout(() => {
      fetch(`/api/schedules/cron-preview?${new URLSearchParams({ cron: cronText, timezone: zone })}`, { signal })
        .then(async res => {
          const body = await res.json().catch(() => ({}));
          if (signal.aborted) return;
          if (res.ok && isCronPreview(body)) {
            setPreview({ key, result: { kind: 'ok', words: body.cron_words, next: body.next_fire_times, timezone: zone } });
          } else if (res.status === 400) {
            setPreview({ key, result: { kind: 'invalid', message: body?.error || 'Not a valid cron' } });
          } else {
            setPreview({ key, result: { kind: 'failed', message: (!res.ok && body?.error) || REQUEST_FAILED } });
          }
        })
        .catch(() => {
          if (!signal.aborted) setPreview({ key, result: { kind: 'failed', message: REQUEST_FAILED } });
        });
    }, edited.current ? PREVIEW_DEBOUNCE_MS : 0);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [cronText, timezone]);

  useEffect(() => () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
  }, []);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !busy) onClose();
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [onClose, busy]);

  // A request disables every control of the dialog, and a browser takes the
  // focus off a control that becomes disabled. When the request ends and the
  // dialog stays open, the focus returns to the cron input if it was inside
  // the dialog when the request started and is now on no element; focus that
  // moved to another element is left there.
  useEffect(() => {
    if (busy || !focusWasInside.current) return;
    focusWasInside.current = false;
    const active = document.activeElement;
    if (active === null || active === document.body) cronRef.current?.focus();
  }, [busy]);

  const succeed = (action: Action) => {
    setState({ action, phase: 'success' });
    closeTimer.current = setTimeout(onClose, CLOSE_AFTER_SUCCESS_MS);
  };

  // send runs one request of the dialog and keeps it open on a refusal,
  // saying why; a 409 stale_version says who saved first.
  const send = async (action: Action, init: RequestInit) => {
    focusWasInside.current = dialogRef.current?.contains(document.activeElement) ?? false;
    setState({ action, phase: 'loading' });
    setSubmitError(null);
    try {
      const res = await fetch(`/api/schedules/${encodeURIComponent(name)}/cron`, init);
      if (res.ok) {
        succeed(action);
        return;
      }
      const body = await res.json().catch(() => ({}));
      setState(null);
      if (res.status === 409 && body?.code === STALE_VERSION_CODE) {
        setStaleBy({ who: typeof body.updated_by === 'string' && body.updated_by ? body.updated_by : null });
        return;
      }
      setSubmitError(body?.error || REQUEST_FAILED);
    } catch {
      setState(null);
      setSubmitError(REQUEST_FAILED);
    }
  };

  const save = () => send('save', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      cron: cronText,
      timezone,
      description: description.trim(),
      paused: isEdit && paused,
      expected_version: opened.version,
    }),
  });

  const remove = () => send('remove', { method: 'DELETE' });

  const onCronChange = (value: string) => {
    edited.current = true;
    setCron(value);
  };
  const onTimezoneChange = (value: string) => {
    edited.current = true;
    setTimezone(value);
  };

  const result = preview?.result ?? null;
  const shown = result?.kind === 'ok' ? result : null;
  const refusal = result && result.kind !== 'ok' ? result : null;
  const previewed = preview?.key === currentKey && result?.kind === 'ok';
  const canSave = !busy && staleBy === null && cronText !== '' && previewed;

  const confirmIdle = isEdit ? 'Save' : 'Add cron';
  const confirmLoading = isEdit ? 'Saving…' : 'Adding…';
  const confirmSuccess = isEdit ? 'Saved' : 'Added';
  const saveState = state?.action === 'save' ? state.phase : null;
  const removeState = state?.action === 'remove' ? state.phase : null;
  const buttonClass = (base: string, phase: 'loading' | 'success' | null) => [
    'btn', base,
    phase === 'loading' ? 'is-loading' : '',
    phase === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const titleId = `${ids}-title`;
  const cronId = `${ids}-cron`;
  const hintId = `${ids}-cron-hint`;
  const wordsId = `${ids}-cron-words`;
  const refusalId = `${ids}-cron-refusal`;
  const describedBy = [hintId, wordsId, refusal ? refusalId : null].filter(Boolean).join(' ');

  return (
    <div className="dialog-overlay" onClick={() => { if (!busy) onClose(); }}>
      <div
        ref={dialogRef}
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id={titleId}>{isEdit ? 'Edit cron' : 'Add cron'} · {name}</h2>
        <p className="dialog-subtitle">
          {isEdit
            ? <>Sets when {name} runs on its own. Which models it runs comes from their dbt tag <b>{name}</b>.</>
            : <>{name} runs only when triggered. A cron makes it also run on its own.</>}
        </p>

        {isEdit && opened.updated_by && (
          <div className="info-strip info-strip--neutral">
            <span>
              Last changed by {opened.updated_by}
              {opened.updated_at ? ` · ${formatDayTime(opened.updated_at)}` : ''}
            </span>
          </div>
        )}

        <div className="dialog-field">
          <label htmlFor={cronId}>Cron</label>
          <input
            id={cronId}
            ref={cronRef}
            className="dialog-input dialog-input--mono"
            type="text"
            value={cron}
            placeholder="0 1 * * *"
            spellCheck={false}
            autoComplete="off"
            autoFocus
            disabled={busy}
            aria-invalid={refusal?.kind === 'invalid' ? true : undefined}
            aria-describedby={describedBy}
            onChange={e => onCronChange(e.target.value)}
          />
          <span className="dialog-field__hint" id={hintId}>{CRON_FIELDS_HINT}</span>
          <span className="dialog-field__words" id={wordsId} aria-live="polite">{shown?.words}</span>
        </div>

        <div className="dialog-field">
          <label htmlFor={`${ids}-timezone`}>Timezone</label>
          <select
            id={`${ids}-timezone`}
            className="dialog-select"
            value={timezone}
            disabled={busy}
            onChange={e => onTimezoneChange(e.target.value)}
          >
            {zones.map(zone => <option key={zone} value={zone}>{zone}</option>)}
          </select>
        </div>

        <div className="dialog-field">
          <label htmlFor={`${ids}-description`}>Description</label>
          <input
            id={`${ids}-description`}
            className="dialog-input"
            type="text"
            value={description}
            placeholder="Optional"
            disabled={busy}
            onChange={e => setDescription(e.target.value)}
          />
        </div>

        {isEdit && (
          <label className="rerun-mode-option">
            <input type="checkbox" checked={paused} disabled={busy} onChange={e => setPaused(e.target.checked)} />
            <span className="rerun-mode-option__title">Paused</span>
            <span className="rerun-mode-option__desc">
              Nothing starts on its own while paused. Resuming counts from then; fires missed while paused don&apos;t run.
            </span>
          </label>
        )}

        {/* The live region is in the dialog before a preview fills it: one inserted together with its content is not reliably announced. */}
        <div className="dialog-live" aria-live="polite">
          {shown && (
            <section className="preview-section">
              <div className="section-header">
                <div className="section-header__main">
                  <span className="section-header__title">Next runs</span>
                  {!(isEdit && paused) && <span className="section-header__count">{shown.next.length}</span>}
                </div>
                {isEdit && paused && <div className="section-header__sub">None while paused</div>}
              </div>
              {!(isEdit && paused) && (
                <ul className="preview-list">
                  {shown.next.map(at => (
                    <li key={at} className="preview-row preview-row--line">
                      <span>{formatFireDate(at, shown.timezone)}</span>
                      <span className="preview-facts">{formatFireTime(at, shown.timezone)}</span>
                    </li>
                  ))}
                </ul>
              )}
            </section>
          )}
        </div>

        {refusal && (
          <div className="info-strip info-strip--error" id={refusalId} role="alert">
            <span className="info-strip__icon" aria-hidden="true">⚠</span>
            <span>{refusal.message}</span>
          </div>
        )}
        {staleBy && (
          <div className="info-strip info-strip--warning" role="alert">
            <span className="info-strip__icon" aria-hidden="true">⚠</span>
            <span>
              {staleBy.who
                ? `${staleBy.who} changed this schedule while you were editing. Close and open it again.`
                : 'This schedule was changed while you were editing. Close and open it again.'}
            </span>
          </div>
        )}
        {submitError && (
          <div className="info-strip info-strip--error" role="alert">
            <span className="info-strip__icon" aria-hidden="true">⚠</span>
            <span>{submitError}</span>
          </div>
        )}

        {isEdit && (
          <div className="dialog-remove-row">
            <button
              type="button"
              className={buttonClass('btn--danger', removeState)}
              disabled={busy || staleBy !== null}
              onClick={remove}
            >
              {removeState === 'loading' ? 'Removing…' : removeState === 'success' ? 'Removed' : 'Remove cron'}
            </button>
            <span className="dialog-remove-row__note">{name} then runs only when triggered.</span>
          </div>
        )}

        <div className="dialog-actions">
          <button type="button" className="btn btn--secondary" disabled={busy} onClick={onClose}>
            Cancel
          </button>
          <button type="button" className={buttonClass('btn--primary', saveState)} disabled={!canSave} onClick={save}>
            {saveState === 'loading' ? confirmLoading : saveState === 'success' ? confirmSuccess : confirmIdle}
          </button>
        </div>
      </div>
    </div>
  );
}
