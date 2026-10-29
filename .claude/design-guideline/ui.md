# Continuo UI design guidelines

Forward-looking reference for any change in `ui/`. Describes the
design system as it stands once the
`2026-05-23-ui-button-and-layout-standardization` work has landed. If a
new page or component does not fit one of these patterns, add to the
system; do not invent a parallel one.

## Page shell

Every top-level route renders the same shell. No exceptions.

```jsx
<div className="page">
  <PageHeader actions={/* page-level badges */}>
    {/* the page's own identity: back link, title, status pill, chips */}
  </PageHeader>
  <main className="page-content">{/* full-width */}</main>
</div>
```

`PageHeader` (`ui/src/client/PageHeader.tsx`) renders the one header every
page shares:

```html
<header class="page-header">
  <a class="brand" href="/">◆ continuo</a>       <!-- h1-wrapped on the home page -->
  <span class="page-header__divider"></span>    <!-- only when the page has an identity -->
  ← Back   page-title   [STATUS]   ⚠ context chip
  <div class="page-actions">…actions…  <UserMenu /></div>
</header>
```

```css
.page         { min-height: 100vh; background: #f8f9fa; padding: 20px 24px; }
.page-header  { display: flex; flex-wrap: wrap; align-items: center;
                gap: 12px; row-gap: 8px;
                padding-bottom: 16px; margin-bottom: 16px;
                border-bottom: 1px solid #e2e8f0; }
.page-header__divider { width: 1px; height: 22px; background: #e2e8f0; flex-shrink: 0; }
.page-content {} /* full-width */
.page-content--readable { max-width: 960px; margin: 0 auto; }
```

Rules:

- The brand comes first on every page, in the same place, and links home.
  A back link never stands in for it: the page's own identity (back link,
  title, status pill, chips) follows the brand after `.page-header__divider`.
- The home page has no identity of its own — the brand is its title, so
  `PageHeader` wraps it in the `h1` (`brandAsTitle`) and draws no divider.
- `.page-actions` is right-aligned and always ends with the user menu, so
  sign-out is reachable from every page. Page-level badges (the home page's
  live badge) go in the `actions` slot before it.
- The row wraps on a narrow viewport (`flex-wrap: wrap`): `.page-actions`
  drops to its own line, the title shrinks and breaks (`min-width: 0;
  overflow-wrap: anywhere`) rather than widening the page, and the email is
  capped at 240px with an ellipsis. A header never makes the page scroll
  sideways; `ui/tests/client/styles-page-header-wrap.test.ts` pins this.
- Same top padding (20px) and side padding (24px) on every page.
- Use `.page-content--readable` only for list/feed pages where row width
  should stay scannable on wide monitors (e.g. the homepage schedule
  list). The shell itself stays full-width.
- The header bar separator (`border-bottom`) is always present; it
  anchors the page visually.

### Full-viewport surfaces

When a page's primary surface is spatial — a dependency graph, a map, a
canvas, a long timeline — it must fill the viewport height regardless
of the size of any secondary panel beside it. Empty secondary panels
must not shrink the primary surface.

Concretely: any multi-column layout container that hosts a spatial
primary surface anchors itself to the viewport instead of sizing to
content, and every column inside it stretches to that height.

```css
.detail-layout {
  display: flex;
  gap: 16px;
  min-height: calc(100vh - 160px); /* viewport minus page chrome */
  align-items: stretch;
}

.detail-right-col > .detail-card { flex: 1; min-height: 0; }
```

The `160px` chrome budget is the page padding + `.page-header` +
`.page-action-row` on DetailPage. Pages with different chrome should
keep the same pattern (viewport-anchored container + stretched
children) and adjust the offset.

### Direct manipulation outranks a background refresh

Spatial surfaces are usually fed by a poll, and their contents are
usually placed by a layout algorithm. Whenever the user can move, size
or reorder something on such a surface, that placement is **user state**
and the poll must not overwrite it. A layout recomputed from server data
applies to the elements the user has not touched; the ones they have
touched keep where they were put. Anything else silently discards
deliberate work seconds after it is done, and the surface reads as
broken rather than automatic.

This covers **how the surface is framed**, not just what sits on it.
Zoom and pan are placements too. An automatic fit exists to give a
sensible *starting* view; the moment the user zooms or pans, it has done
its job and must stop firing. Watch for the indirect triggers, which are
the ones that actually bite: a `ResizeObserver` re-fitting because a
neighbouring column changed height as data arrived, or a piece of state
that "arms" a fit for later so it lands seconds after the interaction
that caused it. Prefer a discriminator the framework already gives you —
a pointer/DOM event present on user-driven callbacks and absent on
programmatic ones — over trying to infer intent from the values.

Rules:

- Hold the user's placements in a map keyed by element id and overlay
  them on each recomputed layout, rather than trying to suppress the
  recomputation. The automatic layout stays correct for everything else,
  including elements that appear later.
- Pair it with a **`Reset layout`** `.btn--secondary` that drops every
  override, restores the automatic framing, and lets automatic behaviour
  resume. Render it only once there is something to reset — which
  includes the user having only re-framed the surface, with nothing
  moved — so the control does not sit dead in the chrome.
- An explicit reset is exempt from whatever suppression keeps automatic
  re-framing from yanking the view — the user asked for the default
  back, so give them all of it.
- Scope the overrides to the mounted surface unless the user asked for
  more. Placement that silently outlives a reload is its own surprise.

## Buttons

One class for the shape, three variants, two orthogonal states.

```css
.btn            { display: inline-flex; align-items: center; gap: 6px;
                  padding: 4px 12px;
                  font-weight: 500; font-size: 12px; line-height: 1.4;
                  border-radius: 4px; border: 1px solid transparent;
                  background: #fff; color: #374151; cursor: pointer;
                  white-space: nowrap; text-decoration: none;
                  transition: background .15s, border-color .15s, color .15s; }

.btn--secondary { border-color: #d1d5db; }
.btn--primary   { background: #4338ca; border-color: #4338ca; color: #fff; }
.btn--danger    { border-color: #fca5a5; color: #dc2626; }

.btn.is-loading { opacity: .6; cursor: progress; }
.btn.is-success { color: #16a34a; border-color: #86efac; }
.btn:disabled   { opacity: .4; cursor: not-allowed; }
```

Variant rules:

- **`btn--secondary`** is the default. Use it for almost everything:
  Trigger run, Rerun, Update Graph, Cancel-in-a-dialog.
- **`btn--primary`** only for the single confirming action inside a
  modal (e.g. the `Rerun` button in the rerun-failed modal).
- **`btn--danger`** for destructive actions (Cancel an active run).
- Never inline `<button style="…">` or introduce a new `.foo-btn` class.
  If a new use case appears, add it to this file first.

State rules (apply to every action button without exception):

| State | Label | Class |
|---|---|---|
| idle | `▶ Trigger run` / `Update Graph` / `↺ Rerun failed` | `.btn .btn--secondary` |
| loading | `Triggering…` / `Updating…` | adds `.is-loading`, disabled |
| success (~3s) | `Triggered` / `Updated` / `Reran` | adds `.is-success` |

- **No checkmarks** in the success label. The green colour IS the cue.
- **Past-tense single word** for success. Not "✓ Rerun triggered".
- **No separate text feedback element** next to a button to say "✓
  triggered". The button colour says it.
- Success state reverts to idle after ~3s (same timeout the homepage
  Update Graph already uses).
- Errors do NOT live next to the button as inline text. They render
  below the action row as an `.info-strip--error` (see below).

## Info-strips

One class, four colour variants. Used for any persistent or transient
status notice (drift warnings, snapshot banners, error messages, info
hints inside dialogs).

```css
.info-strip          { display: flex; align-items: center; gap: 8px;
                       padding: 7px 12px; border-radius: 6px;
                       font-size: 12px; border: 1px solid transparent; }
.info-strip__icon    { flex-shrink: 0; font-size: 13px; }
.info-strip__action  { margin-left: auto; }  /* optional .btn slot */

.info-strip--warning { background: #fef3c7; border-color: #fde68a; color: #92400e; }
.info-strip--error   { background: #fef2f2; border-color: #fca5a5; color: #dc2626; }
.info-strip--info    { background: #eef2ff; border-color: #c7d2fe; color: #4338ca; }
.info-strip--neutral { background: #f3f4f6; border-color: #e5e7eb; color: #374151; }
```

When in doubt:

- **warning** = something the user should know but the system is still
  functioning (drift, viewing a past snapshot, fresh image not yet
  rolled out).
- **error** = a request just failed.
- **info** = neutral context (e.g. tooltip-ish in-page hint).
- **neutral** = unknown / no signal yet.

Compact inline use is allowed (e.g. drift chip in the topbar — same
class, just sitting in a flex row).

For chip-style usage (drift warning, topology version, etc.), apply the
`.info-strip--inline` modifier in addition to the variant class:

```css
.info-strip--inline { padding: 2px 8px; font-size: 11px; }
```

Example: `<span className="info-strip info-strip--info info-strip--inline">topology v12</span>`.

## Action layout in topbars

When a page has more than two action buttons in its header:

```
Row 1: identity   ◆ continuo │ ← Back  page-title  [STATUS]  ⚠ context chip     user menu
Row 2: actions                                        [Action]  [Action]  [Action]
```

- Identity row is `PageHeader`: the brand, then back navigation, page
  title, status pill, *compact* info-strips (chips), then the user menu.
- A run that failed without a failed task says why in an inline
  `.info-strip--error` right after its status pill: `failed: stalled`,
  `failed: empty selection`, `failed: invalid node type`
  (`runFailureLabel`). A run whose task failed shows no such chip.
- A node page's task-timeout chip is `.info-strip--neutral` and always names
  the length and its source: `task timeout 8 h · declared` or
  `task timeout 24 h · install default`.
- Action row sits below it, right-aligned, containing only `.btn`
  elements.
- Errors from any action button render full-width directly below row 2
  as an `.info-strip--error`.
- If you find yourself wanting four or more buttons in the action row,
  collapse the closely-related ones into a single button that opens a
  modal with the choice.

## Modals

- Reuse the existing `.dialog-overlay` + `.dialog` shell. Dialog header
  uses `.dialog-title`. Dialog buttons MUST be `.btn` — no
  `.dialog-btn--*` siblings.
- Confirmation modals (e.g. rerun-failed-modal) have:
  - Optional context info-strip at the top (e.g. drift warning).
  - Option rows — radio choices, or a checkbox for an on/off setting —
    each with a short description (see "Option rows").
  - Optionally, what the confirming verb will touch, as a preview list
    (see "Preview list").
  - Status strips (loading, a failed request) directly above the footer,
    next to the button they explain.
  - Footer: `Cancel` (`.btn--secondary`) on the left, the confirming
    verb (`.btn--primary`) on the right, with the standard
    `.is-loading` (e.g. `Rerunning…`; both buttons disabled while the
    request is in flight) and `.is-success` states. Escape and an overlay
    click close the modal unless its request is in flight.
- The confirming verb is the action the modal performs (`Rerun`,
  `Cancel run`, `Trigger`). Not "OK" or "Submit".
- The `.dialog` is 360px by default. A picker/list dialog that needs more
  room uses the `.dialog--wide` modifier (480px); it still caps at
  `calc(100vw - 32px)` so it never overflows a narrow viewport. Do not
  hand a dialog a bespoke width. A `.dialog--wide` also caps its height at
  `calc(100vh - 32px)` and scrolls inside, so a list-carrying dialog never
  pushes its footer off a short viewport:

  ```css
  .dialog--wide { width: 480px; max-height: calc(100vh - 32px); overflow-y: auto; }
  ```
- A muted one-line explanation directly under the `.dialog-title` uses
  `.dialog-subtitle` (12px gray). One line; longer guidance is an
  `.info-strip` instead.

### Snapshot pick list

A **select-to-act** list inside a modal: each row is the action (clicking
it commits the choice and closes), so the footer carries only `Cancel` —
there is no confirming verb. The snapshot picker (`RunSourcePickerDialog`,
opened by `Run with old snapshot`) is the reference: it collapses a node's
runs into the distinct snapshots it can be re-run against — one row per
`image_tag` — and picking one runs the node
against that snapshot's most-recent terminal run.

```jsx
<div className="section-header">
  <div className="section-header__main">
    <span className="section-header__title">Snapshots</span>
    <span className="section-header__count">8</span>
  </div>
  <div className="section-header__sub">35 runs total</div>
</div>
<ul className="pick-list">
  <li>
    <button type="button" className="pick-row" onClick={() => onPick(id)}>
      <span className="pick-row__main">
        <span className="pick-row__id">dfee104</span>
        <span className="pill-sm pill-sm--succeeded">succeeded</span>
      </span>
      <span className="pick-row__meta">
        <span>2 runs</span><span className="pick-row__sep">·</span>
        <span>last 11 Sep 2026</span>
      </span>
    </button>
  </li>
</ul>
```

```css
.pick-list { list-style: none; margin: 0; padding: 0; display: flex;
             flex-direction: column; gap: 4px;
             max-height: min(52vh, 420px); overflow-y: auto; }
.pick-row  { width: 100%; display: flex; flex-direction: column; gap: 4px;
             padding: 8px 10px; border: 1px solid #e2e8f0; border-radius: 6px;
             background: #fff; text-align: left; cursor: pointer; font: inherit; }
.pick-row:hover { background: #f8f9fa; border-color: #c7d2fe; }
.pick-row__id   { font-family: 'SF Mono', 'Fira Mono', monospace; font-size: 12.5px;
                  font-weight: 600; color: #111827; }
.pick-row__meta { font-size: 11.5px; color: #6b7280; font-variant-numeric: tabular-nums; }
```

Rules:

- The list scrolls inside its own `max-height` region so a long history
  stays inside the dialog instead of pushing the title and `Cancel` off
  the viewport. This bounded scroll is the whole point of the pattern.
- Precede the list with a `.section-header` whose count is the number of
  rows and whose sub-line counts what they roll up (e.g. `35 runs total`).
- Each row is a `.pick-row` button — the whole row is the click target,
  keyboard-operable for free. The primary line pairs a monospace
  identifier (`.pick-row__id`) with the rolled-up status `pill-sm`; a
  muted `.pick-row__meta` line follows, its facts separated by
  `.pick-row__sep` (`·`), never punctuation inside the text. A meta fact
  that is only sometimes known (e.g. a completion time on an in-flight
  run) is simply absent, never an empty label.
- Empty state is an `.info-strip--neutral`, not a bare paragraph.

### Preview list

The **read-only** sibling of the snapshot pick list: a list inside a
modal that names what the confirming verb will touch, so the user can
judge it before confirming. Rows are facts, not choices — nothing happens
on click, and the footer keeps its confirming verb. The rerun-failed
dialog is the reference: under "Latest snapshot" it previews the rebase —
how many nodes will run and why — and lists the changed nodes upstream of
the failure ("Changed upstream").

```jsx
<section className="preview-section">
  <div className="section-header">
    <div className="section-header__main">
      <span className="section-header__title">Will run</span>
      <span className="section-header__count">41</span>
    </div>
    <div className="section-header__sub">of 2,000 nodes · 1,959 inherited</div>
  </div>
  <span className="preview-facts preview-facts--summary">
    <span>3 failed or skipped</span><span className="preview-facts__sep">·</span>
    <span>1 new</span><span className="preview-facts__sep">·</span>
    <span>2 changed upstream</span><span className="preview-facts__sep">·</span>
    <span>35 downstream</span>
  </span>
</section>

<section className="preview-section">
  <div className="section-header">
    <div className="section-header__main">
      <span className="section-header__title">Changed upstream</span>
      <span className="section-header__count">2</span>
    </div>
  </div>
  <ul className="preview-list">
    <li className="preview-row">
      <span className="preview-row__main">
        <span className="nodes-group-dot" style={{ background: accent }} />
        <span className="preview-row__id" title="core.stg.orders">stg.orders</span>
      </span>
      <span className="preview-facts">
        <span>+18 downstream</span><span className="preview-facts__sep">·</span>
        <span>last run 4m 12s</span>
      </span>
    </li>
  </ul>
</section>
```

Past twelve rows the list groups them by service; each group is one
`li.preview-group` holding a header and, while open, its rows:

```jsx
<li className="preview-group">
  <div className="nodes-group-header preview-group__header"
       role="button" tabIndex={0} aria-expanded={isOpen}>
    <span className="nodes-group-chevron">▸</span>
    <span className="nodes-group-dot" style={{ background: accent }} />
    <span className="nodes-group-name">core</span>
    <span className="nodes-group-count">9</span>
  </div>
  {isOpen && <ul className="preview-group__rows">{/* .preview-row … */}</ul>}
</li>
```

The list, the row box and the row's text share their rules with the
snapshot pick list by selector grouping — one declaration block per
shared rule, never a copy. Only what differs is declared apart: a pick
row's button behaviour (`cursor`, `:hover`, `:focus-visible`), each
list's height, and the preview's own section, summary and group rules.

```css
.pick-list, .preview-list   { list-style: none; margin: 0; padding: 0; display: flex;
                              flex-direction: column; gap: 4px; overflow-y: auto; }
.pick-list                  { max-height: min(52vh, 420px); }
.preview-list               { max-height: min(32vh, 280px); }
.pick-row, .preview-row     { display: flex; flex-direction: column; gap: 4px;
                              padding: 8px 10px; border: 1px solid #e2e8f0;
                              border-radius: 6px; background: #fff; }
.pick-row__main, .preview-row__main { display: flex; align-items: center; gap: 8px; }
.pick-row__id, .preview-row__id     { font-family: 'SF Mono', 'Fira Mono', monospace;
                                      font-size: 12.5px; font-weight: 600; color: #111827; }
.pick-row__meta, .preview-facts     { font-size: 11.5px; color: #6b7280;
                                      font-variant-numeric: tabular-nums; }
.pick-row__sep, .preview-facts__sep { color: #cbd5e1; }
.preview-section            { display: flex; flex-direction: column; gap: 8px; }
.preview-facts--summary     { padding: 0 16px; font-size: 12px; }
.preview-group__header      { padding: 7px 10px; border-radius: 6px; background: #f8fafc;
                              cursor: pointer; }
.preview-group__header:hover { background: #f1f5f9; }
.preview-group__rows        { list-style: none; margin: 4px 0 0; padding: 0 0 0 18px;
                              display: flex; flex-direction: column; gap: 4px; }
```

Rules:

- The list scrolls inside its own `max-height` region, as `.pick-list`
  does. It is shorter than a pick list because it shares its dialog with
  option rows and counts; the dialog is a `.dialog--wide`, which itself
  scrolls when a short viewport cannot hold it.
- Rows are **not buttons**: a `.preview-row` is an `<li>` with no hover
  state, no pointer cursor and no click handler. If picking a row is the
  action, use the snapshot pick list instead.
- Precede the list with a `.section-header`: its title names what the
  rows are, its count pill is the number of rows, and its sub-line
  qualifies them when needed (e.g. `Left out of this rerun` when an
  option leaves them out, so the list never reads as "will run").
- A totals section ("Will run") states its total in the count pill and
  what it is out of in the sub-line; a breakdown of the same total is one
  `.preview-facts--summary` line directly under the header, never a
  second sub-line. A count that is zero is left out of the breakdown.
- Each row: the service accent dot (`.nodes-group-dot`, colour from the
  page's own `buildServiceColors` map so a service is painted as
  everywhere else on the page, slate `#94a3b8` for a service the page
  does not show — the sanctioned style exception, see Service accent
  colors), then the monospace identifier (`.preview-row__id`, ellipsised,
  with the full id in `title`). A muted `.preview-facts` line follows,
  its facts separated by `.preview-facts__sep` (`·`), never punctuation
  inside the text. Numbers carry thousands separators (`2,000`); a
  duration uses `formatDuration`, and a sub-second one reads `<1s`.
- A fact that is not known (the duration of a node that never succeeded)
  is absent, never an empty label or a dash; a row with no known fact
  has no facts line.
- Past twelve rows, rows group under collapsible per-service headers,
  sorted by service name and collapsed by default. The header reuses the
  Grouped table rows header (`.nodes-group-header`: chevron, accent dot,
  service name, `.nodes-group-count`) and its keyboard/role contract
  (`role="button"`, `tabIndex={0}`, Enter/Space, `aria-expanded`). There
  is no rolled-up status pill: preview rows carry no status.
- Empty state: no section at all when an empty list would say nothing the
  counts above have not said; otherwise an `.info-strip--neutral`.
- While a preview that an explicit action started loads (the modal
  opening, a click on an option), an `.info-strip--neutral` says so and
  the confirming verb is disabled; a failed preview is an
  `.info-strip--error` and the confirming verb stays disabled until a
  preview arrives. A preview that follows a field the operator types has
  no loading strip (see "Dialog fields"), and the region that holds its
  `.preview-section` (`.dialog-live`, `aria-live="polite"`) is in the dialog
  from the start, empty and taking none of the dialog's gap (`:empty` takes
  it back), because a live region inserted together with its content is not
  reliably announced. A reload (an option changed)
  keeps the last preview on screen until the next one replaces it, so the
  option the user just clicked does not move. When the confirmation is
  refused because what was previewed is no longer true, the modal previews
  again and says so in an `.info-strip--warning`.

A row that is one fact and its value — a date and a time in the cron
dialog's "Next runs" — sits on one line: `.preview-row--line` lays the row
out as a row, the fact on the left and its value (`.preview-facts`) on the
right, with no identifier or accent dot.

```jsx
<li className="preview-row preview-row--line">
  <span>Wed 28 Oct 2026</span>
  <span className="preview-facts">01:00</span>
</li>
```

```css
.preview-row--line { flex-direction: row; align-items: center; justify-content: space-between;
                     padding: 6px 10px; font-size: 12px; color: #111827; }
```

### Option rows

A choice inside a modal — one of several radio choices, or an on/off
checkbox — is a `.rerun-mode-option` row: the control, a bold title, and
one muted line saying what the option does. The whole row is the label,
so clicking anywhere on it toggles the control.

```jsx
<label className="rerun-mode-option">
  <input type="checkbox" checked={include} onChange={…} />
  <span className="rerun-mode-option__title">Include changed upstream nodes</span>
  <span className="rerun-mode-option__desc">
    Unchecked, only the failed part re-runs; a failure caused by one of these changes fails again
  </span>
</label>
```

```css
.rerun-mode-option { display: grid; grid-template-columns: auto 1fr;
                     grid-template-rows: auto auto; column-gap: 10px; row-gap: 2px;
                     padding: 10px 12px; border: 1px solid #e2e8f0; border-radius: 6px;
                     cursor: pointer; }
.rerun-mode-option:hover { background: #f8fafc; }
.rerun-mode-option > input[type="radio"],
.rerun-mode-option > input[type="checkbox"] { grid-row: 1 / span 2; align-self: center; }
.rerun-mode-option__title { font-size: 13px; font-weight: 600; color: #0f172a; }
.rerun-mode-option__desc  { font-size: 12px; color: #475569; }
```

Rules:

- Radios for mutually exclusive choices, a checkbox for one setting that
  is on or off. Never a toggle switch or a custom control.
- The title is a short noun phrase or imperative ("Latest snapshot",
  "Include changed upstream nodes"); the description is one line and says
  what happens, including what is lost when the option is off.
- A checkbox defaults to the choice most users want (the rerun-failed
  dialog includes changed upstream nodes by default). An option that
  changes what a preview list shows previews again when it changes.
- Show a checkbox only when it changes something: the rerun-failed
  dialog hides "Include changed upstream nodes" when nothing upstream
  changed.

### Dialog fields

A labelled text input or select inside a modal is a `.dialog-field`: the
label, the native control, and optional muted lines under the control. It
shares its typography with `.dialog-label` (the label that wraps its
control, as in the cancel dialog) by selector grouping; use `.dialog-field`
when the label is tied by `htmlFor` or the field carries lines under its
control.

```jsx
<div className="dialog-field">
  <label htmlFor="cron">Cron</label>
  <input id="cron" className="dialog-input dialog-input--mono" aria-invalid={refused || undefined}
         aria-describedby={refused ? 'cron-hint cron-words cron-refusal' : 'cron-hint cron-words'} />
  <span className="dialog-field__hint" id="cron-hint">minute · hour · day of month · month · day of week</span>
  <span className="dialog-field__words" id="cron-words" aria-live="polite">every day at 01:00</span>
</div>
{refused && <div className="info-strip info-strip--error" id="cron-refusal" role="alert">…</div>}
```

```css
.dialog-label, .dialog-field { display: flex; flex-direction: column; gap: 5px;
                               font-size: 12px; font-weight: 600; color: #475569; }
.dialog-input, .dialog-select, .dialog-textarea { padding: 7px 10px; border: 1px solid #e2e8f0;
                               border-radius: 6px; font-size: 13px; color: #1e293b; }
.dialog-input--mono          { font-family: 'SF Mono', 'Fira Mono', monospace; }
.dialog-input[aria-invalid="true"] { border-color: #fca5a5; background: #fffafa; }
.dialog-field__hint          { font-size: 11px; font-weight: 400; color: #94a3b8; }
.dialog-field__words         { font-size: 12px; font-weight: 400; color: #374151; }
.dialog-field__words:empty   { margin-top: -5px; }  /* the field's gap, taken back */
```

Rules:

- One label and one native control per field. An optional field says so
  in its placeholder (`Optional`), not in its label.
- `--mono` is for a value a parser reads (a cron), never for prose.
- The hint names the format the field expects, on one line; the words line
  says what the value means, in the server's words, and is empty until the
  server has read the value. It is a polite live region (`aria-live`) that
  is in the page from the start, so its first answer is announced; empty, it
  takes no room (`:empty` takes the field's gap back; never `display: none`,
  which drops the region from the accessibility tree).
- A field the server reads is previewed as the operator types, 300 ms after
  the last change. The previous answer stays on screen while the next one
  loads (no loading strip, which would flash at every keystroke; the
  "Preview list" rule of a loading strip is for previews an explicit action
  started), an answer for an older value never replaces a newer one, and
  the confirming verb is disabled until the preview of the value as typed
  has come back.
- A value the server refused sets `aria-invalid` on its control and shows
  the server's message in an `.info-strip--error` with `role="alert"`; the
  confirming verb is disabled. The control's `aria-describedby` names the
  hint, the words line and, while it is shown, that strip, so a screen
  reader reads the refusal with the field.
- In a dialog that previews a field the operator types (the cron dialog),
  the other failure strips (a refused save, a change made by someone else)
  are `role="alert"` too. When a request ends and the dialog stays open,
  focus goes back to the first field if it was inside the dialog when the
  request started and a disabled control dropped it; focus that moved
  elsewhere stays there. The dialogs that confirm an action (cancelling a
  run, opening a fix PR) keep their error strips as they are.

### Remove row

An action that undoes what an edit dialog edits — `Remove cron` — is not a
way of confirming the edit, so it does not sit in the footer beside the
confirming verb. It has a row of its own above the footer, set apart by a
top rule, with one muted line saying what is left once it is done.

```jsx
<div className="dialog-remove-row">
  <button type="button" className="btn btn--danger">Remove cron</button>
  <span className="dialog-remove-row__note">daily then runs only when triggered.</span>
</div>
```

```css
.dialog-remove-row       { display: flex; align-items: center; gap: 10px;
                           padding-top: 12px; border-top: 1px solid #f1f5f9; }
.dialog-remove-row > .btn { flex-shrink: 0; }
.dialog-remove-row__note { min-width: 0; font-size: 11.5px; color: #6b7280; }
```

Rules:

- `.btn--danger`, with the standard loading and success states
  (`Removing…`, `Removed`); the dialog closes once it succeeds. The note
  sits beside the button and wraps its own text when it is long.
- It asks for no second confirmation: its own row and its note are the
  safeguard, and what it removes can be added again.
- Only an edit of something that exists has a remove row; an add dialog has
  none.
- Status strips (a refused request, a stale version) sit above the remove
  row, so the footer stays the dialog's last row.

## Section headers

Used as the leading row of any `.detail-card` or any content section
inside `.page-content`. One markup pattern for every page.

```jsx
<div className="section-header">
  <div className="section-header__main">
    <span className="section-header__title">Nodes</span>
    <span className="section-header__count">13</span>
  </div>
  <div className="section-header__sub">75% succeeded · avg 8s</div>
</div>
```

```css
.section-header              { padding: 10px 16px; border-bottom: 1px solid #f1f5f9; }
.section-header__main        { display: flex; align-items: center; gap: 8px; }
.section-header__title       { font-size: 11px; font-weight: 700; color: #94a3b8;
                               text-transform: uppercase; letter-spacing: 0.7px; }
.section-header__count       { background: #f1f3f5; color: #555; font-size: 11px;
                               padding: 1px 7px; border-radius: 999px; font-weight: 500; }
.section-header__sub         { font-size: 12px; color: #6b7280; margin-top: 2px; }
```

Rules:

- Count pill is optional. Use it whenever the section contains a
  countable collection (cards, rows, runs). Omit when the content is
  a single artifact (one graph, one form).
- Sub-line is optional. Use for one short metadata string. Never put
  actions in the sub-line — actions go in their own right-aligned
  slot containing only `.btn` elements.
- The existing `.detail-card-header` class is a back-compat alias for
  the title styling and is still allowed on card headers that have
  no count or sub-line.

## Tabs

Two scales: **page-level** tabs live directly under `.page-header`;
**panel-level** tabs live inside a `.detail-card` and replace the
section header on cards that host multiple peer panels.

```jsx
<nav className="tabs" role="tablist">
  <a className="tabs__tab tabs__tab--active" role="tab" aria-selected="true">
    Runs <span className="tabs__count">3</span>
  </a>
  <a className="tabs__tab" role="tab" aria-selected="false">
    Topology <span className="tabs__count">3</span>
  </a>
</nav>
```

```css
.tabs                  { display: flex; gap: 24px; border-bottom: 1px solid #e2e8f0;
                         margin-bottom: 16px; }
.tabs__tab             { padding: 10px 0; font-size: 13px; color: #6b7280;
                         border-bottom: 2px solid transparent; cursor: pointer;
                         display: inline-flex; align-items: center; gap: 8px;
                         text-decoration: none; }
.tabs__tab--active     { color: #111827; border-bottom-color: #111827; font-weight: 500; }
.tabs__count           { background: #f1f3f5; color: #555; font-size: 11px;
                         padding: 1px 7px; border-radius: 999px; font-weight: 500; }
.tabs__tab--active .tabs__count { background: #111827; color: #fff; }

.tabs--panel           { padding: 0 16px; border-bottom-color: #f1f5f9; }
.tabs--panel .tabs__tab { padding: 10px 0; font-size: 12px; }
```

Rules:

- Active state is the inverted-pill + dark underline + 500-weight
  label. Inactive tabs are gray with a transparent underline.
- Labels are short nouns (`Runs`, `Topology`, `Nodes`, `Past Runs`).
  No verbs, no icons, no truncation.
- Active tab is encoded in the URL. Use `?tab=<slug>` for page-level
  tabs and `?panel=<slug>` for panel-level tabs. The default tab
  emits no query string. Unknown values fall back to the default.
- Maximum five tabs per row at either scale. The dashboard's fifth tab
  is `Remediation` — the cross-cutting healing surface that spans all
  services and releases. Above five, the surface needs a different
  pattern — propose it in this file first.
- Always paired with `.tabs__count` when the tabs index a countable
  collection. Same rule as `.section-header__count`.

## Form fields

The sanctioned wrapper for a labelled native control (e.g. a filter
`<select>`). One label plus one control per `.form-field`.

```jsx
<div className="form-field">
  <label htmlFor="release-status-filter">Status</label>
  <select id="release-status-filter">{/* … */}</select>
</div>
```

```css
.form-field          { display: flex; align-items: center; gap: 8px; margin: 8px 0 12px; }
.form-field > label  { font-size: 11px; font-weight: 700; color: #94a3b8;
                       text-transform: uppercase; letter-spacing: 0.7px; }
.form-field > select { font-weight: 500; font-size: 12px; line-height: 1.4;
                       padding: 4px 8px;
                       border: 1px solid #d1d5db; border-radius: 4px;
                       background: #fff; color: #374151; cursor: pointer; }

/* Inline in an action row, the block margin is wrong: it inflates the flex
   row and (being asymmetric) shifts the select off the buttons' centre line. */
.page-action-row .form-field,
.scheduler-card-header .form-field { margin: 0; }
```

Rules:

- The label is the uppercase gray micro-label — same treatment as
  `.section-header__title`.
- Use a native control. Do not build a custom dropdown widget.
- For a filter that drives a list, the `.form-field` sits above the
  `.section-header` of the list it filters. There the `8px 0 12px`
  block margin gives it breathing room.
- When the `.form-field` is used *inline* as an operation selector inside
  an action row (`.page-action-row`, `.scheduler-card-header`), that block
  margin must be zeroed. Otherwise it makes the field taller than its
  sibling `.btn`s and pushes the `<select>` above their shared centre line
  — the field and the buttons must sit on one baseline.

## Tables

- Reuse `.nodes-table` shape. Header row is uppercase 10.5px gray.
  Body rows have hover background and pointer cursor when clickable.
- Long error/log text NEVER expands a row to multiple visual lines by
  default. Truncate with `.nodes-error-short` (single-line ellipsis,
  ~280px). Provide a `more` toggle that expands inline via
  `.nodes-error-full--visible`. Always also link out to the full log
  source.
- A cell may carry a secondary metadata line under its primary value with
  `.nodes-node-subpath` (11.5px gray monospace) — e.g. the offending source
  `file_path` under a compile/seed node id. One line only; never actions.
- A table may carry an optional muted-text qualifier cell (`.nodes-reason`,
  12px slate gray) for a per-row attribute present only on some rows — e.g.
  the failed stage on a rejected release. Rows without the attribute render
  the `.nodes-dash` marker (`—`), never a coloured info-strip. Do not use
  `info-strip--error` as a persistent per-row label.

### Load more

A paged table whose loaded row count is less than the server `total_count`
shows a single centered **Load more** button below the rows:

```jsx
{loaded < total && (
  <div className="nodes-loadmore">
    <button type="button" className="btn btn--secondary" onClick={loadMore}>
      Load more
    </button>
  </div>
)}
```

```css
.nodes-loadmore { display: flex; justify-content: center; padding: 12px; }
```

Rules:

- Render the wrapper only while `loaded < total`. When all rows are
  loaded the wrapper is absent — no disabled button, no "end of list"
  message.
- Use the existing `.btn .btn--secondary` — no new button class.
- No full pagination controls (page numbers, prev/next). Incremental
  append ("load more") is the only sanctioned pattern for this surface.

## Log block

For showing fetched log or code text inline (e.g. a per-node dbt log on
the release detail page). A light, scrollable, monospace surface — never
a dark terminal box.

```jsx
<button type="button" className="btn btn--secondary" onClick={toggle}>view</button>
<a className="btn btn--secondary" href={logUrl} target="_blank" rel="noreferrer">open full log ↗</a>
{open && <pre className="log-block">{content}</pre>}
```

```css
.log-block { margin-top: 8px; background: #f8fafc; border: 1px solid #e2e8f0;
             border-radius: 6px; padding: 10px 12px; max-height: 220px;
             overflow: auto; white-space: pre-wrap;
             font-family: 'SF Mono', 'Fira Mono', monospace;
             font-size: 11.5px; color: #334155; }
```

Rules:

- A toggle (`view` / `hide`, a `.btn--secondary`) reveals the block
  inline; collapsed by default so rows stay scannable.
- Always pair it with a link-out to the full source (`open full log ↗`,
  also a `.btn--secondary`). The inline block is a preview, not the
  system of record.
- A failed fetch renders below as an `.info-strip--error`, not inside
  the `.log-block`.
- This is distinct from the Tables truncate-with-`more` rule, which
  covers short inline error text inside a single cell.

## Service accents and grouped rows

The run detail page groups both of its panels by service. Two patterns
support this; reuse them for any future service-scoped surface.

### Service accent colors

Each service gets a stable accent color from the fixed 8-color palette in
`ui/src/client/service-helpers.ts` (`buildServiceColors`), assigned
by sorted service name so the same service is painted identically on every
surface in a view. Accents are identity cues, deliberately distinct from
the status hues (indigo/green/red) — never use an accent to convey state,
and never use a status hue as an identity accent.

Accents render as a 10px rounded square dot (`.nodes-group-dot`,
`.dag-service-vertex-dot`) and, on graph canvas nodes, as a 4–5px left
border. Because the color is data-driven (resolved per service at runtime)
it is applied via the `style` attribute; this is the sanctioned exception
to the no-inline-style rule, matching the React Flow canvas whose node API
only accepts style objects.

### Node type icons

Every topology node belongs to one of three families — dbt
(`dbt-model` / `dbt-seed` / `dbt-snapshot`), `python-node`, `python-csv` —
and surfaces that render a single node mark it with the family's icon via
the `NodeTypeIcon` component (`ui/src/client/NodeTypeIcon.tsx`):

- **dbt** — the dbt Labs mark in its brand orange (`#FF694A`).
- **python** — the Python logo in its brand blue (`#3776AB`).
- **python-csv** — the Python logo with a small slate table badge overlaid
  bottom-right on a white plate (`.node-type-csv` / `.node-type-csv-badge`);
  no official mark exists, and the badge says "this node loads a file, it
  does not run a script".

The SVG paths are vendored in the component (dbt mark from dbt Labs'
dbt-docs repository, Python logo from simple-icons) so no network fetch is
involved. An empty or unknown `node_type` renders no icon.

Rules:

- The icon is an **identity cue for the tool that owns the node**, like the
  service accent is for the service. It never conveys state; status keeps
  the fill/border, the service accent keeps the left bar.
- It renders wherever a *single* node is shown: graph canvas node labels
  (`.dag-node-label`, 14px), the DAG focus legend title (12px), the node
  detail header (15px). Aggregate surfaces — service vertices, the
  minimap — never carry it: a mixed-family aggregate would lie, and the
  minimap is too small to read it.
- The node detail header pairs the icon with the **exact** `node_type` in a
  neutral inline chip (`.info-strip--neutral .info-strip--inline`); the
  graph shows family only.
- A python-csv node's detail header also carries a `source: <uri>` line
  under the title (`.detail-node-source`, the `.nodes-node-subpath`
  monospace treatment). It renders only when the URI is known — no empty
  `source:` label, and no such line on any other node type.

### Grouped table rows

A `.nodes-table` may group its rows under collapsible per-group header rows:

```jsx
<tr className="nodes-group-row" role="button" aria-expanded={isExpanded}>
  <td colSpan={6}>
    <div className="nodes-group-header">
      <span className="nodes-group-chevron">▸</span>
      <span className="nodes-group-dot" style={{ background: accent }} />
      <span className="nodes-group-name">service-1</span>
      <span className="nodes-group-count">8</span>
      <span className="pill-sm pill-sm--failed">failed</span>
    </div>
  </td>
</tr>
```

Rules:

- The header row spans every column and toggles its group on click and on
  Enter/Space (`role="button"`, `tabIndex={0}`, `aria-expanded`).
- `.nodes-group-count` uses the same count-pill treatment as
  `.section-header__count` / `.tabs__count` (bare number, `#f1f3f5` pill).
- The trailing status pill is the group's rolled-up status using the
  existing `.pill-sm--*` vocabulary (failed > running > pending >
  skipped > succeeded > cancelled).
- Node rows inside an expanded group keep the table's normal columns
  unchanged — grouping never alters row content.
- Collapsed service vertices on the graph canvas mirror the header:
  accent dot, service name, count pill (`.dag-service-vertex-*`), with the
  vertex fill/border painted by rolled-up status.

### Nested attempt rows

When a group row expands into rows that are its *children* — the
remediation attempts of one (release, round) — the children render inside
one contained block hosted by the group's expansion row, never as a second
full-width table whose header competes with the group table's:

```jsx
<tr className="remediation-group remediation-group--open nodes-row--no-border" role="button" aria-expanded>
  <td className="remediation-group__toggle" aria-hidden="true">
    <span className="remediation-chevron remediation-chevron--open">▸</span>
  </td>
  <td className="nodes-node-name">rel-…</td>
  {/* … round · services · nodes · <StatusPill/> · count · PR chips */}
</tr>
<tr className="remediation-group__body nodes-row--static">
  <td colSpan={8}>
    <div className="remediation-attempts">
      <table className="nodes-table remediation-attempts__table">…</table>
    </div>
  </td>
</tr>
```

```css
.remediation-attempts { background: #f8fafc; border-left: 3px solid #c7d2fe;
                        border-radius: 0 8px 8px 0; padding: 2px 4px 6px; }
.remediation-attempts__table thead th { position: static; background: transparent;
                                        font-size: 10px; color: #a8b3c2; }
```

Rules:

- The expansion row's cell indents the block under the release id (34px),
  the block is tinted and carries a 3px left accent rail, and an open group
  row is tinted the same and drops its divider — so the block reads as the
  group row's continuation.
- The inner column labels are a **muted sub-label** of the block (10px,
  lighter than `.nodes-table thead th`, never sticky, no background, never
  wrapped onto a second line), not a peer header. Inner rows keep the table row contract (hover, selected,
  static) and use `.pill-sm` for status.
- Per-item detail lines under a row (a verification run, say) are a
  `.remediation-verif` sub-row: a 10px uppercase label followed by compact
  12px lines, each with its `.btn--secondary` link-out.
- The expander is `.remediation-chevron` in the first cell — a 14px chevron
  that rotates to point down while the group is open and darkens on row
  hover — with the whole row as the click target (Grouped table rows
  keyboard/role contract).
- Pull-request states are `.pr-chip` chips (`--merged` green, `--rejected`
  red, `--open` indigo, anything else the grey base), prefixed by their
  service via `.pr-chip-labeled` (11px muted `__service` + chip, never split
  across lines) and laid out in one wrapping `.remediation-prs` row — a
  proposal split across many services stays one compact cell.
- Status pills for remediation attempts come from `proposalPillClass`
  (`release-helpers.ts`), not `releasePillClass`: `proposed` → succeeded,
  `verifying` → running, `generating` → pending, `failed`/`escalated` →
  failed, `skipped` → skipped. Do not map a new lifecycle vocabulary through
  the release fallback and let everything land on grey.

The run page's node table uses the same block for the attempts of one
task. Its node row is the parent, and the expansion row spans the node
table's seven columns:

```jsx
<tr className="remediation-group remediation-group--open nodes-row--no-border" role="button" tabIndex={0} aria-expanded>
  <td>
    <div className="nodes-node-toggle">
      <span className="remediation-chevron remediation-chevron--open" aria-hidden="true">▸</span>
      <div>
        <div className="nodes-node-name">orders</div>
        <div className="nodes-node-schema">core · sales</div>
      </div>
    </div>
  </td>
  {/* … Status · Attempt (2 / 3) · Error · Started · Completed · Logs */}
</tr>
<tr className="remediation-group__body nodes-row--static">
  <td colSpan={7}>
    <div className="remediation-attempts">
      <table className="nodes-table remediation-attempts__table">
        {/* # · Status · Class · reason · Wait · Run time · Log */}
      </table>
    </div>
  </td>
</tr>
```

Rules:

- The expander chevron sits in the Node cell, before the node name
  (`.nodes-node-toggle`), not in a column of its own: the node table keeps
  its seven columns, and the expansion row has `colSpan={7}`. The block's
  indent lines it up under the node name.
- The inner columns are `#`, `Status`, `Class · reason`, `Wait`,
  `Run time` and `Log`. `#` is the 1-based attempt number; `Class · reason`
  joins the failure class and reason (`.nodes-reason`, either alone when
  only one is known, `.nodes-dash` when neither is); `Wait` is
  `started_at − queued_at` and `—` when either is unknown; `Run time` is the
  attempt's recorded run time; `Log` is an `open log ↗` link-out
  (`.nodes-log-link`) when the attempt has a log, `—` otherwise. Inner rows
  are `nodes-row--static`: nothing in the block is a click target except
  its links.
- Status pills for task attempts come from `attemptPillClass`
  (`detail-page-helpers.ts`): `dispatched` and `queued` get the pending
  pill; `running`, `succeeded`, `failed`, `cancelled` and `stopped` get their own.
- Task status pills (`.pill-sm--dispatched`, `.pill-sm--queued`) are neutral,
  like pending: nothing of the task runs yet. A `queued` pill carries the
  title "waiting for a slot" (`taskStatusTitle`). Wherever task statuses are
  ordered or counted (the nodes table, the run-progress header, the swimlane
  legend, a service's rolled-up status, the dashboard run card's summary
  row) dispatched and queued come just before running.
- Two statuses mean a cancel is in progress, and both are neutral-warning
  (`#fef3c7` fill, `#92400e` text, the `.info-strip--warning` palette): a run
  is `cancelling` (title "cancelling — waiting for its Jobs to stop") and a
  task is `stopping` (title "stopping — its Job is being deleted"). Neither has
  ended: a cancelling run counts as active (the run page keeps polling it, the
  dashboard card keeps its Cancel button and is not offered a rerun), and a
  stopping task is not counted complete. Wherever tasks are ordered or counted
  `stopping` comes between queued and running (a service containing one rolls
  up to `stopping`, above running); on the swimlane it is a dashed amber node.
  An attempt whose Job is gone after a cancel is `stopped` and reads with the
  muted `.pill-sm--stopped`, like cancelled. Map task and run statuses to pills
  with `taskPillClass`, never a template string, so an unknown status reads
  pending instead of an unstyled pill.
- A pending task that waits for another run to finish writing its node
  (`wait_reason` `node_lease`) carries a "waiting for node" pill
  (`LeaseWaitPill`, `.pill-sm--waiting`) beside its `pending` pill, never in
  place of it: the status stays what is counted and filtered, the pill says
  why. It is neutral like queued (`#f1f5f9` fill, `#475569` text) with a
  1px dashed `#94a3b8` border, the pending node's dash. Its title is
  `leaseWaitTitle`: "waiting for run <first 8 characters of the run id> to
  finish writing this node since <time>", or "waiting for its turn to write
  this node since <time>" while no run holds the node. While a run holds the
  node the pill is a link to `/run/<run id>` (`runPagePath`), which opens
  that run on its schedule's run page; its click never reaches the row
  beneath. On the node table the two pills share the status cell
  (`.nodes-pills`, 4px gap); on the swimlane the pill (`.swim-wait`, 9.5px)
  sits on the node's top border, right-aligned, and a collapsed lane puts
  the same title on the node's cell.
- A node page's run row shows the failure class and reason of its latest
  attempt (`.nodes-reason`, joined with ` · `) under its status pill.
- The node row's own cells stay the task's summary: its Attempt cell reads
  `attempt + 1 / max_retries + 1`, and its Error, Started, Completed and
  Logs cells come from the task's highest-numbered attempt. A task whose
  `max_retries` is 0 shows the attempt number alone (`1`, not `1 / 1`):
  0 is what a task with no stored retry budget, or the default one,
  carries, so a denominator would claim a single allowed attempt it may
  not have.
- A task without attempts (pending, or skipped before it ran) has no
  expander: no chevron, no `role="button"`, no click or keyboard handler.
  Its Node cell keeps a chevron-wide spacer so node names stay aligned.
- A node row whose click already selects the node (the nodes panel beside
  a graph) never doubles as an attempts toggle; only the run page's node
  table, whose rows select nothing, expands into attempts.

### Inline actionable rows

A row whose item still needs a human decision renders its `.detail-card`
expanded, in place, inside the table — never in a separate section above
or below it. There is no separate "first row" or "top of list" special
case: the trigger is a predicate over the item's own fields, evaluated per
row. **The card's primary action button is gated by that same predicate**
— define it once and route both the expansion check and the button gate
through it, so the two can never drift apart. The remediation panel is the
reference implementation:

```jsx
// isActionable is the single source of truth for "this item still needs a
// human decision." Both the row's auto-expansion and the card's primary
// action button read it — never duplicate the condition inline. Express
// the predicate in terms of the server's own precondition for the action
// (here, the same '' / 'failed' claim-state check BeginPullRequest's
// compare-and-set enforces) rather than inferring it from a side effect
// like an unset URL field — a field that goes from empty to set on the
// happy path might also stay empty while the item is claimed but not yet
// resolved, which would wrongly read as still-actionable.
function isActionable(p) {
  return p.status === 'proposed' && p.source_resolved
    && (p.pr_state === '' || p.pr_state === 'failed');
}

const showCard = isActionable(p) || isSelected;
// Whichever row sits directly above its own card — auto-expanded or
// manually selected — drops its border so it doesn't draw a hairline
// against that card. The card row keeps its own border; that's what
// separates it from the next proposal.
const compactRowClass = [
  isActionable(p) ? 'nodes-row--static' : '',
  isSelected ? 'nodes-row--selected' : '',
  showCard ? 'nodes-row--no-border' : '',
].filter(Boolean).join(' ');

<Fragment key={p.id}>
  <tr
    className={compactRowClass}
    onClick={isActionable(p) ? undefined : toggle}
    role={isActionable(p) ? undefined : 'button'}
    tabIndex={isActionable(p) ? undefined : 0}
    aria-expanded={isActionable(p) ? undefined : isSelected}
    onKeyDown={isActionable(p) ? undefined : (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); }
    }}
  >
    {/* compact cells, unchanged */}
  </tr>
  {showCard && (
    <tr className="nodes-row--static">
      <td colSpan={5}>
        <div className="detail-card">
          {/* rationale */}
          {isOperator && isActionable(p) && <button className="btn btn--secondary">{/* primary action */}</button>}
        </div>
      </td>
    </tr>
  )}
</Fragment>
```

```css
/* Non-interactive row: an auto-expanded actionable row, or the row hosting
   an expanded detail card. Neither responds to click, so neither gets the
   table's default pointer cursor or hover affordance. */
.nodes-table tbody tr.nodes-row--static { cursor: default; }
.nodes-table tbody tr.nodes-row--static:hover { background-color: transparent; }
/* Applied to whichever row sits directly above its own expanded card —
   auto-expanded or manually selected, either way — so the two don't draw a
   hairline against each other. The card row itself keeps its border,
   which is what separates it from the next proposal row. */
.nodes-table tbody tr.nodes-row--no-border { border-bottom: none; }
```

Rules:

- **Expansion depends only on item state, never on viewer role.** Everyone
  sees the card and its rationale; only the action button inside stays
  role-gated (`currentUser?.role === 'operator'`, unchanged from the
  ordinary button-gating rule) in addition to `isActionable`.
- **One predicate function, two call sites.** Do not write the expansion
  condition and the button gate as separately-maintained boolean
  expressions, even if they look identical today — that's exactly the setup
  that lets them silently diverge later. Extract `isActionable` (or
  equivalent) and call it from both places.
- **Collapse on resolution.** The moment `isActionable` goes false — the
  claim moves out of its retryable state (e.g. `pr_state` leaving `''` /
  `failed` for `opening`), or the item's own lifecycle field moves it off
  the actionable state (e.g. `status` leaving `proposed` for `skipped` or
  `escalated`, or `pr_state` resolving further to `open`, `merged` /
  `rejected`) — the row reverts to a normal compact row on the next render.
  No transition, no manual dismiss. Know which field on your domain object
  actually carries "resolved" — don't assume outcome and lifecycle state
  live in the same column, and don't gate on a field (like a URL) that is
  merely a side effect of the state transition rather than the state itself.
- **Creating the PR from an auto-expanded row must not collapse it into
  nothing.** The action inside the card mutates the same field the
  predicate reads (e.g. the server-side claim moving `pr_state` off `''`/
  `failed`), so the row stops being auto-expanded in the same render the
  action completes. Only set locally what the client actually knows to be
  true — the claim step guarantees `pr_state='opening'`, so that is the
  value to mirror locally, not a terminal state like `open` that a
  best-effort recording step further downstream has not yet confirmed;
  follow up with a refetch of the authoritative list so the row settles on
  whatever the server actually recorded. If the "selected"
  state that keeps a manually-opened row open is a separate piece of state
  from the auto-expand predicate, the completing action must explicitly set
  that selected state too — otherwise the row collapses at the exact moment
  it should show the result (the new `open PR ↗` link) and the operator's
  one action looks like it did nothing.
- **Manual selection still works for collapsed rows**, using the same
  click-to-toggle behaviour tables already have. Mark the selected row with
  `.nodes-row--selected` (already defined for the Nodes table) and scroll
  its detail row into view (`ref` + `scrollIntoView({ behavior: 'smooth',
  block: 'nearest' })`) — an auto-expanded row needs neither, since it is
  already in place and already visible.
- **Collapsed rows follow the Grouped table rows keyboard/role contract**
  above: `role="button"`, `tabIndex={0}`, Enter/Space activation,
  `aria-expanded`. An auto-expanded row gets none of those attributes — it
  isn't a toggle, so it has nothing to expose to assistive tech beyond the
  card that's already rendered.
- **`.nodes-row--static`** removes the table's default pointer cursor and
  hover background from any row that isn't a click target: the auto-expanded
  compact row, and the `<tr>` hosting any expanded detail card (auto or
  manual). Use it whenever a `.nodes-table` row exists but isn't
  interactive.
- **`.nodes-row--no-border`** removes the row divider from whichever compact
  row sits directly above its own expanded card — this is independent of
  `.nodes-row--static`/`.nodes-row--selected`, since a manually-selected row
  needs the no-border treatment too, not just an auto-expanded one. Never
  put `border-bottom: none` on the card row itself; the card row's border is
  what separates one proposal's card from the next proposal's row.

## Schedule cards

Each schedule on the home page's Runs tab is a `SchedulerCard`
(`ui/src/client/SchedulerCard.tsx`). After the name and the status badge of
its last run, its header says whether and when the schedule runs on its own.

```jsx
<div className="scheduler-card-header">
  <span className="schedule-name">daily</span>
  <span className="status-badge status-succeeded">succeeded</span>
  <span className="schedule-pill schedule-pill--scheduled">scheduled</span>
  <span className="schedule-cron" title="every day at 01:00">0 1 * * * · Europe/Paris</span>
  <span className="timestamps">next 28 Oct 01:00</span>
  <span className="timestamps">last run 01:00:04</span>
  <div className="scheduler-card-actions">
    {/* Operation select, Trigger run, Cancel */}
    <button type="button" className="btn btn--secondary">Edit cron</button>
  </div>
</div>
```

```css
.schedule-pill            { display: inline-block; padding: 2px 8px; border-radius: 12px;
                            font-size: 11px; font-weight: 500; text-transform: uppercase;
                            letter-spacing: 0.05em; border: 1px solid transparent; }
.schedule-pill--scheduled { background: #ecfdf5; color: #047857; border-color: #a7f3d0; }
.schedule-pill--paused    { background: #fef3c7; color: #92400e; border-color: #fde68a; }
.schedule-pill--manual    { background: #f3f4f6; color: #6b7280; border-color: #e5e7eb; }
.schedule-cron            { color: #6b7280; font-size: 12px; font-family: monospace;
                            white-space: nowrap; }
.schedule-cron--paused    { text-decoration: line-through; }
.scheduler-card-header    { display: flex; flex-wrap: wrap; align-items: center;
                            gap: 12px; row-gap: 8px; }
.scheduler-card-actions   { display: flex; flex-wrap: wrap; align-items: center;
                            gap: 12px; margin-left: auto; }
.timestamps               { color: #6b7280; font-size: 12px; white-space: nowrap; }
```

Rules:

- **One pill per card**: `scheduled` (a cron that fires), `paused` (a cron
  that starts nothing on its own) or `manual` (no cron: it runs only when
  triggered). The pill says whether the schedule runs on its own; the
  status badge before it says how its last run went. The pill's border sets
  it apart from the borderless status badge.
- **The cron span** is the raw cron and its timezone, monospace, on a
  schedule that has a cron, struck through (`.schedule-cron--paused`) while
  paused; its `title` is the cron in words. One muted `.timestamps` fact
  follows it: `next 28 Oct 01:00`, in the schedule's timezone, or
  `paused by bob@example.com · 27 Oct 14:02`, in the viewer's. A cron that
  fires no more has no fact. A manual schedule shows `no cron: runs only when
  triggered` in place of the span.
- **The header wraps** when its facts and controls do not fit: each fact
  keeps to one line (`white-space: nowrap`), and the controls — Operation,
  Trigger, Cancel, Edit cron — stay together in `.scheduler-card-actions`,
  right-aligned, and move to the next line as one group.
  `ui/tests/client/styles-schedule-card-header.test.ts` pins this.
- **`Edit cron`** (scheduled or paused) / **`Add cron`** (manual) is a
  `.btn--secondary`, last of the card's controls, and opens the cron dialog. Only an
  operator sees it, by the session's role (`useCurrentUser()`); a viewer
  sees no button rather than a disabled one.
- **A scheduled name with no models** (no model of the live release has its
  tag) carries an `.info-strip--warning` under the header: "No model in the
  live release has the tag **daily**. Each fire records a failed run until a
  release brings them back, or you remove its cron." A paused or manual
  schedule fires nothing and has no such strip.

## Snapshot tiles

Used inside the homepage `Topology` tab. One tile per schedule
with at least one active topology node. Clicking the tile navigates;
the whole tile IS the click target.

```css
.snapshot-tile-grid { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 12px; }

.snapshot-tile      { display: flex; flex-direction: column; align-items: flex-start;
                      gap: 4px; min-width: 180px; padding: 12px 16px;
                      background: #fff; border: 1px solid #e2e8f0;
                      border-radius: 6px; cursor: pointer; text-align: left;
                      font: inherit;
                      transition: background .15s, border-color .15s; }

.snapshot-tile:hover { background: #f8f9fa; border-color: #c7d2fe; }

.snapshot-tile__name { font-weight: 600; font-size: 13px; color: #111827; }
.snapshot-tile__meta { font-size: 12px; color: #6b7280; }
```

Content rules:

- One bold title line (the schedule name).
- Exactly one metadata line: `N nodes · updated Xm ago`.
- Never embed action buttons inside a tile. The tile is the action.
- For "no topology yet" states render an `.info-strip--neutral` in
  place of the grid — do not render empty tiles.

## Provenance line

One line directly under the header of a detail page saying what the page
is about, before any strip or section. Used on the release page
(`Changes core · acme/demo @ c2d1add ↗`) and the verification-run page
(`Verification run for release rel-1 · attempt 2 · service core · dbt`).

```jsx
<p className="page-sub">
  Changes <strong>core</strong>
  <span className="page-sub__sep">·</span>
  <a href={rel.commit_url} target="_blank" rel="noopener noreferrer">acme/demo @ c2d1add ↗</a>
</p>
```

```css
.page-sub      { display: flex; flex-wrap: wrap; align-items: center; gap: 6px;
                 margin: -6px 0 14px; font-size: 12.5px; color: #6b7280; }
.page-sub strong { color: #1e293b; font-weight: 600; }
.page-sub a    { color: #4338ca; text-decoration: none; }
.page-sub__sep { color: #cbd5e1; }
```

Rules:

- One line, plain words; `strong` marks the subject, never a status.
- A link-out ends with `↗` and opens in a new tab; a fact that cannot be
  linked (no repo or commit recorded) is simply absent, never a dead link.
  The client never builds an external URL itself: the ui server attaches
  `commit_url` under the install's GitHub host (derived from
  `GITHUB_API_BASE_URL`), so a GitHub Enterprise install links to its own
  host.
- Separate facts with `.page-sub__sep`, not punctuation inside the text.

## Service tiles

The set of service images a run executes with, on the release and
verification-run detail pages (`ServiceTiles`, `ui/src/client/ServiceTiles.tsx`).
One tile per service. Tiles are identity, not navigation: nothing happens
on click, so there is no hover state.

```jsx
<div className="section-header">
  <div className="section-header__main">
    <span className="section-header__title">Services</span>
    <span className="section-header__count">7</span>
  </div>
  <div className="section-header__sub">core is new in this release · 6 carried over from prod</div>
</div>
<div className="service-tile-grid">
  <div className="service-tile service-tile--changed">
    <div className="service-tile__head">
      <span className="nodes-group-dot" style={{ background: accent }} />
      <span className="service-tile__name">core</span>
      <span className="pill-sm pill-sm--changed">changed</span>
    </div>
    <div className="service-tile__tag">c2d1add</div>
  </div>
  …
</div>
```

```css
.service-tile-grid  { display: grid; gap: 10px; margin: 12px 0 18px;
                      grid-template-columns: repeat(auto-fill, minmax(210px, 1fr)); }
.service-tile       { display: flex; flex-direction: column; gap: 5px; min-width: 0;
                      padding: 10px 12px; background: #fff;
                      border: 1px solid #e2e8f0; border-radius: 6px; }
.service-tile__head { display: flex; align-items: center; gap: 8px; min-width: 0; }
.service-tile__name { flex: 1; font-size: 13px; font-weight: 600; color: #111827;
                      overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.service-tile__tag  { font-family: 'SF Mono', 'Fira Mono', monospace; font-size: 11.5px;
                      color: #6b7280; word-break: break-all; }
.service-tile--changed { border-color: #c7d2fe; background: #fbfbff; }
.pill-sm--changed   { background: #eef2ff; color: #4338ca; }
```

Rules:

- Tiles sort by service name so a service sits in the same spot on every
  release; the accent dot is the service's colour from `buildServiceColors`
  (see Service accents), never a status hue.
- Exactly one tile carries `--changed` and the `changed` chip: the service
  whose image is new in this run. The section sub-line says so and counts
  the rest as carried over, naming the kind of run the page shows
  (`subject`: "release", or "verification run") and where the rest came
  from (`carriedFrom`: "prod" for a candidate; for a verification run, the
  release it verifies, since its image set is that release's with the fixed
  service swapped in). When the changed service is the only one, the
  sub-line stops after "is new in this release". Unknown changed service:
  no chip, no sub-line.
- The image tag is whatever the release recorded — a short tag or a full
  registry reference — in monospace, wrapping inside its tile.
- No images recorded yet: render nothing, not an empty section.

## Pipeline timeline

A run's path through the pipeline, on the release and verification-run
detail pages (`PipelineTimeline`, `ui/src/client/PipelineTimeline.tsx`):
one step per recorded transition, in order, joined by connectors.

```jsx
<div className="section-header">…Timeline…</div>
<ol className="release-timeline">
  <li className="release-timeline__step">
    <span className="release-timeline__connector" aria-hidden="true" />
    <div className="release-timeline__body">
      <span className="pill-sm pill-sm--running">compiling</span>
      <span className="release-timeline__at">2026-09-05 14:25:36</span>
    </div>
  </li>
  <li className="release-timeline__step release-timeline__step--upcoming">
    <span className="release-timeline__connector" aria-hidden="true" />
    <div className="release-timeline__body">
      <span className="pill-sm">seed_building<span className="sr-only"> (upcoming)</span></span>
      <span className="release-timeline__hint">if seeds changed</span>
    </div>
  </li>
</ol>
```

```css
.release-timeline           { display: flex; align-items: flex-start; list-style: none;
                              margin: 14px 0 18px; padding: 0 0 4px; overflow-x: auto; }
.release-timeline__step     { display: flex; align-items: flex-start; }
.release-timeline__connector { width: 28px; height: 1px; margin-top: 9px; background: #cbd5e1;
                              flex-shrink: 0; }
.release-timeline__step:first-child .release-timeline__connector { display: none; }
.release-timeline__body     { display: flex; flex-direction: column; gap: 4px; }
.release-timeline__at       { font-size: 11.5px; color: #64748b; white-space: nowrap;
                              font-variant-numeric: tabular-nums; }
.release-timeline__step--upcoming .pill-sm { background: transparent; color: #94a3b8;
                              border: 1px dashed #cbd5e1; }
.release-timeline__step--upcoming .release-timeline__connector { height: 0; background: transparent;
                              border-top: 1px dashed #cbd5e1; }
.release-timeline__hint     { font-size: 11px; color: #94a3b8; white-space: nowrap; }
```

Rules:

- Each step's pill is the same `pill-sm` the header pill uses for that
  status (`releasePillClass`), with the transition's timestamp beneath it.
- While the run is still in flight, the stages it has not reached yet
  follow as ghosted, undated steps, so a two-step timeline reads as
  progress rather than as a list. The ghosts are the path the run's kind
  takes (`upcomingStages`, fed `manifest_kind` and `bootstrap`): a python
  run has no `compiling` and no `seed_building`; a bootstrap release ends
  at `parsing`. A stage that does not run on every path carries a caption
  saying what it depends on (`.release-timeline__hint`: "if seeds changed",
  "unless nothing changed"), so the preview never promises a stage the run
  will silently skip. A ghost's pill carries a visually hidden
  " (upcoming)" (`.sr-only`) so a screen reader does not hear it as a
  completed step. A finished run has no ghosts.
- The list scrolls horizontally inside its own container on narrow
  viewports; it never wraps and never scrolls the page sideways.

## Sign-in page

The only page rendered to an unauthenticated user. Uses the standard `.page`
shell; the single surface is a centered card.

```css
.signin-card        { max-width: 360px; margin: 96px auto 0; background: #fff;
                      border: 1px solid #e2e8f0; border-radius: 6px; padding: 24px;
                      display: flex; flex-direction: column; gap: 12px;
                      align-items: flex-start; }
.signin-card__hint  { font-size: 12px; color: #6b7280; margin: 0; }
```

Rules:

- The card title uses the `.section-header__title` micro-label treatment.
- The sign-in action is the page's single confirming action and uses
  `.btn--primary` (same rationale as a modal's confirming verb). It is an
  anchor to `/auth/login`, not a JavaScript handler.
- Login errors (`no_role`, `login_failed`) render as an `.info-strip--error`
  inside the card, above the button.
- No password fields, ever — authentication is delegated to the deployer's
  identity provider.

## User menu

Identity + sign-out, the last item of `.page-actions` in every page's
`.page-header` (`PageHeader` renders it; pages never place it themselves).

```css
.user-menu          { margin-left: auto; display: flex; align-items: center; gap: 8px; }
.user-menu__email   { font-size: 12px; color: #6b7280; max-width: 240px;
                      overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
```

The sign-out action is a `.btn--secondary` following the standard button state
rules (`Sign out` → `Signing out…` with `.is-loading`).

## Assistant panel

The agent-chat chat, docked as a fixed 360px right-hand sidebar. It reuses
the system's buttons and info-strips; the only things it adds are message
bubbles, an inline confirm prompt, and an input row. It introduces no new
button or banner language.

```css
.chat-panel          { flex: 0 0 360px; display: flex; flex-direction: column;
                       border-left: 1px solid #e2e8f0; background: #f8f9fa;
                       height: 100vh; position: sticky; top: 0; }
.chat-panel__header  { display: flex; align-items: center; gap: 8px;
                       padding: 12px 14px; border-bottom: 1px solid #e2e8f0;
                       background: #fff; }
.chat-panel__title   { /* .section-header__title treatment */
                       font-size: 11px; font-weight: 700; color: #94a3b8;
                       text-transform: uppercase; letter-spacing: 0.7px;
                       margin-right: auto; }
.chat-msg            { padding: 8px 12px; border-radius: 6px; font-size: 13px;
                       line-height: 1.45; max-width: 88%; }
.chat-msg--user      { align-self: flex-end; background: #4338ca; color: #fff; }
.chat-msg--assistant { align-self: flex-start; background: #fff;
                       border: 1px solid #e2e8f0; }
.chat-msg--tool      { align-self: flex-start; color: #6b7280; font-size: 12px; }
```

Rules:

- **Header** carries the `Assistant` title using the `.section-header__title`
  micro-label treatment. `Stop` (only while streaming) and `New chat` are
  `.btn .btn--secondary`, right-aligned.
- **Message bubbles.** User messages are solid indigo, right-aligned; assistant
  messages are white cards with an `#e2e8f0` border, left-aligned; both use a
  6px radius. The tool line (`running <code>…</code>…`) is gray 12px,
  left-aligned. Inline `code` inside a bubble renders at 11.5px monospace, the
  same idiom as the log block.
- **Confirm prompt.** When the assistant asks to run a tool, the prompt renders
  inline as an `.info-strip--info` followed by a `.chat-confirm__actions` row
  with `Confirm` (`.btn--primary`, the confirming verb) and `Deny`
  (`.btn--secondary`). Once resolved it collapses to muted italic outcome text
  (`.chat-confirm__outcome`: `confirmed` / `denied`) with no active buttons.
  This is a deliberate, bounded exception to the Modals rule: a conversational
  agent must not throw a full-screen overlay for every tool it wants to run, so
  the confirmation stays in the message flow. It does not license inline
  confirmations anywhere else — outside the chat panel, confirmations are still
  modals.
- **Errors** render as a standard `.info-strip--error`, left-aligned in the
  message column. The panel has no bespoke error style.
- **Input row.** A bordered input (4px radius, 12px control font) with a
  trailing `Send` (`.btn--secondary`). The input and `Send` are disabled while
  the assistant is streaming or a confirm is pending.

## Things to avoid

- New `.foo-btn` or `.foo-banner` classes — extend `.btn` / `.info-strip`
  variants instead, and update this file.
- `style="…"` inline overrides on visual elements.
- Checkmark glyphs in button labels (`✓ Done`). Use the green
  `.is-success` state.
- Inline red error text next to buttons. Use `.info-strip--error`
  below the action row.
- Fixed widths that break responsive flow (other than the
  `.page-content--readable` opt-in cap).
- Toast notifications, snackbars, or any feedback layer that doesn't
  match the button-colour + info-strip language.

## Adding to the system

If a need arises that doesn't fit:

1. First check whether an existing pattern fits with a different copy
   choice.
2. If not, propose the addition here (new variant, new pattern) BEFORE
   shipping the UI change. The whole point of this file is to keep
   ui feeling like the same product.
