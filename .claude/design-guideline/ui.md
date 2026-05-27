# Continuo UI design guidelines

Forward-looking reference for any change in `ui-service/`. Describes the
design system as it stands once the
`2026-05-23-ui-button-and-layout-standardization` work has landed. If a
new page or component does not fit one of these patterns, add to the
system; do not invent a parallel one.

## Page shell

Every top-level route renders the same shell. No exceptions.

```jsx
<div className="page">
  <header className="page-header">{/* back link, title, badges, actions */}</header>
  <main className="page-content">{/* full-width */}</main>
</div>
```

```css
.page         { min-height: 100vh; background: #f8f9fa; padding: 20px 24px; }
.page-header  { display: flex; align-items: center; gap: 12px;
                padding-bottom: 16px; margin-bottom: 16px;
                border-bottom: 1px solid #e2e8f0; }
.page-content {} /* full-width */
.page-content--readable { max-width: 960px; margin: 0 auto; }
```

Rules:

- Same top padding (20px) and side padding (24px) on every page.
- Use `.page-content--readable` only for list/feed pages where row width
  should stay scannable on wide monitors (e.g. the homepage schedule
  list). The shell itself stays full-width.
- The header bar separator (`border-bottom`) is always present; it
  anchors the page visually.

## Buttons

One class for the shape, three variants, two orthogonal states.

```css
.btn            { display: inline-flex; align-items: center; gap: 6px;
                  padding: 4px 12px; font: 500 12px/1.4 inherit;
                  border-radius: 4px; border: 1px solid transparent;
                  background: #fff; color: #374151; cursor: pointer;
                  white-space: nowrap;
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
Row 1: identity      ← Back   page-title   [STATUS]   ⚠ context chip
Row 2: actions                                  [Action]  [Action]  [Action]
```

- Identity row carries: back navigation, page title, status pill,
  *compact* info-strips (chips).
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
  - Radio choices with a short description per option.
  - Footer: `Cancel` (`.btn--secondary`) on the left, the confirming
    verb (`.btn--primary`) on the right.
- The confirming verb is the action the modal performs (`Rerun`,
  `Cancel run`, `Trigger`). Not "OK" or "Submit".

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
- Maximum four tabs per row at either scale. Above four, the surface
  needs a different pattern — propose it in this file first.
- Always paired with `.tabs__count` when the tabs index a countable
  collection. Same rule as `.section-header__count`.

## Tables

- Reuse `.nodes-table` shape. Header row is uppercase 10.5px gray.
  Body rows have hover background and pointer cursor when clickable.
- Long error/log text NEVER expands a row to multiple visual lines by
  default. Truncate with `.nodes-error-short` (single-line ellipsis,
  ~280px). Provide a `more` toggle that expands inline via
  `.nodes-error-full--visible`. Always also link out to the full log
  source.

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
   ui-service feeling like the same product.
