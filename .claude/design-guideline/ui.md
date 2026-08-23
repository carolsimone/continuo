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
(`dbt-model` / `dbt-seed` / `dbt-snapshot`), `python-model`, `python-csv` —
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

Identity + sign-out, right-aligned in the homepage `.page-header`.

```css
.user-menu          { margin-left: auto; display: flex; align-items: center; gap: 8px; }
.user-menu__email   { font-size: 12px; color: #6b7280; }
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
