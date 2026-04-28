# UI Run Workflows — Design Spec

Date: 2026-04-28

---

## Problem

The ui-service currently exposes two run-triggering affordances on the schedule
page:

- **Rerun** — re-execute the existing snapshot of the schedule's static DAG.
- **Trigger schedule** — capture a new snapshot from the latest graph version
  and execute it from scratch.

These two actions are placed adjacent and are easily conflated. End users
coming from Airflow/Dagster carry a mental model where "rerun from failed"
silently picks up the latest model code. Our `Rerun` does **not**: it executes
the same model versions that were snapshotted, by design — we are stricter
about governance and want every run to have an explicit, recorded snapshot.

A third workflow is also missing from the current UI: **ad-hoc test runs of a
specific node** during model development. Without an intentional surface, this
either lands incorrectly inside the schedule context or gets bolted on as a
catch-all and breaks the governance promise.

---

## Design Principles

1. **Every run has a recorded snapshot.** The snapshot version is part of the
   run identity.
2. **Word reservation.** "Rerun" always means re-execute an existing snapshot
   unchanged. "Trigger" always means create a new snapshot from latest and run
   the full DAG. Neither word is overloaded for any other meaning.
3. **Separation by location, not adjacency.** The three workflows live on
   three different surfaces. They are never presented as adjacent buttons.
4. **Schedule pages are reliability surfaces.** They show only scheduled runs
   so "is this schedule healthy?" can be answered at a glance. Test runs do
   not appear there.
5. **Test runs follow the model, not the schedule.** Engineers iterate on
   models, so the test surface lives on the model/node — not nested under the
   schedule.

---

## The three workflows

A run is defined by three orthogonal axes:

| Axis            | Values                                              |
|-----------------|-----------------------------------------------------|
| Snapshot        | existing · new from latest                          |
| Task selection  | full DAG · failed tasks · subgraph/single node      |
| Trigger source  | schedule · manual full · manual partial (test)      |

The three workflows fix specific points in this space:

| Workflow  | Snapshot         | Task selection      | Trigger source      | UI surface              |
|-----------|------------------|---------------------|---------------------|-------------------------|
| Rerun     | existing         | full or failed-tasks| manual              | run detail              |
| Trigger   | new from latest  | full                | manual or schedule  | schedule list/detail    |
| Test run  | new from latest  | subgraph/single node| manual partial      | model/node detail       |

---

## Surfaces

### Schedule list (`/schedules`)

Only affordance: **Trigger run**.

Confirm modal: *"Create snapshot vN+1 from current graph and execute. Continue?"*

No rerun. No test run.

### Schedule detail (`/schedules/:id`)

- Single primary action at the top: **Trigger run** (same modal as the list).
- Run history table — rows show `snapshot vN · status · started · duration`.
  Rows are navigation-only; no inline rerun action. Click → run detail.
- No tabs. No nested test run history.

### Run detail (`/runs/:id`)

Shared across all three workflows. Carries a badge: `Scheduled` / `Manual` /
`Test`.

Primary actions:

- **Rerun snapshot vN** — always with explicit version suffix in the label.
- **Rerun failed tasks** — when applicable. Partial rerun bound to the same
  snapshot.

Governance banner shown when this run's snapshot is behind the schedule's
current snapshot:

> ⚠ Snapshot vN is M commits behind latest (vK). Rerun executes vN unchanged.
> To pick up new code → **[Trigger latest]**

The banner is the moment users who *thought* rerun would pick up new code
learn otherwise, with a one-click escape to the right action.

### Model/node detail

Reached by clicking a node in the DAG view. Carries:

- **Test run this node** and **Test run + downstream** affordances.
- Test run history list scoped to this node, with deep links to run detail.

Triggering a test run opens a confirm modal:
*"Create snapshot vN+1 and execute `<node>` only. This is a test run — it
won't affect schedule history."*

Test run history is local to the node. There is no global "test runs" list.
Discoverability of "what did I just kick off?" is handled by a toast/notification
with a deep link to the run detail page, plus a small "Recent activity" item in
the user menu (deferred).

---

## Backend prerequisites

Net-new requirements that the orchestrator/state services must support before
this UI is implementable:

1. **Run carries snapshot version.** Must be exposed on the run detail query.
2. **Compare a run's snapshot against the schedule's current snapshot.**
   Needed for the "M commits behind latest" banner. Likely an
   `OrchestratorQuery` method returning
   `(run_snapshot_version, schedule_latest_snapshot_version, distance)`.
3. **Trigger source / run kind on the run record.** The
   `Scheduled` / `Manual` / `Test` badge requires this to be stored on run
   creation and returned on the query.
4. **Test run primitives.** A run must accept a partial task selection
   (single node, optional downstream) at creation time. The snapshot is
   still produced from the latest graph, but execution is scoped.

These are listed for visibility. Exact API shapes will be designed when
implementation begins.

---

## Explicitly rejected alternatives

- **A "Test runs" tab on the schedule detail page.** Pollutes the schedule's
  reliability surface. Test runs are a node-level concern.
- **A top-level "Test runs" list.** Over-promotes ad-hoc work to a primary
  navigation entry. Discoverability via the model is sufficient.
- **A single primary action with a snapshot selector
  ("Run snapshot vN ▾ / latest")** on the schedule page. Makes the
  destructive choice (run latest) too easy and re-conflates the two
  governance positions the design is trying to keep apart.
