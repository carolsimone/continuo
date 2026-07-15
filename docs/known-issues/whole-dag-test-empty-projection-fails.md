# Whole-DAG Test with no test-bearing nodes finalizes as `failed`

## Resolved (2026-07-15)

Fixed: the whole-DAG Test all-gated case now surfaces `snapshot.ErrNoTests`
(reason `no_tests`) instead of `snapshot.ErrEmptyProjection`, matching the
single-node gate. State finalizes the run via the renamed
`MarkDispatchTerminal` (formerly `MarkDispatchFailed`), which branches on the
dispatch-failed reason: the benign `no_tests` reason finalizes the run as the
new terminal status `skipped`, while every other reason (`target_not_found`,
`empty_projection`, `invalid_node_type`, `rerun_of_test_unsupported`) still
finalizes `failed`. `empty_projection` now means only a broken `operation=run`
DAG with zero active nodes. Terminal run outcomes are now `succeeded | failed
| cancelled | skipped`. See `docs/arch/services/orchestrator.md` and
`docs/arch/services/state.md` for the current-state description.

Found: 2026-07-15, during prod QA of the dbt-command-coverage work (unrelated to it).

## Symptom

Triggering a schedule's **whole-DAG run in Test mode** where none of its nodes
have tests produces a run that shows as **`failed`** in the UI, with 0 nodes and
an empty dependency graph. Observed on the `e2e-schedule-failure` schedule
(all `ftable_*` nodes have `test_count = 0`/unknown).

Expected: a Test run with nothing to test should terminate as a **benign
non-failure** (e.g. `skipped` / no-op) — semantically "nothing to test", the
same spirit as the single-node UI gate — not `failed`.

## Reproduce

1. Pick a schedule whose nodes have no dbt tests (known-zero or unset
   `test_count`), e.g. `e2e-schedule-failure`.
2. On the Runs tab, set that schedule's operation selector to **Test** and click
   Trigger.
3. The run finalizes as `failed`; orchestrator logs show
   `Emitted run.entries.dispatch_failed:v1 ... reason="empty_projection"`.

Contrast: **single-node** Test is gated in the UI (the Test option is only
offered when `test_count > 0` is known), so you cannot reach this state per node.
The whole-DAG/schedule-level Test is **not** gated the same way.

## Root cause (code chain)

1. `orchestrator/domain/snapshot/latest_full_dag.go:33-60` — the whole-DAG Test
   selector skips every node where `!TestCountKnown || TestCount <= 0`. When all
   nodes are gated, it returns an **empty projection** (`[]TaskProjection{}, nil`).
2. `orchestrator/service/snapshotsvc/service.go:41-42` — `len(sel) == 0` is
   converted to `snapshot.ErrEmptyProjection`.
3. `orchestrator/service/handlers/dispatch_failed.go:103` — `ErrEmptyProjection`
   maps to reason `empty_projection` and emits `run.entries.dispatch_failed:v1`.
4. `state/service/handlers/run_entries_dispatch_failed_handler.go:44` — calls
   `run.MarkDispatchFailed(reason, now)`.
5. `state/domain/aggregate/run/run.go:596` — **`MarkDispatchFailed` always
   `finalize(SchedulerStatusFailed, now)`; the reason is recorded only as
   metadata (`RunDispatchFailed{Reason}`), never affecting the terminal status.**

So every "no work to dispatch" reason — including the benign test-gating ones
(`no_tests`, and the test-driven `empty_projection`) — collapses to `failed`.

## Why it's the wrong behavior

`empty_projection` is genuinely a failure for an **operation=run** DAG (an empty
DAG means a broken/misconfigured schedule). But for **operation=test**, an empty
projection is the *expected* outcome of gating every test-less node — it is not
an error. The state layer cannot currently tell the two apart because the
whole-DAG Test selector surfaces a generic empty rather than a test-specific
signal, and `MarkDispatchFailed` ignores the reason for status purposes.

## Not caused by

The dbt command-coverage change (executor `commandcfg`, PR #260). That only
affects container argv resolution *after* a node is dispatched. This bug is in
the orchestrator whole-DAG-test selector + the state run aggregate.

## Possible Solution Human Proposed

service layer (`orchestrator/service/snapshotsvc/service.go:41-42`) has the operation 
type passed in as parameter, so we are able to discern if this is a model error or a missing test 
snapshot. We probably should not emitt a `ErrEmptyProjection` at all therefore. We could create a 
new run.entries.test_missing:v1 event instead (it might be an overkill for such an edge case).
Regardless, the event is the medium between orchcestrator and state service to then finalise the 
run into a different state other than error.

What I wonder is two things as of now:
* is creating a new event for this missing test an overkill?
* is defined that run as SKIP a good approximation of what is happening in the system. We already have that status
  but we don't use it in this context (we use it when some nodes are skipped after a failure/cancellation).