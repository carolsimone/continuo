# Rerun Node Button — Design Spec

**Date:** 2026-04-22
**Status:** Approved

## Problem

`TriggerRerun` is wired up end-to-end (ui-service BFF → state gRPC → orchestrator → executor-controller → k8s-controller) but is not surfaced anywhere in the frontend. Users have no way to retry a failed node from the UI.

## Goal

Add a Rerun button to the schedule detail page that triggers `POST /api/schedulers/:id/rerun` for the currently selected node.

## Scope

- Frontend only: `DetailPage.tsx`, `DAGPanel.tsx`, `styles.css`
- No backend changes required
- Historical snapshot mode (past runs) is out of scope — rerun is only available for the current live run

---

## Design

### Placement

The Rerun button lives inside the **DAG focus legend** — the node-info panel that appears when a user clicks a node in the dependency graph. This is already the natural point of interaction for per-node actions.

### Visibility

The legend (with Rerun button) renders when all of the following are true:
- `graphCardState === 'ready'` (graph is loaded)
- `selectedNodeId !== null` (a node is selected)
- `!selectedRunId` (not viewing a historical snapshot)

The button is always enabled — the backend enforces eligibility (task must be FAILED, no tasks RUNNING) and returns a descriptive error if conditions aren't met.

### Architecture

**`DAGPanel.tsx`** — remove the focus legend block entirely. The component retains `selectedNodeId` and `onNodeClick` props because it still uses them for node/edge colour styling. The parent/child sets computed internally for styling are unchanged.

**`DetailPage.tsx`** — renders the focus legend as an absolutely-positioned overlay inside `.dag-card-body`. It has direct access to everything the legend needs:
- `selectedNodeId` — which node is selected
- `activeGraph.edges` — to compute parent/child counts
- `lastRunId` — the `schedule_id` for the rerun POST
- New state: `rerunState: 'idle' | 'loading' | 'success' | 'error'`
- New state: `rerunError: string | null`

**`styles.css`** — `.dag-card-body` gains `position: relative` so the legend can be absolutely positioned within it.

### Legend content

```
┌─────────────────────────┐
│ table_d                 │
│ ● Selected              │
│ ● Depends on (N)        │
│ ● Required by (N)       │
│ ● Unrelated (N)         │
├─────────────────────────┤
│ [↺ Rerun node]          │  ← idle
│ [↺ Running…]            │  ← loading (disabled)
│ ✓ Rerun triggered       │  ← success (3s then idle)
│ ⚠ <error message>       │
│ [↺ Rerun node]          │  ← error (button re-enabled)
└─────────────────────────┘
```

### Interaction flow

1. User clicks a node in the DAG → `selectedNodeId` is set, legend appears, `rerunState` resets to `'idle'`
2. User clicks **Rerun node** → `rerunState = 'loading'`, button disabled
3. `POST /api/schedulers/${lastRunId}/rerun` with body:
   ```json
   { "service_name": "...", "schema": "...", "table_name": "..." }
   ```
   Parsed by splitting `selectedNodeId` on `.` (format: `service_name.schema_name.table_name`)
4. **On success (2xx):** `rerunState = 'success'`. Button area shows "✓ Rerun triggered" in green. Auto-resets to `'idle'` after 3 seconds.
5. **On error (4xx/5xx):** `rerunState = 'error'`, `rerunError` = message from response body. Error banner shown above the button. Button re-enables immediately for retry.
6. Selecting a different node resets `rerunState` to `'idle'` and clears `rerunError`.

### Error handling

| Backend error | Message shown |
|---|---|
| 409 "target task is not in FAILED state" | "target task is not in FAILED state" |
| 409 "schedule has running tasks" | "schedule has running tasks" |
| 404 "schedule not found" | "schedule not found" |
| Network / 5xx | "Request failed — please try again" |

### No new files

All changes are contained to three existing files: `DetailPage.tsx`, `DAGPanel.tsx`, `styles.css`.
