# Rerun Node Button — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface the existing `POST /api/schedulers/:id/rerun` endpoint as a Rerun button inside the DAG focus legend on the schedule detail page.

**Architecture:** The DAG focus legend is lifted out of `DAGPanel` and rendered as an absolutely-positioned overlay by `DetailPage`, which already owns `lastRunId`, `selectedNodeId`, and the active graph. `DetailPage` adds two state variables (`rerunState`, `rerunError`) and calls the rerun endpoint directly. A `parseNodeId` pure helper splits the `service.schema.table` node ID into its three parts.

**Tech Stack:** React 18, TypeScript, Vitest (unit tests), existing CSS classes.

**Working directory for all commands:** `ui-service/` inside the worktree at `.worktree/rerun-button/`

---

## File Map

| File | Change |
|---|---|
| `src/client/detail-page-helpers.ts` | Add `parseNodeId` helper |
| `tests/client/detail-page-helpers.test.ts` | Add tests for `parseNodeId` |
| `src/client/styles.css` | Add `position: relative` to `.dag-card-body`; update `.dag-focus-legend` top + pointer-events; add rerun button/feedback styles |
| `src/client/DAGPanel.tsx` | Remove focus legend block and its component-level `parentIds`/`childIds` |
| `src/client/DetailPage.tsx` | Add rerun state, `handleRerun`, `parseNodeId` import, `parentIds`/`childIds` from `activeGraph.edges`, legend overlay JSX |

---

## Task 1: Add `parseNodeId` helper and test it

**Files:**
- Modify: `src/client/detail-page-helpers.ts`
- Modify: `tests/client/detail-page-helpers.test.ts`

- [ ] **Step 1: Write the failing tests**

In `tests/client/detail-page-helpers.test.ts`, add inside the existing `describe` block:

```typescript
it('parses a node_id into service, schema, and table', () => {
  expect(parseNodeId('svc.analytics.orders')).toEqual({
    service_name: 'svc',
    schema_name: 'analytics',
    table_name: 'orders',
  });
});

it('returns empty strings for missing node_id segments', () => {
  expect(parseNodeId('only')).toEqual({
    service_name: 'only',
    schema_name: '',
    table_name: '',
  });
});
```

Add `parseNodeId` to the import at the top of the test file:

```typescript
import {
  parseNodeId,
  resolveActiveGraph,
  resolveNodeStatus,
  toScheduleGraph,
} from '../../src/client/detail-page-helpers';
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
cd .worktree/rerun-button/ui-service
npm test
```

Expected: 2 failing tests mentioning `parseNodeId is not a function` (or similar).

- [ ] **Step 3: Implement `parseNodeId` in `detail-page-helpers.ts`**

Add at the bottom of `src/client/detail-page-helpers.ts`:

```typescript
export function parseNodeId(nodeId: string): {
  service_name: string;
  schema_name: string;
  table_name: string;
} {
  const [service_name = '', schema_name = '', table_name = ''] = nodeId.split('.');
  return { service_name, schema_name, table_name };
}
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
npm test
```

Expected: all 29 tests pass (27 existing + 2 new).

- [ ] **Step 5: Commit**

```bash
git add src/client/detail-page-helpers.ts tests/client/detail-page-helpers.test.ts
git commit -m "feat(ui): add parseNodeId helper"
```

---

## Task 2: Update CSS

**Files:**
- Modify: `src/client/styles.css`

No tests for CSS — verify visually after Task 4.

- [ ] **Step 1: Add `position: relative` to `.dag-card-body` and fix `.dag-focus-legend`**

`.dag-card-body` is around line 295. Change it from:

```css
.dag-card-body {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
```

To:

```css
.dag-card-body {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  position: relative;
}
```

`.dag-focus-legend` is around line 442. Two changes: `top: 12px` → `top: 52px` (clears the 40px search strip), and remove `pointer-events: none` (the legend now contains a clickable button). Change from:

```css
.dag-focus-legend {
  position: absolute;
  top: 12px;
  right: 12px;
  z-index: 10;
  background: #fff;
  border: 1px solid #e2e8f0;
  border-radius: 7px;
  padding: 8px 12px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
  pointer-events: none;
}
```

To:

```css
.dag-focus-legend {
  position: absolute;
  top: 52px;
  right: 12px;
  z-index: 10;
  background: #fff;
  border: 1px solid #e2e8f0;
  border-radius: 7px;
  padding: 8px 12px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
}
```

- [ ] **Step 2: Add rerun button and feedback styles**

Add after the `.dag-focus-dot--dim` rule (around line 480):

```css
/* ---- Rerun button inside focus legend ---- */
.dag-rerun-divider {
  border: none;
  border-top: 1px solid #f1f5f9;
  margin: 8px 0;
}

.dag-rerun-btn {
  width: 100%;
  padding: 5px 0;
  background: #4f46e5;
  color: #fff;
  border: none;
  border-radius: 5px;
  font-size: 11px;
  font-weight: 500;
  cursor: pointer;
  font-family: inherit;
}
.dag-rerun-btn:hover:not(:disabled) { background: #4338ca; }
.dag-rerun-btn:disabled { background: #a5b4fc; cursor: not-allowed; }

.dag-rerun-feedback {
  padding: 4px 8px;
  border-radius: 5px;
  font-size: 10px;
  text-align: center;
}
.dag-rerun-feedback--success {
  background: #f0fdf4;
  border: 1px solid #86efac;
  color: #16a34a;
}
.dag-rerun-feedback--error {
  background: #fef2f2;
  border: 1px solid #fca5a5;
  color: #dc2626;
  margin-bottom: 6px;
}
```

- [ ] **Step 3: Run tests — confirm nothing broken**

```bash
npm test
```

Expected: all 29 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/client/styles.css
git commit -m "feat(ui): update focus legend CSS for dag-card-body positioning and rerun styles"
```

---

## Task 3: Strip focus legend from `DAGPanel`

**Files:**
- Modify: `src/client/DAGPanel.tsx`

- [ ] **Step 1: Remove the component-level `parentIds`/`childIds` and the legend JSX**

In `DAGPanel.tsx`, locate the block starting around line 257 (just before the `return`):

```tsx
  const parentIds = selectedNodeId
    ? new Set(graphEdges.filter((e) => e.to_node_id === selectedNodeId).map((e) => e.from_node_id))
    : new Set<string>();
  const childIds = selectedNodeId
    ? new Set(graphEdges.filter((e) => e.from_node_id === selectedNodeId).map((e) => e.to_node_id))
    : new Set<string>();
```

And the legend JSX block at the bottom of the returned fragment:

```tsx
        {selectedNodeId && (
          <div className="dag-focus-legend">
            <div className="dag-focus-legend-title">{selectedNodeId.split('.').pop()}</div>
            <div className="dag-focus-legend-row">
              <div className="dag-focus-dot dag-focus-dot--selected" /> Selected
            </div>
            <div className="dag-focus-legend-row">
              <div className="dag-focus-dot dag-focus-dot--parent" />
              Depends on ({parentIds.size})
            </div>
            <div className="dag-focus-legend-row">
              <div className="dag-focus-dot dag-focus-dot--child" />
              Required by ({childIds.size})
            </div>
            <div className="dag-focus-legend-row">
              <div className="dag-focus-dot dag-focus-dot--dim" /> Unrelated
            </div>
          </div>
        )}
```

Delete both blocks. The `parentIds`/`childIds` computed inside `buildLayout` (around line 117) remain untouched — they are still used for node and edge colouring.

- [ ] **Step 2: Run tests — confirm nothing broken**

```bash
npm test
```

Expected: all 29 tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/client/DAGPanel.tsx
git commit -m "refactor(ui): remove focus legend from DAGPanel"
```

---

## Task 4: Add rerun state and focus legend overlay to `DetailPage`

**Files:**
- Modify: `src/client/DetailPage.tsx`

- [ ] **Step 1: Update imports**

At the top of `DetailPage.tsx`, add `useCallback` to the React import and add `parseNodeId` to the helpers import:

```typescript
import { useCallback, useEffect, useRef, useState } from 'react';
```

```typescript
import { parseNodeId, resolveActiveGraph } from './detail-page-helpers';
```

- [ ] **Step 2: Add rerun state variables**

Inside the `DetailPage` function body, after the existing `useState` declarations, add:

```typescript
const [rerunState, setRerunState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
const [rerunError, setRerunError] = useState<string | null>(null);
```

- [ ] **Step 3: Reset rerun state when selected node changes**

Add a new `useEffect` after the existing effects:

```typescript
useEffect(() => {
  setRerunState('idle');
  setRerunError(null);
}, [selectedNodeId]);
```

- [ ] **Step 4: Add the `handleRerun` callback**

Add after the `rerunState` effects:

```typescript
const handleRerun = useCallback(async () => {
  if (!selectedNodeId || !lastRunId) return;
  const { service_name, schema_name, table_name } = parseNodeId(selectedNodeId);
  setRerunState('loading');
  setRerunError(null);
  try {
    const res = await fetch(`/api/schedulers/${lastRunId}/rerun`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ service_name, schema: schema_name, table_name }),
    });
    if (res.ok) {
      setRerunState('success');
      setTimeout(() => setRerunState('idle'), 3000);
    } else {
      const body = await res.json().catch(() => ({ error: 'Request failed — please try again' }));
      setRerunError(body.error ?? 'Request failed — please try again');
      setRerunState('error');
    }
  } catch {
    setRerunError('Request failed — please try again');
    setRerunState('error');
  }
}, [selectedNodeId, lastRunId]);
```

- [ ] **Step 5: Compute `parentIds` and `childIds` for the legend**

Add these two derived values after the existing derived values (e.g., near `latestExecutions`):

```typescript
const legendParentIds = selectedNodeId && activeGraph
  ? new Set(activeGraph.edges.filter((e) => e.to_node_id === selectedNodeId).map((e) => e.from_node_id))
  : new Set<string>();

const legendChildIds = selectedNodeId && activeGraph
  ? new Set(activeGraph.edges.filter((e) => e.from_node_id === selectedNodeId).map((e) => e.to_node_id))
  : new Set<string>();
```

- [ ] **Step 6: Render the focus legend overlay**

In the JSX, find the `dag-card-body` div. It currently looks like:

```tsx
<div className="dag-card-body">
  {graphCardState === 'ready' && activeGraph ? (
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={activeGraph.nodes}
        graphEdges={activeGraph.edges}
        tasks={activeTasks}
        selectedNodeId={selectedNodeId}
        onNodeClick={setSelectedNodeId}
      />
    </ReactFlowProvider>
  ) : graphCardState === 'error' ? (
    ...
```

Wrap the `ReactFlowProvider` and legend together inside a fragment, so the legend is a sibling rendered inside `dag-card-body`:

```tsx
<div className="dag-card-body">
  {graphCardState === 'ready' && activeGraph ? (
    <>
      <ReactFlowProvider>
        <DAGPanel
          graphNodes={activeGraph.nodes}
          graphEdges={activeGraph.edges}
          tasks={activeTasks}
          selectedNodeId={selectedNodeId}
          onNodeClick={setSelectedNodeId}
        />
      </ReactFlowProvider>
      {selectedNodeId && !selectedRunId && lastRunId && (
        <div className="dag-focus-legend">
          <div className="dag-focus-legend-title">{selectedNodeId.split('.').pop()}</div>
          <div className="dag-focus-legend-row">
            <div className="dag-focus-dot dag-focus-dot--selected" /> Selected
          </div>
          <div className="dag-focus-legend-row">
            <div className="dag-focus-dot dag-focus-dot--parent" />
            Depends on ({legendParentIds.size})
          </div>
          <div className="dag-focus-legend-row">
            <div className="dag-focus-dot dag-focus-dot--child" />
            Required by ({legendChildIds.size})
          </div>
          <div className="dag-focus-legend-row">
            <div className="dag-focus-dot dag-focus-dot--dim" /> Unrelated
          </div>
          <hr className="dag-rerun-divider" />
          {rerunState === 'error' && rerunError && (
            <div className="dag-rerun-feedback dag-rerun-feedback--error">{rerunError}</div>
          )}
          {rerunState === 'success' ? (
            <div className="dag-rerun-feedback dag-rerun-feedback--success">✓ Rerun triggered</div>
          ) : (
            <button
              type="button"
              className="dag-rerun-btn"
              disabled={rerunState === 'loading'}
              onClick={handleRerun}
            >
              {rerunState === 'loading' ? 'Running…' : '↺ Rerun node'}
            </button>
          )}
        </div>
      )}
    </>
  ) : graphCardState === 'error' ? (
```

- [ ] **Step 7: Run tests — confirm everything passes**

```bash
npm test
```

Expected: all 29 tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/client/DetailPage.tsx
git commit -m "feat(ui): add rerun node button to DAG focus legend"
```

---

## Task 5: Manual smoke test

Start the dev server and verify the feature end-to-end.

- [ ] **Step 1: Start dev server**

```bash
npm run dev
```

Open the UI, navigate to a schedule detail page that has a completed run.

- [ ] **Step 2: Verify legend appears and button works**

- Click any node in the DAG → legend appears at top-right of graph, shows node name, dependency counts, and "↺ Rerun node" button
- Click the button → button changes to "Running…" and is disabled
- On success → "✓ Rerun triggered" appears for 3 seconds, then button returns
- On error (e.g., click on a non-failed node) → error message from backend appears above the button, button re-enables

- [ ] **Step 3: Verify legend is absent in snapshot mode**

If any past runs are available, click one → legend should not appear even if a node is selected.

- [ ] **Step 4: Verify legend is absent when no run exists**

Navigate to a schedule with no runs → no legend even if a node is selected (since `lastRunId` is null).

---

## Final check

```bash
npm test
```

All 29 tests green. Branch is ready for PR.
