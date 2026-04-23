# Running Nodes Display Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show the count of currently-running nodes alongside succeeded/failed/pending in the SchedulerCard summary row, using a 🏃 emoji.

**Architecture:** Extract a `getRunningCount` helper (consistent with existing `getCompletedCount`), unit-test it in the existing vitest suite, then wire it into the component's summary row. No backend changes needed — running tasks already flow through the API.

**Tech Stack:** TypeScript, React, Vitest (run via `npm test` inside `ui-service/`)

---

### Task 1: Add `getRunningCount` helper and its test

**Files:**
- Modify: `ui-service/src/client/scheduler-card-helpers.ts`
- Modify: `ui-service/tests/client/scheduler-card-helpers.test.ts`

- [ ] **Step 1: Write the failing test**

Open `ui-service/tests/client/scheduler-card-helpers.test.ts`. Add to the `describe` block:

```typescript
it('counts only running nodes', () => {
  const tasks = [
    makeTask('running', 'a'),
    makeTask('running', 'b'),
    makeTask('succeeded', 'c'),
    makeTask('failed', 'd'),
    makeTask('pending', 'e'),
  ];

  expect(getRunningCount(tasks)).toBe(2);
});

it('returns 0 running when no nodes are running', () => {
  const tasks = [
    makeTask('succeeded', 'a'),
    makeTask('failed', 'b'),
    makeTask('pending', 'c'),
  ];

  expect(getRunningCount(tasks)).toBe(0);
});
```

Also add `getRunningCount` to the import at line 3:

```typescript
import {
  getRunningCount,
  getScheduleProgressLabel,
  getScheduleProgressPercent,
} from '../../src/client/scheduler-card-helpers';
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cd ui-service && npm test -- --reporter=verbose tests/client/scheduler-card-helpers.test.ts
```

Expected: FAIL — `getRunningCount is not a function` (or similar import error).

- [ ] **Step 3: Implement `getRunningCount` in the helpers file**

Open `ui-service/src/client/scheduler-card-helpers.ts`. Add after the `getCompletedCount` export:

```typescript
export function getRunningCount(tasks: Task[]): number {
  return tasks.filter((task) => task.status === 'running').length;
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

```bash
cd ui-service && npm test -- --reporter=verbose tests/client/scheduler-card-helpers.test.ts
```

Expected: all 4 tests PASS (2 existing + 2 new).

- [ ] **Step 5: Commit**

```bash
git add ui-service/src/client/scheduler-card-helpers.ts ui-service/tests/client/scheduler-card-helpers.test.ts
git commit -m "feat(ui): add getRunningCount helper"
```

---

### Task 2: Wire running count into SchedulerCard

**Files:**
- Modify: `ui-service/src/client/SchedulerCard.tsx`

- [ ] **Step 1: Import `getRunningCount`**

Open `ui-service/src/client/SchedulerCard.tsx`. Update the import at lines 3-6:

```typescript
import {
  getRunningCount,
  getScheduleProgressLabel,
  getScheduleProgressPercent,
} from './scheduler-card-helpers';
```

- [ ] **Step 2: Compute the running count**

At line 53 (after `const pending = ...`), add:

```typescript
const running = getRunningCount(tasks);
```

The block should now read:

```typescript
const total = tasks.length;
const succeeded = tasks.filter(t => t.status === 'succeeded').length;
const failed = tasks.filter(t => t.status === 'failed').length;
const pending = tasks.filter(t => t.status === 'pending').length;
const running = getRunningCount(tasks);
const pct = getScheduleProgressPercent(tasks);
```

- [ ] **Step 3: Add the running span to the summary row**

At lines 112-116, update the `summary-row` div:

```tsx
<div className="summary-row">
  <span>✅ {succeeded} succeeded</span>
  <span>❌ {failed} failed</span>
  <span>⏳ {pending} pending</span>
  <span>🏃 {running} running</span>
</div>
```

- [ ] **Step 4: Run the full test suite**

```bash
cd ui-service && npm test
```

Expected: all tests PASS. No failures.

- [ ] **Step 5: Commit**

```bash
git add ui-service/src/client/SchedulerCard.tsx
git commit -m "feat(ui): show running nodes count in scheduler card summary row"
```
