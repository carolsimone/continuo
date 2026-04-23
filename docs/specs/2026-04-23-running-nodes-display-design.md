# Running Nodes Count — SchedulerCard Design

**Date:** 2026-04-23  
**Branch:** ui-add-running-tasks  
**Status:** Approved

## Problem

The `SchedulerCard` summary row shows succeeded, failed, and pending counts but omits the running count. When nodes are actively executing, there is no way to tell from the main page — the "x" slot in the annotation is blank.

## Solution

Add a `🏃 {running} running` stat to the existing summary row, always visible (even when 0), using the same client-side filter pattern as the other three counts.

## Scope

`ui-service/src/client/SchedulerCard.tsx` only. No backend, proto, API route, or helper changes required.

## Data Flow

Tasks are fetched every 5 seconds via `GET /api/schedulers/:id/tasks`. The route calls the state service gRPC `ListTasks` RPC and maps `TASK_STATUS_RUNNING` → `'running'`. Running tasks are already present in the `tasks` array used by the component — they are simply not counted today.

## Changes

### `SchedulerCard.tsx`

1. Add running count alongside existing filters (after line 52):
   ```ts
   const running = tasks.filter(t => t.status === 'running').length;
   ```

2. Add to the summary row (after the pending span):
   ```tsx
   <span>🏃 {running} running</span>
   ```

### Display order in summary row
```
✅ {succeeded} succeeded   ❌ {failed} failed   ⏳ {pending} pending   🏃 {running} running
```

## What Does Not Change

- `scheduler-card-helpers.ts`: `getCompletedCount` counts only terminal statuses (succeeded, failed, cancelled). Running tasks are excluded from the "Completed: X/Y" label and the progress percentage — this is correct behavior.
- All API routes, gRPC proto definitions, and backend services.

## Tests

Extend the existing `SchedulerCard` test file to assert:
- Running tasks are counted and the `🏃 N running` span is rendered.
- When `running === 0`, the span still appears with `🏃 0 running`.
