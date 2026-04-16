# Enable Graph Update & DAG Trigger from Host

**Date:** 2026-04-16
**Branch:** `enable-dag-update-from-host`

## Problem

When a user modifies dbt repositories locally, compiles manifests, and loads them to S3, there is no way to trigger a graph update or DAG run from the host machine. The only current mechanism is writing directly to the `update.graph:v1` Redis stream, which is behind the Docker network (and in production, behind a Hetzner firewall). The system needs a safe HTTP-based entry point.

## Solution Overview

Two-step user workflow, available both via the UI and shell scripts:

1. **Update graph** — `scripts/update-graph.sh` calls a new ui-service endpoint that publishes `update.graph:v1` to Redis with `source=s3`. The system follows its standard path: manifest-controller loads manifests from S3, resolves deps, populates Neo4j, publishes `schedules.loaded:v1`, and the state service reconciles the catalog.
2. **Trigger DAG** — `scripts/trigger-dag.sh <schedule-name>` calls the existing `POST /api/schedules/:name/trigger` endpoint to run the full DAG.

The e2e tests are updated to use the new HTTP endpoint instead of writing directly to Redis, ensuring the endpoint is tested through the standard test suite.

## Components

### 1. New HTTP Endpoint: `POST /api/graph/update`

**Location:** `ui-service/src/server/routes/graph.ts`

**Request body** (optional JSON):
```json
{ "source": "s3" }
```

- `source` must be `"s3"` or `"local"`. Defaults to `"s3"` if omitted or if body is empty.
- Returns `400` if source is not one of the two accepted values.

**Response (200):**
```json
{ "ok": true, "source": "s3" }
```

**Response (400):**
```json
{ "error": "source must be \"s3\" or \"local\"" }
```

**Response (500):**
```json
{ "error": "failed to publish graph update" }
```

**Behaviour:** Calls Redis `XADD update.graph:v1 * source <value>`. No authentication required (same as all other ui-service endpoints today).

### 2. Redis Client in ui-service

**Package:** `ioredis` (the de-facto Node.js Redis client; mature, TypeScript types included).

**Client creation:** In `ui-service/src/server/redis-client.ts`, export a factory function that reads `REDIS_URL` from the environment and returns an `ioredis` instance. The client is created once in `index.ts` and passed into `createApp()`.

**`createApp` signature change:**
```typescript
// Before
export function createApp(client: GrpcClient, graphClient: GrpcGraphClient)

// After
import Redis from 'ioredis';
export function createApp(client: GrpcClient, graphClient: GrpcGraphClient, redisClient: Redis)
```

The graph router receives the Redis client and uses it for the single `xadd` call.

### 3. Docker-compose Changes

Add to the `ui` service in `docker-compose.yml`:

```yaml
environment:
  - REDIS_URL=redis://:continuo@redis:6379   # new
depends_on:
  - state
  - localstack
  - redis   # new
```

### 4. E2E Test Update

**File:** `e2e/trigger.go` — `triggerGraphLoad()`

Replace the direct Redis `XAdd`:
```go
// Before
err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
    Stream: "update.graph:v1",
    Values: map[string]interface{}{"source": "local"},
}).Err()
```

With an HTTP POST to the ui-service:
```go
// After
resp, err := http.Post(
    fmt.Sprintf("%s/api/graph/update", uiBase),
    "application/json",
    strings.NewReader(`{"source":"local"}`),
)
```

**Implication:** `triggerGraphLoad` now needs the `UI_HTTP_BASE` env var. This function is called by:
- `trigger_test.go` — already requires `UI_HTTP_BASE`
- `system_test.go` — currently does NOT require `UI_HTTP_BASE` for graph loading (only for optional UI verification at the end)
- `schedule_catalog_test.go` — same as system_test

The `triggerGraphLoad` signature must change to accept `uiBase string`. Tests that call it must read `UI_HTTP_BASE` and skip if not set, or we make `UI_HTTP_BASE` required for all e2e tests (which is reasonable since the e2e suite already runs with docker-compose up, where ui-service is always available).

**Decision:** Make `UI_HTTP_BASE` required for all e2e tests. Add it to `e2e/clients.go` setup alongside the other connection strings. This simplifies the test code and reflects reality — the ui-service is always part of the stack.

### 5. Host-side Scripts

Both scripts live in `dbt/` alongside the existing compile/upload workflow. The user's workflow becomes: compile manifests (`dbt_upload load`) -> update graph (`update-graph.sh`) -> trigger DAG (`trigger-dag.sh`). All three steps live in the same directory.

#### `dbt/update-graph.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SOURCE="${1:-s3}"

resp=$(curl -sf -X POST "$UI_BASE/api/graph/update" \
  -H "Content-Type: application/json" \
  -d "{\"source\":\"$SOURCE\"}")

echo "Graph update triggered: $resp"
```

#### `dbt/trigger-dag.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <schedule-name>" >&2
  exit 1
fi

UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SCHEDULE="$1"

resp=$(curl -sf -X POST "$UI_BASE/api/schedules/$SCHEDULE/trigger" \
  -H "Content-Type: application/json")

echo "DAG triggered: $resp"
```

Both scripts:
- Default to `http://localhost:8090` (local docker-compose)
- Accept `UI_BASE_URL` env var to override (e.g., for remote server)
- Use `curl -sf` to fail on HTTP errors silently (no progress bar)

### 6. "Update Graph" Button in DashboardPage

**Location:** `ui-service/src/client/DashboardPage.tsx`

**Placement:** Top-right of the page header, next to the existing "live" badge. This positions it as a global action (it reloads the entire graph across all schedules), visually separate from per-schedule cards.

**Behaviour:**
- Sends `POST /api/graph/update` with `{ "source": "s3" }` on click.
- Shows loading state ("Updating...") while the request is in-flight.
- Disables while loading to prevent double-clicks.
- On success: briefly shows a confirmation state (e.g., "Updated"), then reverts to default label.
- On error: shows the error message inline near the button, auto-clears after a few seconds.

**Implementation:** Follows the same pattern as the existing "Run" button in `SchedulerCard.tsx`:
- `useState` for `graphLoading` and `graphError`
- `fetch('/api/graph/update', { method: 'POST', ... })`
- `e.stopPropagation()` not needed here (no parent click handler)
- Button styled as `update-graph-btn` — same visual family as `trigger-run-btn` but slightly more prominent since it's a page-level action.

**CSS:** Add `update-graph-btn` class to `styles.css`, following the existing button pattern (white background, gray border, hover darkening, disabled opacity, loading state).

**Header layout change:** The `app-header` currently uses flexbox with `h1` and the live badge. The "Update Graph" button slots in between or after the live badge. Wrap the right side in a flex container to keep badge + button aligned.

### 7. Architecture Documentation Updates

**`docs/arch/services/ui-service.md`:** Add Redis as an infrastructure dependency. Document the new `POST /api/graph/update` endpoint.

**`docs/arch/02-interaction-matrix.md`:** Add ui-service row for Redis writes (`update.graph:v1` producer).

**`docs/arch/01-topology.md`:** Update the ui-service box to show Redis connection (command publishing only).

## Event Flow

```
User (host or browser)
  │
  ├─ dbt/update-graph.sh  OR  "Update Graph" button in UI
  │    └─ POST /api/graph/update {source: "s3"}
  │         └─ ui-service
  │              └─ XADD update.graph:v1 * source s3
  │                   └─ manifest-controller consumes
  │                        ├─ loads manifests from S3
  │                        ├─ resolves deps (sqlglot)
  │                        ├─ gRPC CreateNode → graph (Neo4j)
  │                        └─ XADD schedules.loaded:v1
  │                             └─ state service consumes
  │                                  └─ upserts schedule_catalog
  │
  └─ dbt/trigger-dag.sh <schedule-name>
       └─ POST /api/schedules/<name>/trigger
            └─ ui-service
                 └─ gRPC TriggerSchedule → state
                      └─ scheduler.started:v1 → standard DAG execution
```

## Files Changed

| File | Change |
|------|--------|
| `ui-service/package.json` | Add `ioredis` dependency |
| `ui-service/src/server/redis-client.ts` | New file: Redis client factory |
| `ui-service/src/server/routes/graph.ts` | New file: `POST /api/graph/update` handler |
| `ui-service/src/server/app.ts` | Accept Redis client, register graph router |
| `ui-service/src/server/index.ts` | Create Redis client, pass to `createApp` |
| `ui-service/src/client/DashboardPage.tsx` | Add "Update Graph" button in page header |
| `ui-service/src/client/styles.css` | Add `update-graph-btn` styles |
| `docker-compose.yml` | Add `REDIS_URL` and `redis` dependency to ui service |
| `e2e/trigger.go` | Replace direct Redis XAdd with HTTP POST to ui-service |
| `e2e/clients.go` | Make `UI_HTTP_BASE` a required field |
| `e2e/system_test.go` | Pass `uiBase` to `triggerGraphLoad` |
| `e2e/schedule_catalog_test.go` | Pass `uiBase` to `triggerGraphLoad` |
| `dbt/update-graph.sh` | New file: host-side graph update script |
| `dbt/trigger-dag.sh` | New file: host-side DAG trigger script |
| `docs/arch/services/ui-service.md` | Document Redis dependency and new endpoint |
| `docs/arch/02-interaction-matrix.md` | Add ui-service → Redis row |
| `docs/arch/01-topology.md` | Update ui-service topology |

## Out of Scope

- Authentication/authorization for the new endpoint (consistent with existing endpoints)
- WebSocket/SSE for real-time graph-load progress feedback
- Combining both steps into a single script with polling
