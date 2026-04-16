# Enable Graph Update & DAG Trigger from Host — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow users to trigger graph updates and DAG runs from the host machine via ui-service HTTP endpoints, a UI button, and shell scripts in `dbt/`.

**Architecture:** ui-service gains a Redis client (`ioredis`) to publish `update.graph:v1` commands. A new `POST /api/graph/update` endpoint wraps the `XADD`. The DashboardPage gets an "Update Graph" button. E2E tests switch from direct Redis writes to the HTTP endpoint. Two shell scripts in `dbt/` provide the CLI workflow.

**Tech Stack:** TypeScript/Express (ui-service backend), React (ui-service frontend), ioredis (Redis client), Go (e2e tests), bash (host scripts).

**Spec:** `docs/specs/2026-04-16-enable-dag-update-from-host-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `ui-service/package.json` | Modify | Add `ioredis` dependency |
| `ui-service/src/server/redis-client.ts` | Create | Redis client factory (reads `REDIS_URL`) |
| `ui-service/src/server/routes/graph.ts` | Create | `POST /api/graph/update` handler |
| `ui-service/src/server/app.ts` | Modify | Accept Redis client param, register graph router |
| `ui-service/src/server/index.ts` | Modify | Create Redis client, pass to `createApp` |
| `ui-service/src/client/DashboardPage.tsx` | Modify | Add "Update Graph" button in header |
| `ui-service/src/client/styles.css` | Modify | Add `update-graph-btn` styles |
| `docker-compose.yml` | Modify | Add `REDIS_URL` + `redis` dependency to ui service |
| `e2e/trigger.go` | Modify | Replace Redis XAdd with HTTP POST |
| `e2e/clients.go` | Modify | Add `uiBase` field to `testClients` |
| `e2e/system_test.go` | Modify | Pass `uiBase` to `triggerGraphLoad` |
| `e2e/trigger_test.go` | Modify | Use `clients.uiBase` instead of local var |
| `e2e/schedule_catalog_test.go` | Modify | Replace direct Redis XAdd with HTTP POST |
| `dbt/update-graph.sh` | Create | Host-side graph update script |
| `dbt/trigger-dag.sh` | Create | Host-side DAG trigger script |
| `docs/arch/services/ui-service.md` | Modify | Document Redis dep + new endpoint |
| `docs/arch/02-interaction-matrix.md` | Modify | Add ui-service Redis row |
| `docs/arch/01-topology.md` | Modify | Update topology diagram + ownership note |

---

### Task 1: Add ioredis dependency

**Files:**
- Modify: `ui-service/package.json`

- [ ] **Step 1: Install ioredis**

Run from host (not inside container — this updates the lockfile):

```bash
cd ui-service && npm install ioredis
```

This adds `ioredis` to `dependencies` in `package.json` and updates `package-lock.json`.

- [ ] **Step 2: Verify package.json has ioredis**

```bash
grep ioredis ui-service/package.json
```

Expected: a line like `"ioredis": "^5.x.x"` under `dependencies`.

- [ ] **Step 3: Commit**

```bash
git add ui-service/package.json ui-service/package-lock.json
git commit -m "feat(ui-service): add ioredis dependency for Redis command publishing"
```

---

### Task 2: Redis client factory

**Files:**
- Create: `ui-service/src/server/redis-client.ts`

- [ ] **Step 1: Create redis-client.ts**

```typescript
import Redis from 'ioredis';

export function createRedisClient(): Redis {
  const url = process.env.REDIS_URL;
  if (!url) {
    throw new Error('REDIS_URL environment variable is required');
  }
  return new Redis(url);
}
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd ui-service && npx tsc -p tsconfig.server.json --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add ui-service/src/server/redis-client.ts
git commit -m "feat(ui-service): add Redis client factory reading REDIS_URL"
```

---

### Task 3: Graph update route

**Files:**
- Create: `ui-service/src/server/routes/graph.ts`

- [ ] **Step 1: Create graph.ts route**

```typescript
import { Router } from 'express';
import Redis from 'ioredis';

const VALID_SOURCES = ['s3', 'local'];

export function createGraphRouter(redisClient: Redis) {
  const router = Router();

  // POST /api/graph/update — publish update.graph:v1 to Redis
  router.post('/update', async (req, res) => {
    const source = req.body?.source || 's3';

    if (!VALID_SOURCES.includes(source)) {
      return res.status(400).json({ error: 'source must be "s3" or "local"' });
    }

    try {
      await redisClient.xadd('update.graph:v1', '*', 'source', source);
      res.json({ ok: true, source });
    } catch (err: any) {
      console.error('Failed to publish graph update:', err.message);
      res.status(500).json({ error: 'failed to publish graph update' });
    }
  });

  return router;
}
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd ui-service && npx tsc -p tsconfig.server.json --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add ui-service/src/server/routes/graph.ts
git commit -m "feat(ui-service): add POST /api/graph/update route for Redis XADD"
```

---

### Task 4: Wire Redis client and graph route into the app

**Files:**
- Modify: `ui-service/src/server/app.ts`
- Modify: `ui-service/src/server/index.ts`

- [ ] **Step 1: Update app.ts to accept Redis client and register graph router**

Replace the full content of `ui-service/src/server/app.ts` with:

```typescript
import express from 'express';
import Redis from 'ioredis';
import { GrpcClient } from './grpc-client';
import { GrpcGraphClient } from './grpc-graph-client';
import { createSchedulersRouter } from './routes/schedulers';
import { createSchedulesRouter, createRunsRouter } from './routes/schedules';
import { createExecutionsRouter } from './routes/executions';
import { createTaskExecutionRouter } from './routes/task-execution';
import { createGraphRouter } from './routes/graph';

export function createApp(client: GrpcClient, graphClient: GrpcGraphClient, redisClient: Redis) {
  const app = express();
  app.use(express.json());
  app.use('/api/schedulers', createSchedulersRouter(client));
  app.use('/api/schedules', createSchedulesRouter(client, graphClient));
  app.use('/api/runs', createRunsRouter(graphClient));
  app.use('/api/schedulers', createExecutionsRouter(client));
  app.use('/api/task-execution', createTaskExecutionRouter());
  app.use('/api/graph', createGraphRouter(redisClient));
  return app;
}
```

- [ ] **Step 2: Update index.ts to create Redis client and pass it**

Replace the full content of `ui-service/src/server/index.ts` with:

```typescript
import path from 'path';
import express from 'express';
import { createGrpcClient } from './grpc-client';
import { createGrpcGraphClient } from './grpc-graph-client';
import { createRedisClient } from './redis-client';
import { createApp } from './app';

const PORT = parseInt(process.env.PORT || '8090', 10);
const STATE_GRPC_ADDR = process.env.STATE_GRPC_ADDR || 'localhost:50051';
const GRAPH_GRPC_ADDR = process.env.GRAPH_GRPC_ADDR || 'localhost:50052';

const client = createGrpcClient(STATE_GRPC_ADDR);
const graphClient = createGrpcGraphClient(GRAPH_GRPC_ADDR);
const redisClient = createRedisClient();
const app = createApp(client, graphClient, redisClient);

if (process.env.NODE_ENV === 'production') {
  // dist/ is one level up from dist-server/ at runtime
  const staticDir = path.join(__dirname, '../dist');
  app.use(express.static(staticDir));
  app.get('*', (_req, res) => {
    res.sendFile(path.join(staticDir, 'index.html'));
  });
}

app.listen(PORT, () => {
  console.log(`Continuo UI running on http://localhost:${PORT}`);
});
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd ui-service && npx tsc -p tsconfig.server.json --noEmit
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add ui-service/src/server/app.ts ui-service/src/server/index.ts
git commit -m "feat(ui-service): wire Redis client and graph router into app"
```

---

### Task 5: Docker-compose — add Redis to ui service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add REDIS_URL and redis dependency to ui service**

In `docker-compose.yml`, find the `ui:` service block. Add `REDIS_URL` to its environment and `redis` to its `depends_on`.

Before (lines ~457-478):
```yaml
  ui:
    build:
      context: ./ui-service
      network: host
    container_name: ui
    depends_on:
      - state
      - localstack
    environment:
      - STATE_GRPC_ADDR=state:50051
      - GRAPH_GRPC_ADDR=graph:50052
      - PORT=8090
      - NODE_ENV=production
      - S3_ENDPOINT_URL=http://localstack:4566
      - S3_BUCKET=continuo
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test
      - AWS_DEFAULT_REGION=us-east-1
```

After:
```yaml
  ui:
    build:
      context: ./ui-service
      network: host
    container_name: ui
    depends_on:
      - state
      - localstack
      - redis
    environment:
      - STATE_GRPC_ADDR=state:50051
      - GRAPH_GRPC_ADDR=graph:50052
      - PORT=8090
      - NODE_ENV=production
      - S3_ENDPOINT_URL=http://localstack:4566
      - S3_BUCKET=continuo
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test
      - AWS_DEFAULT_REGION=us-east-1
      - REDIS_URL=redis://:continuo@redis:6379
```

Two changes: add `- redis` under `depends_on` and add `- REDIS_URL=redis://:continuo@redis:6379` under `environment`.

- [ ] **Step 2: Verify compose config is valid**

```bash
docker compose config --quiet
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(docker-compose): add Redis dependency to ui service"
```

---

### Task 6: "Update Graph" button in DashboardPage

**Files:**
- Modify: `ui-service/src/client/DashboardPage.tsx`
- Modify: `ui-service/src/client/styles.css`

- [ ] **Step 1: Add button state and handler to DashboardPage.tsx**

Replace the full content of `ui-service/src/client/DashboardPage.tsx` with:

```tsx
import { useEffect, useState } from 'react';
import { ScheduleSummary } from './types';
import SchedulerCard from './SchedulerCard';

export default function DashboardPage() {
  const [schedules, setSchedules] = useState<ScheduleSummary[]>([]);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [graphLoading, setGraphLoading] = useState(false);
  const [graphStatus, setGraphStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [graphError, setGraphError] = useState<string | null>(null);

  useEffect(() => {
    const fetch_ = () =>
      fetch('/api/schedules')
        .then(r => r.json())
        .then(data => {
          setSchedules(data.schedules || []);
          setLastUpdated(new Date());
          setError(null);
        })
        .catch(e => setError(e.message));

    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => clearInterval(id);
  }, []);

  const handleUpdateGraph = () => {
    setGraphLoading(true);
    setGraphStatus('idle');
    setGraphError(null);
    fetch('/api/graph/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source: 's3' }),
    })
      .then(async r => {
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${r.status}`);
        }
        setGraphStatus('success');
        setTimeout(() => setGraphStatus('idle'), 3000);
      })
      .catch(err => {
        setGraphError(err.message);
        setGraphStatus('error');
        setTimeout(() => {
          setGraphError(null);
          setGraphStatus('idle');
        }, 5000);
      })
      .finally(() => setGraphLoading(false));
  };

  const graphBtnLabel = graphLoading
    ? 'Updating...'
    : graphStatus === 'success'
    ? 'Updated'
    : 'Update Graph';

  return (
    <div className="app">
      <header className="app-header">
        <h1>Continuo</h1>
        <div className="header-actions">
          <span className="live-badge">
            ● live{lastUpdated ? ` · ${lastUpdated.toLocaleTimeString()}` : ''}
          </span>
          <button
            className={`update-graph-btn${graphLoading ? ' loading' : ''}${graphStatus === 'success' ? ' success' : ''}`}
            disabled={graphLoading}
            onClick={handleUpdateGraph}
            title="Reload graph from S3 manifests"
          >
            {graphBtnLabel}
          </button>
        </div>
      </header>
      {graphError && <div className="error-banner">{graphError}</div>}
      {error && <div className="error-banner">{error}</div>}
      <main>
        {schedules.length === 0 && !error && (
          <p className="empty">No schedules found.</p>
        )}
        {schedules.map(s => (
          <SchedulerCard key={s.schedule_name} schedule={s} />
        ))}
      </main>
    </div>
  );
}
```

- [ ] **Step 2: Add CSS for the button and header layout**

Append the following to the end of `ui-service/src/client/styles.css`:

```css
/* Header actions container */
.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-left: auto;
}

/* Update Graph button */
.update-graph-btn {
  padding: 4px 12px;
  font-size: 12px;
  font-weight: 500;
  border: 1px solid #d1d5db;
  border-radius: 4px;
  background: #fff;
  color: #374151;
  cursor: pointer;
  white-space: nowrap;
}
.update-graph-btn:hover:not(:disabled) { background: #f3f4f6; border-color: #9ca3af; }
.update-graph-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.update-graph-btn.loading { opacity: 0.6; }
.update-graph-btn.success { color: #16a34a; border-color: #86efac; }
```

Also update the existing `.live-badge` rule — remove `margin-left: auto;` since the parent `.header-actions` now handles alignment. Change:

```css
.live-badge {
  font-size: 12px;
  color: #22c55e;
  margin-left: auto;
}
```

To:

```css
.live-badge {
  font-size: 12px;
  color: #22c55e;
}
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd ui-service && npx tsc --noEmit
```

Expected: no errors (this checks both client and server).

- [ ] **Step 4: Commit**

```bash
git add ui-service/src/client/DashboardPage.tsx ui-service/src/client/styles.css
git commit -m "feat(ui-service): add Update Graph button to dashboard header"
```

---

### Task 7: E2E — add uiBase to testClients and update triggerGraphLoad

**Files:**
- Modify: `e2e/clients.go`
- Modify: `e2e/trigger.go`

- [ ] **Step 1: Add uiBase field to testClients in clients.go**

In `e2e/clients.go`, add `uiBase` to the `testClients` struct (after the `logger` field):

```go
type testClients struct {
	graphClient  graphv1.GraphServiceClient
	stateClient  statev1.StateServiceClient
	redisClient  *goredis.Client
	neo4jDriver  neo4jdriver.DriverWithContext
	startupDB    *sqlx.DB
	executorDB   *sqlx.DB
	dependencyDB *sqlx.DB
	k8sDB        *sqlx.DB
	stateDB      *sqlx.DB
	logger       *slog.Logger
	uiBase       string
}
```

In the `setupClients` function, read `UI_HTTP_BASE` and set it on the struct. Add this line after the `pgHost` variable (around line 46):

```go
uiBase := getEnv("UI_HTTP_BASE", "http://ui:8090")
```

And set it in the returned struct (after `logger`):

```go
uiBase: uiBase,
```

- [ ] **Step 2: Update triggerGraphLoad to use HTTP instead of direct Redis**

Replace the full content of `e2e/trigger.go` with:

```go
package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

const (
	testScheduleName = "e2e-schedule"
	testSchemaName   = "e2e_schema"
	testOwner        = "test"
)

// expectedTables is the full set of table names the manifest-controller loads
var expectedTables = []string{
	"table_a", "table_b", "table_c",
	"table_d", "table_e", "table_f",
	"table_g", "table_h",
	"table_i", "table_j",
}

// tableServiceMap maps each happy-path table to its owning service
var tableServiceMap = map[string]string{
	"seed_table_1": "service-1",
	"seed_table_2": "service-1",
	"seed_table_3": "service-1",
	"table_a":      "service-1",
	"table_b":      "service-1",
	"table_c":      "service-1",
	"table_d":      "service-3",
	"table_e":      "service-3",
	"table_f":      "service-3",
	"table_g":      "service-2",
	"table_h":      "service-2",
	"table_i":      "service-3",
	"table_j":      "service-3",
}

// getServiceNameForTable returns the service name for a happy-path table
func getServiceNameForTable(tableName string) string {
	svc, ok := tableServiceMap[tableName]
	if !ok {
		panic(fmt.Sprintf("no service mapping for table %q", tableName))
	}
	return svc
}

// triggerGraphLoad triggers a graph update via the ui-service HTTP endpoint
// and waits until all expected nodes are visible in the Neo4j graph.
func triggerGraphLoad(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	resp, err := http.Post(
		fmt.Sprintf("%s/api/graph/update", clients.uiBase),
		"application/json",
		strings.NewReader(`{"source":"local"}`),
	)
	require.NoError(t, err, "POST /api/graph/update: request failed")
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/graph/update: expected 200, got %d: %s", resp.StatusCode, string(body))

	t.Log("Published update.graph:v1 via ui-service — waiting for manifest-controller to load nodes...")

	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
			AccessMode: neo4jdriver.AccessModeRead,
		})
		defer session.Close(ctx)

		result, err := session.Run(ctx,
			"MATCH (t:Table) WHERE t.table_name IN $tables AND t.schedule_name = $schedule_name RETURN count(t) AS cnt",
			map[string]interface{}{
				"tables":        expectedTables,
				"schedule_name": testScheduleName,
			},
		)
		if err != nil {
			return false, nil
		}
		record, err := result.Single(ctx)
		if err != nil {
			return false, nil
		}
		cnt, _ := record.Get("cnt")
		count, ok := cnt.(int64)
		if !ok {
			return false, nil
		}
		return count >= int64(len(expectedTables)), nil
	}, fmt.Sprintf("Timeout waiting for manifest-controller to load %d nodes", len(expectedTables)))

	t.Logf("manifest-controller loaded %d nodes into graph", len(expectedTables))

	// Wait for state service to consume schedules.loaded:v1 and populate schedule_catalog.
	// Neo4j nodes appearing does not guarantee the catalog is ready — they are populated
	// by two separate async steps (graph load → Redis publish → state consumer).
	// Without this wait, ActivateSchedule bypasses the catalog check and the UI sees
	// an empty catalog until the event is eventually processed.
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var count int
		if err := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schedule_catalog WHERE removed_at IS NULL`,
		).Scan(&count); err != nil {
			return false, err
		}
		return count > 0, nil
	}, "Timeout waiting for schedule_catalog to be populated by state service")

	t.Log("schedule_catalog populated — catalog and graph are in sync")
}
```

Key changes:
- Removed `goredis` import (no longer needed in this file).
- Added `io`, `net/http`, `strings` imports.
- `triggerGraphLoad` now uses `clients.uiBase` to POST to the ui-service instead of direct Redis XAdd.
- Signature unchanged: `(t *testing.T, ctx context.Context, clients *testClients)` — callers don't change.

- [ ] **Step 3: Verify e2e compiles**

```bash
cd e2e && go build ./...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add e2e/clients.go e2e/trigger.go
git commit -m "refactor(e2e): trigger graph load via ui-service HTTP instead of direct Redis"
```

---

### Task 8: E2E — update schedule_catalog_test.go to use HTTP

**Files:**
- Modify: `e2e/schedule_catalog_test.go`

This test has its own direct Redis `XAdd` in Step 1 (line 54) — separate from `triggerGraphLoad`. It must also switch to the HTTP endpoint.

- [ ] **Step 1: Replace Redis XAdd with HTTP POST in schedule_catalog_test.go**

In `e2e/schedule_catalog_test.go`, replace the Step 1 block (lines 52-59):

```go
	// Step 1: Trigger a manifest load by publishing to update.graph:v1
	t.Log("Step 1: Publishing update.graph:v1 to trigger manifest load")
	msgID, err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
		Stream: "update.graph:v1",
		Values: map[string]interface{}{"source": "local"},
	}).Result()
	require.NoError(t, err, "Failed to publish to update.graph:v1")
	t.Logf("Published trigger message: %s", msgID)
```

With:

```go
	// Step 1: Trigger a manifest load via ui-service HTTP endpoint
	t.Log("Step 1: Triggering graph update via ui-service HTTP")
	resp, err := http.Post(
		fmt.Sprintf("%s/api/graph/update", clients.uiBase),
		"application/json",
		strings.NewReader(`{"source":"local"}`),
	)
	require.NoError(t, err, "POST /api/graph/update: request failed")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/graph/update: expected 200, got %d: %s", resp.StatusCode, string(body))
	t.Log("Graph update triggered via ui-service")
```

Update the import block: remove the `goredis` import (if no longer needed elsewhere in the file — check: it's only used for the XAdd). Add `"io"`, `"net/http"`, and `"strings"` imports. The updated import block:

```go
import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)
```

Note: keep `goredis` in the import because it's still used in Step 2 for `XRange` on `schedules.loaded:v1` (line 65). That's a read, not a write — it stays.

- [ ] **Step 2: Verify e2e compiles**

```bash
cd e2e && go build ./...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add e2e/schedule_catalog_test.go
git commit -m "refactor(e2e): use ui-service HTTP for graph update in schedule_catalog_test"
```

---

### Task 9: E2E — clean up trigger_test.go to use clients.uiBase

**Files:**
- Modify: `e2e/trigger_test.go`

`trigger_test.go` currently reads `UI_HTTP_BASE` into a local variable and skips if unset. Since `clients.uiBase` now always has a value (defaulting to `http://ui:8090`), remove the skip logic and use `clients.uiBase`.

- [ ] **Step 1: Update trigger_test.go**

Remove the `UI_HTTP_BASE` local var and skip logic (lines 27-30):

```go
	uiBase := os.Getenv("UI_HTTP_BASE")
	if uiBase == "" {
		t.Skip("UI_HTTP_BASE not set — skipping trigger test (requires ui-service)")
	}
```

And update the call to `triggerScheduleHTTP` (lines 68, 80) to use `clients.uiBase` instead of `uiBase`:

```go
scheduleID1 := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
```

```go
scheduleID2 := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
```

Remove `"os"` from the import block (if no longer used).

- [ ] **Step 2: Verify e2e compiles**

```bash
cd e2e && go build ./...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add e2e/trigger_test.go
git commit -m "refactor(e2e): use clients.uiBase in trigger_test instead of local env read"
```

---

### Task 10: Host-side scripts

**Files:**
- Create: `dbt/update-graph.sh`
- Create: `dbt/trigger-dag.sh`

- [ ] **Step 1: Create dbt/update-graph.sh**

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

- [ ] **Step 2: Create dbt/trigger-dag.sh**

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

- [ ] **Step 3: Make scripts executable**

```bash
chmod +x dbt/update-graph.sh dbt/trigger-dag.sh
```

- [ ] **Step 4: Commit**

```bash
git add dbt/update-graph.sh dbt/trigger-dag.sh
git commit -m "feat(dbt): add host-side scripts for graph update and DAG trigger"
```

---

### Task 11: Architecture documentation updates

**Files:**
- Modify: `docs/arch/services/ui-service.md`
- Modify: `docs/arch/02-interaction-matrix.md`
- Modify: `docs/arch/01-topology.md`

- [ ] **Step 1: Update ui-service.md**

In `docs/arch/services/ui-service.md`:

**a)** In the opening paragraph (line 5), add "graph update command publishing" to the list:

```
- graph update triggering: publishes `update.graph:v1` to Redis via `POST /api/graph/update`
```

**b)** Add a row to the Schedule API table (after the trigger row, around line 33):

```
| `/api/graph/update` | POST | `XADD update.graph:v1` → Redis |
```

**c)** Add a new section under "Outbound Interfaces" after the S3 section (after line 81):

```markdown
### Redis (`REDIS_URL`)

| Operation | Route | Description |
|---|---|---|
| `XADD update.graph:v1` | `POST /api/graph/update` | Publishes graph reload command with `source` field (`s3` or `local`) |
```

**d)** Add a row to the "What It Writes" table (around line 99):

```
| Graph update command | Redis `update.graph:v1` stream via `POST /api/graph/update` |
```

**e)** Update the "Reliability Notes" section — change line 127 from:

```
- `ui-service` should remain read-only.
```

Note: this line is in `01-topology.md`, not `ui-service.md`. In `ui-service.md`, update the reliability note (line 127) to mention the Redis write:

```
- Mostly read-only; write-side effects are `TriggerRerun` (via `POST /api/schedulers/:id/rerun`), which resets a failed task and its downstream in `state`, `TriggerSchedule` (via `POST /api/schedules/:name/trigger`), which starts a full DAG run, and `POST /api/graph/update`, which publishes `update.graph:v1` to Redis.
```

- [ ] **Step 2: Update 02-interaction-matrix.md**

**a)** In the Dependency Matrix table, change the `ui-service` row (line 21) from:

```
| `ui-service` | `-` | `-` | `-` | `RW` | `R` | `-` | `-` |
```

To:

```
| `ui-service` | `-` | `-` | `W` | `RW` | `R` | `-` | `-` |
```

**b)** In the Redis Stream Matrix table, update the `update.graph:v1` row (line 27) from:

```
| `update.graph:v1` | not found in repo | `manifest-controller` | Trigger manifest reload from `local` or `s3` source |
```

To:

```
| `update.graph:v1` | `ui-service` | `manifest-controller` | Trigger manifest reload from `local` or `s3` source |
```

- [ ] **Step 3: Update 01-topology.md**

**a)** In the Static System View mermaid diagram, add a connection from UI to Redis. After line 52 (`UI --> GR`), add:

```
  UI --> R
```

**b)** In the Ownership Boundaries table, update the `ui-service` row (line 114) from:

```
| Read-only UI/API facade | `ui-service` | none |
```

To:

```
| UI/API facade + graph update command | `ui-service` | none (publishes to Redis) |
```

**c)** In Key Architectural Rules, update line 125 from:

```
- `ui-service` should remain read-only.
```

To:

```
- `ui-service` is primarily read-only; its only write is publishing `update.graph:v1` commands to Redis.
```

- [ ] **Step 4: Commit**

```bash
git add docs/arch/services/ui-service.md docs/arch/02-interaction-matrix.md docs/arch/01-topology.md
git commit -m "docs(arch): update ui-service docs for Redis dependency and graph update endpoint"
```

---

### Task 12: Rebuild ui-service image and smoke test

- [ ] **Step 1: Rebuild the ui-service Docker image**

```bash
docker compose build ui
```

Expected: builds successfully with the new `ioredis` dependency.

- [ ] **Step 2: Restart the ui container**

```bash
docker compose up -d ui
```

- [ ] **Step 3: Smoke test the endpoint from the host**

```bash
curl -sf -X POST http://localhost:8090/api/graph/update \
  -H "Content-Type: application/json" \
  -d '{"source":"s3"}'
```

Expected: `{"ok":true,"source":"s3"}`

- [ ] **Step 4: Test validation**

```bash
curl -s -X POST http://localhost:8090/api/graph/update \
  -H "Content-Type: application/json" \
  -d '{"source":"invalid"}'
```

Expected: HTTP 400 with `{"error":"source must be \"s3\" or \"local\""}`

- [ ] **Step 5: Test the host scripts**

```bash
./dbt/update-graph.sh
```

Expected: `Graph update triggered: {"ok":true,"source":"s3"}`

- [ ] **Step 6: Open browser and verify the Update Graph button appears**

Navigate to `http://localhost:8090`. Verify:
- "Update Graph" button is visible in the top-right header area
- Clicking it shows "Updating..." then briefly "Updated"
- The schedules list still loads correctly

- [ ] **Step 7: Commit (no code changes — verification only)**

No commit needed. If any issues were found, fix them in the relevant task files and commit the fix.
