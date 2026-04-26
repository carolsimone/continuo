# Schedule Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `CancelSchedule` propagate atomically through the system so no service continues processing for a cancelled run.

**Architecture:** State service bulk-cancels scheduler + tasks in one transaction and publishes `schedule.cancelled:v1` via outbox. Orchestrator, executor-controller, and k8s-controller each maintain a local `cancelled_schedules` Postgres table populated by consuming that stream, and guard their hot paths with a `SELECT EXISTS` check before doing any work. Cancellation is graceful — running K8s pods complete naturally; their results are suppressed at the outbox layer.

**Tech Stack:** Go 1.22, PostgreSQL (sqlx), Redis Streams (go-redis/v9), Node.js/TypeScript (ui-service), testify, grpc-go.

---

## File Map

| Action | Path | Purpose |
|---|---|---|
| Modify | `state/adapters/postgres/scheduler_repository.go` | Add `CancelTx` method |
| Modify | `state/adapters/postgres/task_repository.go` | Add `BulkCancelByScheduleIDTx` method |
| Modify | `state/internal/grpc/handlers/scheduler_handler.go` | Extend `CancelSchedule` — tx, bulk cancel, outbox |
| Modify | `state/internal/grpc/server.go` | Wire new `taskRepo` + `outboxRepo` + `db` into `SchedulerHandler` |
| Modify | `state/service/handlers/run_entries_dispatched_handler.go` | Race fix: no-op if scheduler already cancelled |
| Modify | `state/cmd/main.go` or `state/main.go` | Pass new deps to `NewSchedulerHandler` |
| Create | `db/migration/orchestrator/V5__init_cancelled_schedules.sql` | `cancelled_schedules` table |
| Create | `db/migration/executor/V6__init_cancelled_schedules.sql` | `cancelled_schedules` table |
| Create | `db/migration/k8s/V7__init_cancelled_schedules.sql` | `cancelled_schedules` table |
| Create | `orchestrator/adapters/postgres/cancelled_schedules_repository.go` | Insert/Exists/DeleteExpired |
| Create | `orchestrator/adapters/redis/schedule_cancelled_consumer.go` | Consumer for `schedule.cancelled:v1` |
| Modify | `orchestrator/config/config.go` | Add `ScheduleCancelledStream`, `ScheduleCancelledGroup`, `CancelledSchedulesTTLHours`, `CancelledSchedulesSweepIntervalMinutes` |
| Modify | `orchestrator/main.go` | Wire consumer + sweeper goroutine |
| Modify | `orchestrator/service/command/handle_node_completed.go` | Guard before cascade outbox writes |
| Create | `executor-controller/adapters/postgres/cancelled_schedules_repository.go` | Insert/Exists/DeleteExpired |
| Create | `executor-controller/adapters/redis/schedule_cancelled_consumer.go` | Consumer for `schedule.cancelled:v1` |
| Modify | `executor-controller/config/config.go` | Add stream/group/TTL/sweep env vars |
| Modify | `executor-controller/main.go` | Wire consumer + sweeper goroutine |
| Modify | `executor-controller/adapters/redis/consumer.go` | Guard in `processMessage` before deploy |
| Create | `k8s-controller/adapters/postgres/cancelled_schedules_repository.go` | Insert/Exists/DeleteExpired |
| Create | `k8s-controller/adapters/redis/schedule_cancelled_consumer.go` | Consumer for `schedule.cancelled:v1` |
| Modify | `k8s-controller/config/config.go` | Add stream/group/TTL/sweep env vars |
| Modify | `k8s-controller/main.go` | Wire consumer + sweeper goroutine |
| Modify | `k8s-controller/service/handlers/check_status_handler.go` | Guard after dedup, before outbox writes |
| Modify | `ui-service/proto/state.proto` | Add `CancelSchedule` RPC + messages |
| Modify | `ui-service/src/server/grpc-client.ts` | Add `cancelSchedule` to `GrpcClient` |
| Modify | `ui-service/src/server/routes/schedules.ts` | Add `POST /:name/cancel` route |
| Delete | `orchestrator/adapters/grpc/state_client.go` | Dead code — never wired up |
| Modify | `docs/arch/01-topology.md` | Add `schedule.cancelled:v1` to Redis topology |
| Modify | `docs/arch/03-sequence-flows.md` | Add cancel sequence flow |

---

## Task 1: State — Add `CancelTx` to `SchedulerTrackerRepository`

**Files:**
- Modify: `state/adapters/postgres/scheduler_repository.go`

- [ ] **Step 1: Write the failing test**

In `state/adapters/postgres/scheduler_repository_test.go` (or integration test), add:
```go
func TestSchedulerTrackerRepository_CancelTx(t *testing.T) {
    // Uses real DB via getTestDB() pattern from existing integration tests
    db := getTestDB(t)
    repo := NewSchedulerTrackerRepository(db, newTestLogger())
    ctx := context.Background()

    // Create a running scheduler
    id := uuid.New()
    require.NoError(t, repo.Create(ctx, &model.SchedulerTracker{
        ScheduleID:           id,
        ScheduleName:         "test-sched",
        Status:               model.SchedulerStatusRunning,
        CreatedAt:            time.Now(),
        InitializationStatus: "completed",
    }))

    tx, err := db.BeginTxx(ctx, nil)
    require.NoError(t, err)
    defer tx.Rollback()

    require.NoError(t, repo.CancelTx(ctx, tx, id, "test-user", "test reason"))
    require.NoError(t, tx.Commit())

    got, err := repo.GetByID(ctx, id)
    require.NoError(t, err)
    assert.Equal(t, model.SchedulerStatusCancelled, got.Status)
    assert.NotNil(t, got.CancelledAt)
    assert.Equal(t, "test-user", *got.CancelledBy)
    assert.Equal(t, "test reason", *got.CancellationReason)
}
```

- [ ] **Step 2: Add `CancelTx` to the interface**

In `state/adapters/postgres/scheduler_repository.go`, add to `SchedulerTrackerRepository` interface:
```go
// CancelTx cancels a scheduler within an existing transaction.
CancelTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy, reason string) error
```

- [ ] **Step 3: Implement `CancelTx`**

Add to `schedulerTrackerRepository`:
```go
func (r *schedulerTrackerRepository) CancelTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy, reason string) error {
    result, err := tx.ExecContext(ctx, `
        UPDATE scheduler_tracker
        SET status              = $1,
            cancelled_at        = $2,
            cancelled_by        = $3,
            cancellation_reason = $4
        WHERE schedule_id = $5
          AND status NOT IN ('succeeded', 'failed', 'cancelled')
    `, model.SchedulerStatusCancelled, time.Now(), cancelledBy, reason, scheduleID)
    if err != nil {
        return fmt.Errorf("cancel scheduler tx: %w", err)
    }
    rows, _ := result.RowsAffected()
    if rows == 0 {
        return ErrNotCancellable
    }
    return nil
}
```

- [ ] **Step 4: Run test**
```bash
docker exec state go test ./adapters/postgres/... -run TestSchedulerTrackerRepository_CancelTx -v
```
Expected: PASS

- [ ] **Step 5: Commit**
```bash
rtk git add state/adapters/postgres/scheduler_repository.go
rtk git commit -m "feat(state): add CancelTx to SchedulerTrackerRepository"
```

---

## Task 2: State — Add `BulkCancelByScheduleIDTx` to `TaskTrackerRepository`

**Files:**
- Modify: `state/adapters/postgres/task_repository.go`

- [ ] **Step 1: Write the failing test**

```go
func TestTaskTrackerRepository_BulkCancelByScheduleIDTx(t *testing.T) {
    db := getTestDB(t)
    schedRepo := NewSchedulerTrackerRepository(db, newTestLogger())
    taskRepo := NewTaskTrackerRepository(db, newTestLogger())
    ctx := context.Background()

    // Create scheduler
    schedID := uuid.New()
    require.NoError(t, schedRepo.Create(ctx, &model.SchedulerTracker{
        ScheduleID: schedID, ScheduleName: "s", Status: model.SchedulerStatusRunning,
        CreatedAt: time.Now(), InitializationStatus: "completed",
    }))

    // Create tasks in various states
    pending := uuid.New()
    running := uuid.New()
    succeeded := uuid.New()
    for _, tt := range []struct{ id uuid.UUID; status model.TaskStatus }{
        {pending, model.TaskStatusPending},
        {running, model.TaskStatusRunning},
        {succeeded, model.TaskStatusSucceeded},
    } {
        require.NoError(t, taskRepo.Create(ctx, &model.TaskTracker{
            TaskID: tt.id, ScheduleID: schedID, CreatedAt: time.Now(),
            ServiceName: "svc", SchemaName: "sch", TableName: tt.id.String(),
            JobName: "j-" + tt.id.String()[:8], Status: tt.status, MaxRetries: 0,
        }))
    }

    tx, err := db.BeginTxx(ctx, nil)
    require.NoError(t, err)
    defer tx.Rollback()

    n, err := taskRepo.BulkCancelByScheduleIDTx(ctx, tx, schedID, "user1")
    require.NoError(t, err)
    require.NoError(t, tx.Commit())
    assert.EqualValues(t, 2, n) // pending + running; succeeded untouched

    for _, id := range []uuid.UUID{pending, running} {
        got, _ := taskRepo.GetByID(ctx, id)
        assert.Equal(t, model.TaskStatusCancelled, got.Status)
        assert.NotNil(t, got.CancelledAt)
        assert.Equal(t, "user1", *got.CancelledBy)
    }
    got, _ := taskRepo.GetByID(ctx, succeeded)
    assert.Equal(t, model.TaskStatusSucceeded, got.Status) // untouched
}
```

- [ ] **Step 2: Add to the interface**

In `state/adapters/postgres/task_repository.go`, add to `TaskTrackerRepository` interface:
```go
// BulkCancelByScheduleIDTx sets status='cancelled' for all pending/running tasks
// in a schedule. Returns the number of rows updated.
BulkCancelByScheduleIDTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy string) (int64, error)
```

- [ ] **Step 3: Implement**

```go
func (r *taskTrackerRepository) BulkCancelByScheduleIDTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy string) (int64, error) {
    result, err := tx.ExecContext(ctx, `
        UPDATE task_tracker
        SET status       = $1,
            cancelled_at = $2,
            cancelled_by = $3
        WHERE schedule_id = $4
          AND status IN ('pending', 'running')
    `, model.TaskStatusCancelled, time.Now(), cancelledBy, scheduleID)
    if err != nil {
        return 0, fmt.Errorf("bulk cancel tasks: %w", err)
    }
    n, _ := result.RowsAffected()
    return n, nil
}
```

- [ ] **Step 4: Run test**
```bash
docker exec state go test ./adapters/postgres/... -run TestTaskTrackerRepository_BulkCancelByScheduleIDTx -v
```
Expected: PASS

- [ ] **Step 5: Commit**
```bash
rtk git add state/adapters/postgres/task_repository.go
rtk git commit -m "feat(state): add BulkCancelByScheduleIDTx to TaskTrackerRepository"
```

---

## Task 3: State — Extend `CancelSchedule` handler (tx + bulk cancel + outbox)

**Files:**
- Modify: `state/internal/grpc/handlers/scheduler_handler.go`
- Modify: `state/internal/grpc/server.go`
- Modify: `state/main.go` (or wherever `NewSchedulerHandler` is called)

- [ ] **Step 1: Write the failing test**

In `state/internal/grpc/handlers/scheduler_handler_test.go`:
```go
// stubTaskRepo implements TaskTrackerRepository minimally for cancel tests.
type stubTaskRepo struct {
    bulkCancelCalled bool
    bulkCancelN      int64
    err              error
}

func (s *stubTaskRepo) BulkCancelByScheduleIDTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) (int64, error) {
    s.bulkCancelCalled = true
    return s.bulkCancelN, s.err
}
// Implement all other interface methods as no-ops returning nil.
// (Add each method required by TaskTrackerRepository with empty bodies.)

type stubOutboxRepo struct {
    created []*postgres.OutboxEntry
}

func (s *stubOutboxRepo) Create(_ context.Context, _ *sqlx.Tx, e *postgres.OutboxEntry) error {
    s.created = append(s.created, e)
    return nil
}
func (s *stubOutboxRepo) ListPending(_ context.Context, _ int) ([]*postgres.OutboxEntry, error) { return nil, nil }
func (s *stubOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID) error                    { return nil }
func (s *stubOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error                   { return nil }

func TestSchedulerHandler_CancelSchedule_BulkCancelsTasks(t *testing.T) {
    schedID := uuid.New()
    taskRepo := &stubTaskRepo{bulkCancelN: 3}
    outboxRepo := &stubOutboxRepo{}
    schedRepo := &stubSchedulerRepo{
        activeScheduler: &model.SchedulerTracker{
            ScheduleID:   schedID,
            ScheduleName: "my-schedule",
            Status:       model.SchedulerStatusRunning,
        },
    }

    db, mock, err := sqlmock.New()  // or use an in-memory test db
    require.NoError(t, err)
    mock.ExpectBegin()
    mock.ExpectCommit()
    sqlxDB := sqlx.NewDb(db, "postgres")

    h := NewSchedulerHandler(schedRepo, nil, nil, nil, newTestLogger())
    h.WithCancelDeps(sqlxDB, taskRepo, outboxRepo) // new helper method

    resp, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
        ScheduleName:       "my-schedule",
        CancelledBy:        "operator",
        CancellationReason: "manual stop",
    })

    require.NoError(t, err)
    assert.Equal(t, schedID.String(), resp.ScheduleId)
    assert.True(t, taskRepo.bulkCancelCalled)
    require.Len(t, outboxRepo.created, 1)
    assert.Equal(t, "schedule.cancelled:v1", outboxRepo.created[0].StreamName)
    assert.Equal(t, schedID, outboxRepo.created[0].AggregateID)
}

func TestSchedulerHandler_CancelSchedule_NoActiveRun_ReturnsFailedPrecondition(t *testing.T) {
    h := NewSchedulerHandler(&stubSchedulerRepo{activeScheduler: nil}, nil, nil, nil, newTestLogger())
    _, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{ScheduleName: "no-run"})
    require.Error(t, err)
    s, _ := status.FromError(err)
    assert.Equal(t, codes.FailedPrecondition, s.Code())
}
```

- [ ] **Step 2: Update `SchedulerHandler` struct and constructor**

In `state/internal/grpc/handlers/scheduler_handler.go`, add fields and a `WithCancelDeps` method:
```go
type SchedulerHandler struct {
    repo            postgres.SchedulerTrackerRepository
    activator       schedulerpkg.ScheduleActivator
    catalogRepo     postgres.ScheduleCatalogRepository
    schedulesConfig *schedulerpkg.SchedulesConfig
    logger          *slog.Logger
    // Cancel dependencies — set via WithCancelDeps after construction.
    db         *sqlx.DB
    taskRepo   postgres.TaskTrackerRepository
    outboxRepo postgres.OutboxRepository
}

// WithCancelDeps injects the dependencies needed by CancelSchedule.
func (h *SchedulerHandler) WithCancelDeps(db *sqlx.DB, taskRepo postgres.TaskTrackerRepository, outboxRepo postgres.OutboxRepository) {
    h.db = db
    h.taskRepo = taskRepo
    h.outboxRepo = outboxRepo
}
```

- [ ] **Step 3: Rewrite `CancelSchedule`**

Replace the existing `CancelSchedule` implementation:
```go
func (h *SchedulerHandler) CancelSchedule(
    ctx context.Context,
    req *statev1.CancelScheduleRequest,
) (*statev1.CancelScheduleResponse, error) {
    if req.ScheduleName == "" {
        return nil, status.Errorf(codes.InvalidArgument, "schedule_name is required")
    }

    active, err := h.repo.GetActiveScheduler(ctx, req.ScheduleName)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "get active scheduler: %v", err)
    }
    if active == nil {
        return nil, status.Errorf(codes.FailedPrecondition,
            "no active run for schedule %q", req.ScheduleName)
    }

    tx, err := h.db.BeginTxx(ctx, nil)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
    }
    defer tx.Rollback() //nolint:errcheck

    if err := h.repo.CancelTx(ctx, tx, active.ScheduleID, req.CancelledBy, req.CancellationReason); err != nil {
        if errors.Is(err, postgres.ErrNotCancellable) {
            return nil, status.Errorf(codes.FailedPrecondition,
                "schedule %q run already in terminal state", req.ScheduleName)
        }
        h.logger.Error("Failed to cancel scheduler", "error", err)
        return nil, status.Errorf(codes.Internal, "cancel scheduler: %v", err)
    }

    if _, err := h.taskRepo.BulkCancelByScheduleIDTx(ctx, tx, active.ScheduleID, req.CancelledBy); err != nil {
        h.logger.Error("Failed to bulk-cancel tasks", "schedule_id", active.ScheduleID, "error", err)
        return nil, status.Errorf(codes.Internal, "bulk cancel tasks: %v", err)
    }

    payload, _ := json.Marshal(map[string]string{
        "schedule_id":   active.ScheduleID.String(),
        "schedule_name": active.ScheduleName,
    })
    if err := h.outboxRepo.Create(ctx, tx, &postgres.OutboxEntry{
        ID:            uuid.New(),
        AggregateType: "scheduler",
        AggregateID:   active.ScheduleID,
        EventType:     "schedule_cancelled",
        Payload:       payload,
        StreamName:    "schedule.cancelled:v1",
        Status:        "pending",
        MaxRetries:    3,
        CreatedAt:     time.Now(),
    }); err != nil {
        return nil, status.Errorf(codes.Internal, "write outbox: %v", err)
    }

    if err := tx.Commit(); err != nil {
        return nil, status.Errorf(codes.Internal, "commit: %v", err)
    }

    h.logger.Info("Schedule cancelled", "schedule_name", req.ScheduleName, "schedule_id", active.ScheduleID)
    return &statev1.CancelScheduleResponse{ScheduleId: active.ScheduleID.String()}, nil
}
```

- [ ] **Step 4: Wire `WithCancelDeps` in `state/main.go`**

Find where `NewSchedulerHandler` is called and add:
```go
schedulerHandler.WithCancelDeps(pgDB, taskRepo, outboxRepo)
```
(Use the existing `pgDB`, `taskRepo`, `outboxRepo` variables already present in `main.go`.)

- [ ] **Step 5: Run tests**
```bash
docker exec state go test ./internal/grpc/handlers/... -run TestSchedulerHandler_CancelSchedule -v
```
Expected: PASS

- [ ] **Step 6: Compile check**
```bash
docker exec state go build ./...
```
Expected: no errors

- [ ] **Step 7: Commit**
```bash
rtk git add state/
rtk git commit -m "feat(state): CancelSchedule atomically bulk-cancels tasks and writes outbox"
```

---

## Task 4: State — Fix `RunEntriesDispatchedHandler` race condition

**Files:**
- Modify: `state/service/handlers/run_entries_dispatched_handler.go`

- [ ] **Step 1: Write the failing test**

In `state/service/handlers/run_entries_dispatched_handler_test.go` (create if doesn't exist), add:
```go
func TestRunEntriesDispatchedHandler_NoopWhenSchedulerCancelled(t *testing.T) {
    db := getTestDB(t)
    schedRepo := postgres.NewSchedulerTrackerRepository(db, newTestLogger())
    taskRepo := postgres.NewTaskTrackerRepository(db, newTestLogger())
    ctx := context.Background()

    // Create a CANCELLED scheduler
    schedID := uuid.New()
    require.NoError(t, schedRepo.Create(ctx, &model.SchedulerTracker{
        ScheduleID: schedID, ScheduleName: "test", Status: model.SchedulerStatusRunning,
        CreatedAt: time.Now(), InitializationStatus: "completed",
    }))
    tx, _ := db.BeginTxx(ctx, nil)
    require.NoError(t, schedRepo.CancelTx(ctx, tx, schedID, "tester", "test"))
    require.NoError(t, tx.Commit())

    h := handlers.NewRunEntriesDispatchedHandler(db, schedRepo, taskRepo, newTestLogger())

    payload, _ := json.Marshal(events.RunEntriesDispatched{
        ScheduleID: schedID.String(),
        AllTasks: []events.DispatchedTask{
            {TaskID: uuid.New().String(), ServiceName: "s", SchemaName: "sc", TableName: "t", MaxRetries: 0},
        },
        TotalTaskCount: 1,
    })
    ack, err := h.Handle(ctx, "msg-1", string(payload))

    require.NoError(t, err)
    assert.True(t, ack, "should ACK (no-op) when scheduler already cancelled")

    // Scheduler status must remain cancelled, not revert to running
    got, _ := schedRepo.GetByID(ctx, schedID)
    assert.Equal(t, model.SchedulerStatusCancelled, got.Status)
}
```

- [ ] **Step 2: Add status check before `UpdateStatusTx`**

In `state/service/handlers/run_entries_dispatched_handler.go`, after the dedup check and before `BulkCreateTx`, add:
```go
// Guard: if the scheduler was cancelled between event publication and now, treat as no-op.
var currentStatus string
if dbErr := tx.QueryRowContext(ctx,
    `SELECT status FROM scheduler_tracker WHERE schedule_id = $1`,
    scheduleID,
).Scan(&currentStatus); dbErr != nil {
    return false, fmt.Errorf("read scheduler status: %w", dbErr)
}
if currentStatus == string(model.SchedulerStatusCancelled) {
    h.logger.Info("run.entries.dispatched: scheduler already cancelled — skipping", "schedule_id", scheduleID)
    _ = tx.Commit()
    return true, nil
}
```

- [ ] **Step 3: Run test**
```bash
docker exec state go test ./service/handlers/... -run TestRunEntriesDispatchedHandler_NoopWhenSchedulerCancelled -v
```
Expected: PASS

- [ ] **Step 4: Run full state tests**
```bash
docker exec state go test ./... 2>&1 | tail -20
```
Expected: all pass

- [ ] **Step 5: Commit**
```bash
rtk git add state/service/handlers/run_entries_dispatched_handler.go
rtk git commit -m "fix(state): RunEntriesDispatchedHandler no-ops when scheduler already cancelled"
```

---

## Task 5: DB Migrations — `cancelled_schedules` tables

**Files:**
- Create: `db/migration/orchestrator/V5__init_cancelled_schedules.sql`
- Create: `db/migration/executor/V6__init_cancelled_schedules.sql`
- Create: `db/migration/k8s/V7__init_cancelled_schedules.sql`

- [ ] **Step 1: Create all three migration files**

Content is identical for all three:
```sql
CREATE TABLE cancelled_schedules (
    schedule_id  UUID        PRIMARY KEY,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE cancelled_schedules IS
  'Local guard table populated from schedule.cancelled:v1 stream. '
  'Rows are swept after a configurable TTL (default 24h).';
```

- [ ] **Step 2: Apply migrations**
```bash
docker exec orchestrator flyway migrate
docker exec executor-controller flyway migrate
docker exec k8s-controller flyway migrate
```
Or via setup script:
```bash
bash scripts/setup.sh
```

- [ ] **Step 3: Commit**
```bash
rtk git add db/migration/
rtk git commit -m "feat(db): add cancelled_schedules table to orchestrator, executor, k8s migrations"
```

---

## Task 6: Orchestrator — `CancelledSchedulesRepository`

**Files:**
- Create: `orchestrator/adapters/postgres/cancelled_schedules_repository.go`

- [ ] **Step 1: Write failing tests**

Create `orchestrator/adapters/postgres/cancelled_schedules_repository_test.go`:
```go
package postgres_test

import (
    "context"
    "testing"
    "time"

    "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
    "github.com/google/uuid"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestCancelledSchedulesRepository_InsertAndExists(t *testing.T) {
    db := getTestDB(t) // use existing test helper pattern in orchestrator
    repo := postgres.NewCancelledSchedulesRepository(db)
    ctx := context.Background()

    id := uuid.New()
    assert.False(t, must(repo.Exists(ctx, id)))

    require.NoError(t, repo.Insert(ctx, id))
    assert.True(t, must(repo.Exists(ctx, id)))

    // Idempotent second insert
    require.NoError(t, repo.Insert(ctx, id))
}

func TestCancelledSchedulesRepository_DeleteExpired(t *testing.T) {
    db := getTestDB(t)
    repo := postgres.NewCancelledSchedulesRepository(db)
    ctx := context.Background()

    old := uuid.New()
    require.NoError(t, repo.Insert(ctx, old))
    // Manually backdate the row
    _, err := db.ExecContext(ctx,
        `UPDATE cancelled_schedules SET cancelled_at = $1 WHERE schedule_id = $2`,
        time.Now().Add(-25*time.Hour), old)
    require.NoError(t, err)

    fresh := uuid.New()
    require.NoError(t, repo.Insert(ctx, fresh))

    n, err := repo.DeleteExpired(ctx, 24*time.Hour)
    require.NoError(t, err)
    assert.EqualValues(t, 1, n)

    assert.False(t, must(repo.Exists(ctx, old)))
    assert.True(t, must(repo.Exists(ctx, fresh)))
}

func must[T any](v T, err error) T { return v }
```

- [ ] **Step 2: Create the repository**

Create `orchestrator/adapters/postgres/cancelled_schedules_repository.go`:
```go
package postgres

import (
    "context"
    "time"

    "github.com/google/uuid"
    "github.com/jmoiron/sqlx"
)

// CancelledSchedulesRepository provides read/write access to the cancelled_schedules table.
type CancelledSchedulesRepository interface {
    Insert(ctx context.Context, scheduleID uuid.UUID) error
    Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
    DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

type cancelledSchedulesRepository struct {
    db *sqlx.DB
}

func NewCancelledSchedulesRepository(db *sqlx.DB) CancelledSchedulesRepository {
    return &cancelledSchedulesRepository{db: db}
}

func (r *cancelledSchedulesRepository) Insert(ctx context.Context, scheduleID uuid.UUID) error {
    _, err := r.db.ExecContext(ctx,
        `INSERT INTO cancelled_schedules (schedule_id) VALUES ($1) ON CONFLICT DO NOTHING`,
        scheduleID)
    return err
}

func (r *cancelledSchedulesRepository) Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error) {
    var exists bool
    err := r.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)`,
        scheduleID).Scan(&exists)
    return exists, err
}

func (r *cancelledSchedulesRepository) DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error) {
    result, err := r.db.ExecContext(ctx,
        `DELETE FROM cancelled_schedules WHERE cancelled_at < $1`,
        time.Now().Add(-ttl))
    if err != nil {
        return 0, err
    }
    n, _ := result.RowsAffected()
    return n, nil
}
```

- [ ] **Step 3: Run tests**
```bash
docker exec orchestrator go test ./adapters/postgres/... -run TestCancelledSchedulesRepository -v
```
Expected: PASS

- [ ] **Step 4: Commit**
```bash
rtk git add orchestrator/adapters/postgres/cancelled_schedules_repository.go
rtk git commit -m "feat(orchestrator): add CancelledSchedulesRepository"
```

---

## Task 7: Orchestrator — Consumer, config, sweeper, and wiring

**Files:**
- Create: `orchestrator/adapters/redis/schedule_cancelled_consumer.go`
- Modify: `orchestrator/config/config.go`
- Modify: `orchestrator/main.go`

- [ ] **Step 1: Write failing test for consumer**

Create `orchestrator/adapters/redis/schedule_cancelled_consumer_test.go`:
```go
package redis_test

import (
    "context"
    "testing"

    "github.com/carolsimone/continuo/orchestrator/adapters/redis"
    "github.com/google/uuid"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

type fakeCancelledRepo struct {
    inserted []uuid.UUID
}

func (f *fakeCancelledRepo) Insert(_ context.Context, id uuid.UUID) error {
    f.inserted = append(f.inserted, id)
    return nil
}
func (f *fakeCancelledRepo) Exists(_ context.Context, _ uuid.UUID) (bool, error) { return false, nil }
func (f *fakeCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) { return 0, nil }

func TestScheduleCancelledHandler_InsertsScheduleID(t *testing.T) {
    repo := &fakeCancelledRepo{}
    handler := redis.NewScheduleCancelledHandler(repo, newTestLogger())

    id := uuid.New()
    msg := goredis.XMessage{
        ID: "1-0",
        Values: map[string]interface{}{
            "schedule_id":   id.String(),
            "schedule_name": "test-schedule",
        },
    }

    err := handler(context.Background(), msg)
    require.NoError(t, err)
    require.Len(t, repo.inserted, 1)
    assert.Equal(t, id, repo.inserted[0])
}
```

- [ ] **Step 2: Create the handler function**

Create `orchestrator/adapters/redis/schedule_cancelled_consumer.go`:
```go
package redis

import (
    "context"
    "fmt"
    "log/slog"

    "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
    "github.com/google/uuid"
    goredis "github.com/redis/go-redis/v9"
)

// NewScheduleCancelledHandler returns a MessageHandler that records cancelled
// schedule IDs in the local cancelled_schedules table.
func NewScheduleCancelledHandler(
    repo postgres.CancelledSchedulesRepository,
    logger *slog.Logger,
) MessageHandler {
    return func(ctx context.Context, msg goredis.XMessage) error {
        idStr, _ := msg.Values["schedule_id"].(string)
        scheduleID, err := uuid.Parse(idStr)
        if err != nil {
            logger.Error("schedule.cancelled: invalid schedule_id — discarding", "id", idStr)
            return nil // permanent error: ack implicitly by returning nil from MessageHandler
        }
        if err := repo.Insert(ctx, scheduleID); err != nil {
            return fmt.Errorf("insert cancelled schedule %s: %w", scheduleID, err)
        }
        logger.Info("Recorded cancelled schedule", "schedule_id", scheduleID)
        return nil
    }
}
```

- [ ] **Step 3: Add config fields**

In `orchestrator/config/config.go`, add to `Config` struct:
```go
ScheduleCancelledStream            string
ScheduleCancelledGroup             string
CancelledSchedulesTTLHours         int
CancelledSchedulesSweepIntervalMin int
```

In `Load`, add:
```go
ScheduleCancelledStream:            v.Require("SCHEDULE_CANCELLED_STREAM"),
ScheduleCancelledGroup:             v.Require("SCHEDULE_CANCELLED_GROUP"),
CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),
```

- [ ] **Step 4: Wire in `orchestrator/main.go`**

After repository initialization, add:
```go
cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

// Consumer: schedule.cancelled:v1 -> record in local cancelled_schedules
scheduleCancelledHandler := redis.NewScheduleCancelledHandler(cancelledSchedulesRepo, logger)
scheduleCancelledConsumer := redis.NewStreamConsumer(
    redisClient,
    cfg.ScheduleCancelledStream,
    cfg.ScheduleCancelledGroup,
    scheduleCancelledHandler,
    logger,
)
go func() {
    if err := scheduleCancelledConsumer.Start(ctx); err != nil {
        logger.Error("Schedule cancelled consumer error", "error", err)
    }
}()

// Sweeper: purge expired rows from cancelled_schedules
go func() {
    ticker := time.NewTicker(time.Duration(cfg.CancelledSchedulesSweepIntervalMin) * time.Minute)
    defer ticker.Stop()
    ttl := time.Duration(cfg.CancelledSchedulesTTLHours) * time.Hour
    for {
        select {
        case <-ticker.C:
            if n, err := cancelledSchedulesRepo.DeleteExpired(ctx, ttl); err != nil {
                logger.Error("cancelled_schedules sweep failed", "error", err)
            } else if n > 0 {
                logger.Info("Swept expired cancelled_schedules rows", "count", n)
            }
        case <-ctx.Done():
            return
        }
    }
}()
```

Pass `cancelledSchedulesRepo` to `NewHandleNodeCompletedHandler` (Task 8 will add the parameter).

- [ ] **Step 5: Run tests**
```bash
docker exec orchestrator go test ./adapters/redis/... -run TestScheduleCancelledHandler -v
```
Expected: PASS

- [ ] **Step 6: Compile check**
```bash
docker exec orchestrator go build ./...
```

- [ ] **Step 7: Commit**
```bash
rtk git add orchestrator/
rtk git commit -m "feat(orchestrator): schedule.cancelled:v1 consumer + cancelled_schedules sweeper"
```

---

## Task 8: Orchestrator — Guard in `HandleNodeCompletedHandler`

**Files:**
- Modify: `orchestrator/service/command/handle_node_completed.go`

- [ ] **Step 1: Write failing test**

In `orchestrator/service/command/handle_node_completed_test.go`, add:
```go
func TestHandleNodeCompleted_DropsOutboxWhenScheduleCancelled(t *testing.T) {
    scheduleID := uuid.New()

    cancelledRepo := &fakeCancelledSchedulesRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
    uow := newFakeUnitOfWork()
    runRepo := &fakeRunRepository{readyDownstream: []run.TopologyNode{{
        ServiceName: "svc", SchemaName: "sc", TableName: "t1", NodeType: "dbt_model",
    }}}

    handler := command.NewHandleNodeCompletedHandler(uow, runRepo, cancelledRepo, newTestLogger())

    err := handler.Handle(context.Background(), domainCmd.HandleNodeCompletedCmd{
        ScheduleID:   scheduleID,
        ScheduleName: "my-schedule",
        SchemaName:   "sc",
        TableName:    "t0",
        Status:       "SUCCEEDED",
    }, "msg-1")

    require.NoError(t, err)
    // Neo4j update happened
    assert.True(t, runRepo.updateNodeStatusCalled)
    // No outbox entries written
    assert.Empty(t, uow.outboxRepo.created)
}

// fakeCancelledSchedulesRepo implements postgres.CancelledSchedulesRepository
type fakeCancelledSchedulesRepo struct {
    ids map[uuid.UUID]bool
}
func (f *fakeCancelledSchedulesRepo) Insert(_ context.Context, id uuid.UUID) error { f.ids[id] = true; return nil }
func (f *fakeCancelledSchedulesRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) { return f.ids[id], nil }
func (f *fakeCancelledSchedulesRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) { return 0, nil }
```

- [ ] **Step 2: Add `cancelledSchedulesRepo` to `HandleNodeCompletedHandler`**

In `orchestrator/service/command/handle_node_completed.go`:
```go
type HandleNodeCompletedHandler struct {
    uow                  uow.UnitOfWork
    runRepo              run.Repository
    cancelledSchedules   postgresadapter.CancelledSchedulesRepository
    logger               *slog.Logger
}

func NewHandleNodeCompletedHandler(
    u uow.UnitOfWork,
    runRepo run.Repository,
    cancelledSchedules postgresadapter.CancelledSchedulesRepository,
    logger *slog.Logger,
) *HandleNodeCompletedHandler {
    return &HandleNodeCompletedHandler{
        uow:                u,
        runRepo:            runRepo,
        cancelledSchedules: cancelledSchedules,
        logger:             logger,
    }
}
```

(Add `postgresadapter "github.com/carolsimone/continuo/orchestrator/adapters/postgres"` to imports.)

- [ ] **Step 3: Add guard in `Handle`**

After `UpdateNodeStatus` and before the `if cmd.Status == "SUCCEEDED"` block, add:
```go
// Guard: if the schedule was cancelled, record the node status but do not cascade.
cancelled, err := h.cancelledSchedules.Exists(ctx, cmd.ScheduleID)
if err != nil {
    return fmt.Errorf("cancelled schedules check: %w", err)
}
if cancelled {
    h.logger.Info("Schedule is cancelled — suppressing cascade",
        "schedule_id", cmd.ScheduleID, "table", cmd.TableName)
    // Commit the message_processing record and return — no outbox entries.
    if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
        return fmt.Errorf("update message state: %w", err)
    }
    return h.uow.Commit()
}
```

- [ ] **Step 4: Update `main.go` call site**

In `orchestrator/main.go`, update:
```go
handleNodeCompletedHandler := command.NewHandleNodeCompletedHandler(unitOfWork, runRepo, cancelledSchedulesRepo, logger)
```

- [ ] **Step 5: Run tests**
```bash
docker exec orchestrator go test ./service/command/... -run TestHandleNodeCompleted -v
docker exec orchestrator go build ./...
```
Expected: PASS, no build errors

- [ ] **Step 6: Commit**
```bash
rtk git add orchestrator/
rtk git commit -m "feat(orchestrator): suppress cascade in HandleNodeCompletedHandler for cancelled schedules"
```

---

## Task 9: Executor-controller — Repository, consumer, config, sweeper, wiring

**Files:**
- Create: `executor-controller/adapters/postgres/cancelled_schedules_repository.go`
- Create: `executor-controller/adapters/redis/schedule_cancelled_consumer.go`
- Modify: `executor-controller/config/config.go`
- Modify: `executor-controller/main.go`

- [ ] **Step 1: Create repository** (identical structure to Task 6 Step 2)

Create `executor-controller/adapters/postgres/cancelled_schedules_repository.go`:
```go
package postgres

import (
    "context"
    "time"

    "github.com/google/uuid"
    "github.com/jmoiron/sqlx"
)

type CancelledSchedulesRepository interface {
    Insert(ctx context.Context, scheduleID uuid.UUID) error
    Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
    DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

type cancelledSchedulesRepository struct{ db *sqlx.DB }

func NewCancelledSchedulesRepository(db *sqlx.DB) CancelledSchedulesRepository {
    return &cancelledSchedulesRepository{db: db}
}

func (r *cancelledSchedulesRepository) Insert(ctx context.Context, id uuid.UUID) error {
    _, err := r.db.ExecContext(ctx,
        `INSERT INTO cancelled_schedules (schedule_id) VALUES ($1) ON CONFLICT DO NOTHING`, id)
    return err
}

func (r *cancelledSchedulesRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
    var exists bool
    err := r.db.QueryRowContext(ctx,
        `SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)`, id).Scan(&exists)
    return exists, err
}

func (r *cancelledSchedulesRepository) DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error) {
    res, err := r.db.ExecContext(ctx,
        `DELETE FROM cancelled_schedules WHERE cancelled_at < $1`, time.Now().Add(-ttl))
    if err != nil {
        return 0, err
    }
    n, _ := res.RowsAffected()
    return n, nil
}
```

- [ ] **Step 2: Create `ScheduleCancelledConsumer`**

Create `executor-controller/adapters/redis/schedule_cancelled_consumer.go`:
```go
package redis

import (
    "context"
    "fmt"
    "log/slog"
    "strings"
    "time"

    "github.com/carolsimone/continuo/executor-controller/adapters/postgres"
    "github.com/google/uuid"
    goredis "github.com/redis/go-redis/v9"
)

// ScheduleCancelledConsumer consumes schedule.cancelled:v1 and records schedule IDs
// in the local cancelled_schedules table so the deploy path can drop in-flight jobs.
type ScheduleCancelledConsumer struct {
    client        *goredis.Client
    streamName    string
    consumerGroup string
    consumerName  string
    repo          postgres.CancelledSchedulesRepository
    logger        *slog.Logger
    stopCh        chan struct{}
}

func NewScheduleCancelledConsumer(
    client *goredis.Client,
    streamName string,
    repo postgres.CancelledSchedulesRepository,
    logger *slog.Logger,
) (*ScheduleCancelledConsumer, error) {
    c := &ScheduleCancelledConsumer{
        client:        client,
        streamName:    streamName,
        consumerGroup: "executor-schedule-cancelled",
        consumerName:  fmt.Sprintf("consumer-%s", uuid.New().String()[:8]),
        repo:          repo,
        logger:        logger,
        stopCh:        make(chan struct{}),
    }
    err := client.XGroupCreateMkStream(context.Background(), streamName, c.consumerGroup, "0").Err()
    if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
        return nil, fmt.Errorf("create consumer group for %s: %w", streamName, err)
    }
    return c, nil
}

func (c *ScheduleCancelledConsumer) Start(ctx context.Context) error {
    c.logger.Info("Starting ScheduleCancelledConsumer", "stream", c.streamName)
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-c.stopCh:
            return nil
        default:
            if err := c.readAndProcess(ctx); err != nil {
                c.logger.Error("Consumer error", "error", err)
                time.Sleep(3 * time.Second)
            }
        }
    }
}

func (c *ScheduleCancelledConsumer) readAndProcess(ctx context.Context) error {
    streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
        Group: c.consumerGroup, Consumer: c.consumerName,
        Streams: []string{c.streamName, ">"}, Count: 10, Block: time.Second,
    }).Result()
    if err != nil {
        if err == goredis.Nil {
            return nil
        }
        if strings.Contains(err.Error(), "NOGROUP") {
            return c.client.XGroupCreateMkStream(ctx, c.streamName, c.consumerGroup, "0").Err()
        }
        return fmt.Errorf("xreadgroup: %w", err)
    }
    for _, stream := range streams {
        for _, msg := range stream.Messages {
            idStr, _ := msg.Values["schedule_id"].(string)
            scheduleID, err := uuid.Parse(idStr)
            if err != nil {
                c.logger.Error("schedule.cancelled: invalid schedule_id — discarding", "id", idStr)
                _ = c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID)
                continue
            }
            if err := c.repo.Insert(ctx, scheduleID); err != nil {
                c.logger.Error("Failed to insert cancelled schedule", "id", scheduleID, "error", err)
                continue // leave in PEL for retry
            }
            _ = c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID)
        }
    }
    return nil
}
```

- [ ] **Step 3: Add config fields**

In `executor-controller/config/config.go`:
```go
ScheduleCancelledStream            string
ScheduleCancelledGroup             string
CancelledSchedulesTTLHours         int
CancelledSchedulesSweepIntervalMin int
```

In `Load`:
```go
ScheduleCancelledStream:            v.Require("SCHEDULE_CANCELLED_STREAM"),
ScheduleCancelledGroup:             v.Require("SCHEDULE_CANCELLED_GROUP"),
CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),
```

- [ ] **Step 4: Wire in `executor-controller/main.go`**

After Postgres client init:
```go
cancelledSchedulesRepo := postgres.NewCancelledSchedulesRepository(pgDB)

cancelledConsumer, err := redis.NewScheduleCancelledConsumer(redisClient, cfg.ScheduleCancelledStream, cancelledSchedulesRepo, logger)
if err != nil {
    logger.Error("Failed to create schedule cancelled consumer", "error", err)
    os.Exit(1)
}
go func() {
    if err := cancelledConsumer.Start(ctx); err != nil {
        logger.Error("Schedule cancelled consumer error", "error", err)
    }
}()

go func() {
    ticker := time.NewTicker(time.Duration(cfg.CancelledSchedulesSweepIntervalMin) * time.Minute)
    defer ticker.Stop()
    ttl := time.Duration(cfg.CancelledSchedulesTTLHours) * time.Hour
    for {
        select {
        case <-ticker.C:
            if n, err := cancelledSchedulesRepo.DeleteExpired(ctx, ttl); err != nil {
                logger.Error("cancelled_schedules sweep failed", "error", err)
            } else if n > 0 {
                logger.Info("Swept expired cancelled_schedules rows", "count", n)
            }
        case <-ctx.Done():
            return
        }
    }
}()
```

Pass `cancelledSchedulesRepo` to the existing `Consumer` constructor (Task 10 will add the parameter).

- [ ] **Step 5: Run tests and compile**
```bash
docker exec executor-controller go test ./adapters/... -v
docker exec executor-controller go build ./...
```
Expected: PASS / no errors

- [ ] **Step 6: Commit**
```bash
rtk git add executor-controller/
rtk git commit -m "feat(executor-controller): schedule.cancelled:v1 consumer + cancelled_schedules sweeper"
```

---

## Task 10: Executor-controller — Guard in `processMessage`

**Files:**
- Modify: `executor-controller/adapters/redis/consumer.go`

- [ ] **Step 1: Write failing test**

In `executor-controller/adapters/redis/consumer_test.go`, add:
```go
func TestConsumer_DropsQueryModelWhenScheduleCancelled(t *testing.T) {
    scheduleID := uuid.New()
    taskID := uuid.New()

    cancelledRepo := &fakeCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
    bus := &fakeMessageBus{}
    consumer := newTestConsumer(t, bus, cancelledRepo)

    msg := goredis.XMessage{
        ID: "1-0",
        Values: map[string]interface{}{
            "schedule_id":   scheduleID.String(),
            "task_id":       taskID.String(),
            "schedule_name": "s", "service_name": "svc",
            "schema_name": "sc", "table_name": "t",
            "job_name": "j", "node_type": "dbt_model",
            "outbox_entry_id": uuid.New().String(),
        },
    }

    err := consumer.processMessage(context.Background(), msg, "query.model:v1")
    require.NoError(t, err)
    assert.Empty(t, bus.handled) // no commands dispatched
}

type fakeCancelledRepo struct {
    ids map[uuid.UUID]bool
}
func (f *fakeCancelledRepo) Insert(_ context.Context, id uuid.UUID) error { return nil }
func (f *fakeCancelledRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) { return f.ids[id], nil }
func (f *fakeCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) { return 0, nil }
```

- [ ] **Step 2: Add `cancelledSchedules` to `Consumer`**

In `executor-controller/adapters/redis/consumer.go`, add to `Consumer` struct:
```go
cancelledSchedules postgres.CancelledSchedulesRepository
```

Update `NewConsumer` (or the constructor function) to accept and store `cancelledSchedules postgres.CancelledSchedulesRepository`.

- [ ] **Step 3: Add guard in `processMessage`**

After the deduplication check block and before `cmd := command.DeployJob{...}`, add:
```go
// Guard: drop the message if the schedule was cancelled.
cancelled, err := c.cancelledSchedules.Exists(ctx, scheduleID)
if err != nil {
    return fmt.Errorf("cancelled schedules check: %w", err)
}
if cancelled {
    c.logger.Info("Schedule cancelled — dropping deploy message",
        "schedule_id", scheduleID, "task_id", taskID)
    return c.client.XAck(ctx, streamName, c.consumerGroup, msg.ID).Err()
}
```

- [ ] **Step 4: Run tests**
```bash
docker exec executor-controller go test ./adapters/redis/... -run TestConsumer -v
docker exec executor-controller go build ./...
```
Expected: PASS

- [ ] **Step 5: Commit**
```bash
rtk git add executor-controller/adapters/redis/consumer.go
rtk git commit -m "feat(executor-controller): drop query.model:v1 messages for cancelled schedules"
```

---

## Task 11: K8s-controller — Repository, consumer, config, sweeper, wiring

**Files:**
- Create: `k8s-controller/adapters/postgres/cancelled_schedules_repository.go`
- Create: `k8s-controller/adapters/redis/schedule_cancelled_consumer.go`
- Modify: `k8s-controller/config/config.go`
- Modify: `k8s-controller/main.go`

- [ ] **Step 1: Create repository**

Create `k8s-controller/adapters/postgres/cancelled_schedules_repository.go` — identical content to executor-controller Task 9 Step 1 but with package `package postgres`.

- [ ] **Step 2: Create `ScheduleCancelledConsumer`**

Create `k8s-controller/adapters/redis/schedule_cancelled_consumer.go` — identical content to executor-controller Task 9 Step 2 but with:
- package `package redis`
- import path `github.com/carolsimone/continuo/k8s-controller/adapters/postgres`
- consumer group `"k8s-schedule-cancelled"`

- [ ] **Step 3: Add config fields**

In `k8s-controller/config/config.go`, add the same four fields and `Load` lines as in executor-controller (Task 9 Step 3).

- [ ] **Step 4: Wire in `k8s-controller/main.go`**

Same sweeper + consumer wiring as executor-controller (Task 9 Step 4), using k8s-controller package paths. Pass `cancelledSchedulesRepo` to `NewCheckStatusHandler` (Task 12 will add the parameter).

- [ ] **Step 5: Compile check**
```bash
docker exec k8s-controller go build ./...
```

- [ ] **Step 6: Commit**
```bash
rtk git add k8s-controller/
rtk git commit -m "feat(k8s-controller): schedule.cancelled:v1 consumer + cancelled_schedules sweeper"
```

---

## Task 12: K8s-controller — Guard in `CheckStatusHandler`

**Files:**
- Modify: `k8s-controller/service/handlers/check_status_handler.go`

- [ ] **Step 1: Write failing test**

In `k8s-controller/service/handlers/check_status_handler_test.go`, add:
```go
func TestCheckStatusHandler_DropsOutboxWhenScheduleCancelled(t *testing.T) {
    scheduleID := uuid.New()
    cancelledRepo := &fakeCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}

    handler := newHandler(t, WithCancelledRepo(cancelledRepo))

    cmd := command.CheckJobStatus{
        TaskID:       uuid.New(),
        ScheduleID:   scheduleID,
        ScheduleName: "my-schedule",
        ServiceName:  "svc", SchemaName: "sc", TableName: "t",
        JobName: "job-1",
    }

    // Fake K8s reports job as succeeded
    handler.k8sClient = &fakeK8sClient{result: &model.K8sPodResult{Status: model.JobStatusSucceeded}}

    err := handler.Handle(context.Background(), cmd)
    require.NoError(t, err)

    // No outbox entries should have been written
    assert.Empty(t, handler.uow.outboxRepo.created)
}

type fakeCancelledRepo struct {
    ids map[uuid.UUID]bool
}
func (f *fakeCancelledRepo) Insert(_ context.Context, id uuid.UUID) error { return nil }
func (f *fakeCancelledRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) { return f.ids[id], nil }
func (f *fakeCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) { return 0, nil }
```

- [ ] **Step 2: Add `cancelledSchedules` to `CheckStatusHandler`**

In `k8s-controller/service/handlers/check_status_handler.go`:
```go
type CheckStatusHandler struct {
    k8sClient          K8sStatusChecker
    uow                uow.UnitOfWork
    logUploader        s3adapter.LogUploader
    config             *HandlerConfig
    cancelledSchedules postgresadapter.CancelledSchedulesRepository
    logger             *slog.Logger
}

func NewCheckStatusHandler(
    k8sClient K8sStatusChecker,
    uow uow.UnitOfWork,
    logUploader s3adapter.LogUploader,
    config *HandlerConfig,
    cancelledSchedules postgresadapter.CancelledSchedulesRepository,
    logger *slog.Logger,
) *CheckStatusHandler {
    return &CheckStatusHandler{
        k8sClient:          k8sClient,
        uow:                uow,
        logUploader:        logUploader,
        config:             config,
        cancelledSchedules: cancelledSchedules,
        logger:             logger,
    }
}
```

- [ ] **Step 3: Add guard in `Handle`**

After the dedup guard (Step 4, line ~100) and before the `switch result.Status` block, add:
```go
// Guard: if the schedule was cancelled, absorb the result silently.
// Running pods are left to complete naturally (graceful cancellation by design).
cancelled, err := h.cancelledSchedules.Exists(ctx, cmd.ScheduleID)
if err != nil {
    return fmt.Errorf("cancelled schedules check: %w", err)
}
if cancelled {
    h.logger.Info("Schedule cancelled — absorbing job result",
        "schedule_id", cmd.ScheduleID, "job_name", cmd.JobName, "status", result.Status)
    // Rollback the transaction (no outbox written); consumer will XACK.
    return nil
}
```

- [ ] **Step 4: Update call site in `main.go`**

Add `cancelledSchedulesRepo` as a parameter to `NewCheckStatusHandler`.

- [ ] **Step 5: Run tests**
```bash
docker exec k8s-controller go test ./service/handlers/... -run TestCheckStatusHandler -v
docker exec k8s-controller go build ./...
```
Expected: PASS

- [ ] **Step 6: Commit**
```bash
rtk git add k8s-controller/
rtk git commit -m "feat(k8s-controller): absorb job results for cancelled schedules in CheckStatusHandler"
```

---

## Task 13: ui-service — proto, grpc-client, route

**Files:**
- Modify: `ui-service/proto/state.proto`
- Modify: `ui-service/src/server/grpc-client.ts`
- Modify: `ui-service/src/server/routes/schedules.ts`

- [ ] **Step 1: Write failing test**

In `ui-service/tests/routes/schedules.test.ts`, add:
```typescript
describe('POST /api/schedules/:name/cancel', () => {
  it('returns 200 with schedule_id on success', async () => {
    const mockStateClient = {
      cancelSchedule: vi.fn((req, cb) => cb(null, { schedule_id: 'abc-123' })),
    };
    const app = createApp(mockStateClient as any, mockGraphClient as any, null);
    const res = await request(app)
      .post('/api/schedules/my-schedule/cancel')
      .send({ cancelled_by: 'operator', cancellation_reason: 'manual' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ schedule_id: 'abc-123' });
    expect(mockStateClient.cancelSchedule).toHaveBeenCalledWith(
      { schedule_name: 'my-schedule', cancelled_by: 'operator', cancellation_reason: 'manual' },
      expect.any(Function),
    );
  });

  it('returns 409 on FAILED_PRECONDITION', async () => {
    const err = { code: 9, message: 'no active run' }; // grpc FAILED_PRECONDITION = 9
    const mockStateClient = {
      cancelSchedule: vi.fn((req, cb) => cb(err, null)),
    };
    const app = createApp(mockStateClient as any, mockGraphClient as any, null);
    const res = await request(app).post('/api/schedules/no-run/cancel').send({});
    expect(res.status).toBe(409);
  });
});
```

- [ ] **Step 2: Run test to confirm it fails**
```bash
cd ui-service && rtk vitest run tests/routes/schedules.test.ts
```
Expected: FAIL — `cancelSchedule` not found

- [ ] **Step 3: Update `ui-service/proto/state.proto`**

Add to `service StateService` block (after `TriggerSchedule`):
```proto
// CancelSchedule cancels the active run of a named schedule.
// Errors: INVALID_ARGUMENT (empty name), FAILED_PRECONDITION (no active run or run not cancellable).
rpc CancelSchedule(CancelScheduleRequest) returns (CancelScheduleResponse);
```

Add message definitions (after `TriggerScheduleResponse`):
```proto
message CancelScheduleRequest {
  string schedule_name       = 1;
  string cancelled_by        = 2;
  string cancellation_reason = 3;
}

message CancelScheduleResponse {
  string schedule_id = 1;
}
```

- [ ] **Step 4: Add `cancelSchedule` to `GrpcClient` interface**

In `ui-service/src/server/grpc-client.ts`, add to `GrpcClient` interface:
```typescript
cancelSchedule: (request: any, callback: (err: any, res: any) => void) => void;
```

- [ ] **Step 5: Add cancel route**

In `ui-service/src/server/routes/schedules.ts`, inside `createSchedulesRouter`, add after the trigger route:
```typescript
// POST /api/schedules/:name/cancel — cancel the active run of a named schedule
router.post('/:name/cancel', (req, res) => {
  stateClient.cancelSchedule(
    {
      schedule_name:       req.params.name,
      cancelled_by:        req.body.cancelled_by,
      cancellation_reason: req.body.cancellation_reason,
    },
    (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      res.json({ schedule_id: response.schedule_id });
    },
  );
});
```

- [ ] **Step 6: Run tests**
```bash
cd ui-service && rtk vitest run tests/routes/schedules.test.ts
```
Expected: PASS

- [ ] **Step 7: Commit**
```bash
rtk git add ui-service/
rtk git commit -m "feat(ui-service): add POST /api/schedules/:name/cancel route"
```

---

## Task 14: Cleanup — delete dead `orchestrator/adapters/grpc/state_client.go`

**Files:**
- Delete: `orchestrator/adapters/grpc/state_client.go`

- [ ] **Step 1: Check nothing imports it**
```bash
grep -r "orchestrator/adapters/grpc" /Users/simonecarolini/Desktop/github/continuo/orchestrator --include="*.go" | grep -v "state_client.go\|_test.go"
```
Expected: only the gRPC query handler (not `state_client.go`). Confirm the output before deleting.

- [ ] **Step 2: Delete the file**
```bash
rm orchestrator/adapters/grpc/state_client.go
```

- [ ] **Step 3: Compile check**
```bash
docker exec orchestrator go build ./...
```
Expected: no errors

- [ ] **Step 4: Commit**
```bash
rtk git add orchestrator/adapters/grpc/state_client.go
rtk git commit -m "chore(orchestrator): remove unused StateClient dead code"
```

---

## Task 15: docker-compose.yml — add new env vars

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add env vars to all affected services**

For `orchestrator`, `executor-controller`, and `k8s-controller` service definitions, add:
```yaml
- SCHEDULE_CANCELLED_STREAM=schedule.cancelled:v1
- SCHEDULE_CANCELLED_GROUP=<service-name>-schedule-cancelled
- CANCELLED_SCHEDULES_TTL_HOURS=24
- CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES=60
```

Use distinct group names:
- orchestrator: `orchestrator-schedule-cancelled`
- executor-controller: `executor-schedule-cancelled`
- k8s-controller: `k8s-schedule-cancelled`

- [ ] **Step 2: Commit**
```bash
rtk git add docker-compose.yml
rtk git commit -m "chore: add SCHEDULE_CANCELLED_STREAM env vars to docker-compose"
```

---

## Task 16: Update architecture documentation

**Files:**
- Modify: `docs/arch/01-topology.md`
- Modify: `docs/arch/03-sequence-flows.md`

- [ ] **Step 1: Add `schedule.cancelled:v1` to Redis topology diagram**

In `docs/arch/01-topology.md`, in the Redis Topology flowchart, add:
```
SC_EV[schedule.cancelled:v1]
ST --> SC_EV
SC_EV --> OR
SC_EV --> EC
SC_EV --> KC
```

- [ ] **Step 2: Add cancel sequence flow**

In `docs/arch/03-sequence-flows.md`, add a new section `## 6. Schedule Cancellation` with the mermaid diagram from `docs/arch/schedule-cancellation.md`.

- [ ] **Step 3: Commit**
```bash
rtk git add docs/arch/
rtk git commit -m "docs(arch): add schedule.cancelled:v1 to topology and sequence flows"
```

---

## Task 17: Integration smoke test

- [ ] **Step 1: Start the full stack**
```bash
bash scripts/setup.sh
docker compose up -d
```

- [ ] **Step 2: Trigger a schedule and immediately cancel it**
```bash
# Trigger
curl -s -X POST http://localhost:<ui-port>/api/schedules/e2e-schedule/trigger

# Cancel (within a few seconds, before tasks finish)
curl -s -X POST http://localhost:<ui-port>/api/schedules/e2e-schedule/cancel \
  -H "Content-Type: application/json" \
  -d '{"cancelled_by": "smoke-test"}'
```

- [ ] **Step 3: Verify state**
```bash
# scheduler_tracker should be cancelled
docker exec postgres psql -U continuo continuo_state \
  -c "SELECT status, cancelled_by FROM scheduler_tracker ORDER BY created_at DESC LIMIT 1"

# task_tracker rows should all be cancelled (non-terminal ones)
docker exec postgres psql -U continuo continuo_state \
  -c "SELECT status, count(*) FROM task_tracker WHERE schedule_id = '<id>' GROUP BY status"
```
Expected: `cancelled` for scheduler and all non-terminal tasks.

- [ ] **Step 4: Verify no new K8s jobs created after cancel**
```bash
kubectl get jobs --all-namespaces | grep e2e-schedule
```
Expected: no new jobs created after cancel timestamp.

- [ ] **Step 5: Run e2e tests (read e2e/README.md for exact command)**
```bash
# Follow e2e/README.md
```
Expected: existing happy-path and failure-path tests still pass.

- [ ] **Step 6: Final commit if any fixes were needed**
```bash
rtk git add .
rtk git commit -m "test(e2e): verify schedule cancellation smoke test passes"
```
