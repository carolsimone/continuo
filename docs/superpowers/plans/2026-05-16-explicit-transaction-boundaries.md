# Explicit Transaction Boundaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace shared mutable unit-of-work state in `executor-controller` and `k8s-controller` with explicit transaction callbacks so split consumers can process messages concurrently without colliding on one in-memory transaction slot.

**Architecture:** Each service gets a concurrency-safe `TransactionRunner` that opens one fresh `*sqlx.Tx` per `WithinTransaction` call and passes transaction-scoped repositories into a callback. Handlers remain reusable across concurrent goroutines because they no longer own hidden mutable transaction state; transaction-specific repositories are threaded explicitly through handler logic.

**Tech Stack:** Go, `sqlx`, PostgreSQL, Redis stream consumers, `testify`, Go standard library concurrency primitives.

---

## File map

### Executor controller

- `executor-controller/adapters/postgres/outbox_repository.go`
  - Generalize the repository executor from `*sqlx.DB` to a small interface so the same implementation can run on `*sqlx.DB` or `*sqlx.Tx`.
- `executor-controller/service/uow/uow.go`
  - Replace `UnitOfWork` mutable state with `Transaction`, `TransactionRunner`, `PostgresTransaction`, and `PostgresTransactionRunner`.
- `executor-controller/service/uow/uow_test.go`
  - New tests for transaction-runner commit and rollback behavior.
- `executor-controller/service/handlers/deploy_handler.go`
  - Use `WithinTransaction`.
- `executor-controller/service/handlers/deploy_handler_test.go`
  - Update fakes to the new API and add concurrent-handler regression coverage.
- `executor-controller/service/messagebus/messagebus.go`
  - Remove the now-unused UOW field and constructor argument.
- `executor-controller/main.go`
  - Wire `NewPostgresTransactionRunner`.
- `executor-controller/go.mod`
- `executor-controller/go.sum`
  - Add `github.com/DATA-DOG/go-sqlmock` for focused transaction-runner tests.

### K8s controller

- `k8s-controller/service/uow/uow.go`
  - Replace the mutable UOW API with explicit transaction runner types.
- `k8s-controller/service/uow/uow_test.go`
  - New tests for transaction-runner commit and rollback behavior.
- `k8s-controller/service/handlers/check_status_handler.go`
  - Use `WithinTransaction` and pass transaction-scoped repositories into helper methods.
- `k8s-controller/service/handlers/check_status_handler_test.go`
  - Update fakes and add concurrent-handler regression coverage.
- `k8s-controller/test/fakes/unit_of_work.go`
  - Adapt shared test fake to the new transaction-runner API.
- `k8s-controller/main.go`
  - Wire `NewPostgresTransactionRunner`.
- `k8s-controller/go.mod`
- `k8s-controller/go.sum`
  - Add `github.com/DATA-DOG/go-sqlmock` for focused transaction-runner tests.

### Documentation and generated repo metadata

- `docs/arch/services/executor-controller.md`
- `docs/arch/services/k8s-controller.md`
  - Reconcile service docs with explicit transaction boundaries and safe concurrent handler reuse.
- `graphify-out/*` generated outputs if the rebuild command materializes them.

## Task 1: Make executor transactions explicit and truly tx-scoped

**Files:**
- Modify: `executor-controller/adapters/postgres/outbox_repository.go`
- Modify: `executor-controller/service/uow/uow.go`
- Create: `executor-controller/service/uow/uow_test.go`
- Modify: `executor-controller/service/handlers/deploy_handler_test.go`
- Modify: `executor-controller/service/handlers/deploy_handler.go`
- Modify: `executor-controller/service/messagebus/messagebus.go`
- Modify: `executor-controller/main.go`
- Modify: `executor-controller/go.mod`
- Modify: `executor-controller/go.sum`

- [ ] **Step 1: Write a failing concurrent-handler test**

Add this test to `executor-controller/service/handlers/deploy_handler_test.go`:

```go
func TestDeployHandler_Handle_AllowsConcurrentCalls(t *testing.T) {
	repo := &threadSafeStubOutboxRepo{}
	runner := &stubTransactionRunner{
		tx: &stubTransaction{outboxRepo: repo},
	}
	h := handlers.NewDeployHandler(runner, slog.Default())

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, table := range []string{"orders", "customers"} {
		wg.Add(1)
		go func(table string) {
			defer wg.Done()
			errs <- h.Handle(context.Background(), command.DeployJob{
				TaskID:       uuid.New(),
				ScheduleID:   uuid.New(),
				ScheduleName: "daily",
				ServiceName:  "dbt",
				SchemaName:   "public",
				TableName:    table,
				JobName:      "job-" + table,
				NodeType:     pkg_model.NodeTypeDbtModel,
			})
		}(table)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, repo.entriesSnapshot(), 2)
}
```

Add the supporting fake types in the same file:

```go
type threadSafeStubOutboxRepo struct {
	mu      sync.Mutex
	entries []*model.DeploymentOutboxEntry
}

func (r *threadSafeStubOutboxRepo) Create(_ context.Context, entry *model.DeploymentOutboxEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
	return nil
}

func (r *threadSafeStubOutboxRepo) entriesSnapshot() []*model.DeploymentOutboxEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*model.DeploymentOutboxEntry(nil), r.entries...)
}
```

The fake transaction-runner types referenced here are introduced in Step 3.

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
rtk go test ./executor-controller/service/handlers -run TestDeployHandler_Handle_AllowsConcurrentCalls -count=1
```

Expected: build failure because `stubTransactionRunner` / `stubTransaction` do not exist yet and `NewDeployHandler` still expects the old UOW type.

- [ ] **Step 3: Write transaction-runner tests**

Add the testing dependency:

```bash
cd executor-controller && rtk go get github.com/DATA-DOG/go-sqlmock@v1.5.2
```

Expected: `executor-controller/go.mod` and `executor-controller/go.sum` gain the `go-sqlmock` dependency.

Create `executor-controller/service/uow/uow_test.go` with tests that exercise the desired API:

```go
func TestPostgresTransactionRunner_WithinTransaction_CommitsOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectCommit()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	err = runner.WithinTransaction(context.Background(), func(tx Transaction) error {
		require.NotNil(t, tx.OutboxRepo())
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresTransactionRunner_WithinTransaction_RollsBackOnCallbackError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectRollback()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	err = runner.WithinTransaction(context.Background(), func(Transaction) error {
		return errors.New("boom")
	})
	require.EqualError(t, err, "boom")
	require.NoError(t, mock.ExpectationsWereMet())
}
```

Use:

```go
import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)
```

- [ ] **Step 4: Run the UOW tests and verify they fail**

Run:

```bash
rtk go test ./executor-controller/service/uow -count=1
```

Expected: build failure because `NewPostgresTransactionRunner`, `TransactionRunner`, and `Transaction` do not exist yet.

- [ ] **Step 5: Implement tx-capable executor repositories**

Refactor `executor-controller/adapters/postgres/outbox_repository.go` so the repository accepts either DB or transaction executors:

```go
type Executor interface {
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type outboxRepository struct {
	executor Executor
	logger   *slog.Logger
}

func NewOutboxRepository(db *sqlx.DB, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{executor: db, logger: logger}
}

func NewOutboxRepositoryWithTx(tx *sqlx.Tx, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{executor: tx, logger: logger}
}
```

Replace existing `r.db.*` calls with `r.executor.*`.

- [ ] **Step 6: Implement the explicit executor transaction runner**

Replace `executor-controller/service/uow/uow.go` with:

```go
type Transaction interface {
	OutboxRepo() postgres.OutboxRepository
}

type TransactionRunner interface {
	WithinTransaction(ctx context.Context, fn func(Transaction) error) error
}

type PostgresTransactionRunner struct {
	db     *sqlx.DB
	logger *slog.Logger
}

type postgresTransaction struct {
	tx     *sqlx.Tx
	logger *slog.Logger
}

func NewPostgresTransactionRunner(db *sqlx.DB, logger *slog.Logger) TransactionRunner {
	return &PostgresTransactionRunner{db: db, logger: logger}
}

func (r *PostgresTransactionRunner) WithinTransaction(ctx context.Context, fn func(Transaction) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		r.logger.Error("Failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	scope := &postgresTransaction{tx: tx, logger: r.logger}
	if err := fn(scope); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			r.logger.Error("Failed to rollback transaction", "error", rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		r.logger.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (tx *postgresTransaction) OutboxRepo() postgres.OutboxRepository {
	return postgres.NewOutboxRepositoryWithTx(tx.tx, tx.logger)
}
```

- [ ] **Step 7: Update deploy-handler tests to the new API**

Replace the old fake UOW with:

```go
type stubTransaction struct {
	outboxRepo postgres.OutboxRepository
}

func (tx *stubTransaction) OutboxRepo() postgres.OutboxRepository { return tx.outboxRepo }

type stubTransactionRunner struct {
	tx uow.Transaction
}

func (r *stubTransactionRunner) WithinTransaction(_ context.Context, fn func(uow.Transaction) error) error {
	return fn(r.tx)
}
```

Update existing tests to construct `handlers.NewDeployHandler(&stubTransactionRunner{tx: &stubTransaction{outboxRepo: repo}}, logger)`.

- [ ] **Step 8: Update `DeployHandler` and wiring**

Change the handler field and constructor to use `uow.TransactionRunner`, and wrap the outbox write:

```go
type DeployHandler struct {
	txRunner uow.TransactionRunner
	logger   *slog.Logger
}

func NewDeployHandler(txRunner uow.TransactionRunner, logger *slog.Logger) *DeployHandler {
	return &DeployHandler{txRunner: txRunner, logger: logger}
}

if err := h.txRunner.WithinTransaction(ctx, func(tx uow.Transaction) error {
	return tx.OutboxRepo().Create(ctx, entry)
}); err != nil {
	return fmt.Errorf("failed to create outbox entry: %w", err)
}
```

In `executor-controller/service/messagebus/messagebus.go`, remove the dead UOW dependency:

```go
type MessageBus struct {
	commandHandlers map[string]CommandHandler
	eventHandlers   map[string][]EventHandler
	logger          *slog.Logger
}

func NewMessageBus(
	commandHandlers map[string]CommandHandler,
	eventHandlers map[string][]EventHandler,
	logger *slog.Logger,
) *MessageBus {
	return &MessageBus{
		commandHandlers: commandHandlers,
		eventHandlers:   eventHandlers,
		logger:          logger,
	}
}
```

In `executor-controller/main.go`, replace:

```go
unitOfWork := uow.NewPostgresUnitOfWork(pgDB, logger)
deployHandler := handlers.NewDeployHandler(unitOfWork, logger)
messageBus := messagebus.NewMessageBus(unitOfWork, commandHandlers, map[string][]messagebus.EventHandler{}, logger)
```

with:

```go
txRunner := uow.NewPostgresTransactionRunner(pgDB, logger)
deployHandler := handlers.NewDeployHandler(txRunner, logger)
messageBus := messagebus.NewMessageBus(commandHandlers, map[string][]messagebus.EventHandler{}, logger)
```

- [ ] **Step 9: Run executor tests**

Run:

```bash
rtk go test ./executor-controller/... -count=1
```

Expected: all executor-controller tests pass, including the new concurrent handler regression.

- [ ] **Step 10: Commit**

```bash
rtk git add executor-controller
rtk git commit -m "refactor(executor): make transactions explicit"
```

## Task 2: Make k8s transactions explicit and safe under concurrent handlers

**Files:**
- Modify: `k8s-controller/service/uow/uow.go`
- Create: `k8s-controller/service/uow/uow_test.go`
- Modify: `k8s-controller/service/handlers/check_status_handler_test.go`
- Modify: `k8s-controller/service/handlers/check_status_handler.go`
- Modify: `k8s-controller/test/fakes/unit_of_work.go`
- Modify: `k8s-controller/main.go`
- Modify: `k8s-controller/go.mod`
- Modify: `k8s-controller/go.sum`

- [ ] **Step 1: Write a failing concurrent-handler test**

Add this test to `k8s-controller/service/handlers/check_status_handler_test.go`:

```go
func TestCheckStatusHandler_Handle_AllowsConcurrentCalls(t *testing.T) {
	repo := &threadSafeFakeOutboxRepo{}
	runner := &fakeTransactionRunner{
		newTx: func() uow.Transaction {
			return &fakeTransaction{
				outboxRepo:          repo,
				processedEventsRepo: &fakeProcessedEventsRepo{},
			}
		},
	}
	handler := newHandler(&fakeK8sClient{status: failedResult()}, runner, noopCancelledRepo(), 3)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- handler.Handle(context.Background(), command.CheckJobStatus{
				TaskID:     uuid.New(),
				ScheduleID: uuid.New(),
				JobName:    "job-abc",
				RetryCount: 0,
				MaxRetries: 3,
			})
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, repo.entriesSnapshot(), 2)
}
```

Add a mutex-backed fake repository:

```go
type threadSafeFakeOutboxRepo struct {
	mu      sync.Mutex
	entries []*model.K8sStatusOutboxEntry
}

func (r *threadSafeFakeOutboxRepo) Create(_ context.Context, e *model.K8sStatusOutboxEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, e)
	return nil
}

func (r *threadSafeFakeOutboxRepo) entriesSnapshot() []*model.K8sStatusOutboxEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*model.K8sStatusOutboxEntry(nil), r.entries...)
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
rtk go test ./k8s-controller/service/handlers -run TestCheckStatusHandler_Handle_AllowsConcurrentCalls -count=1
```

Expected: build failure because `fakeTransactionRunner`, `fakeTransaction`, and the new `newHandler` signature do not exist yet.

- [ ] **Step 3: Write transaction-runner tests**

Add the testing dependency:

```bash
cd k8s-controller && rtk go get github.com/DATA-DOG/go-sqlmock@v1.5.2
```

Expected: `k8s-controller/go.mod` and `k8s-controller/go.sum` gain the `go-sqlmock` dependency.

Create `k8s-controller/service/uow/uow_test.go`:

```go
func TestPostgresTransactionRunner_WithinTransaction_CommitsOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectCommit()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	err = runner.WithinTransaction(context.Background(), func(tx Transaction) error {
		require.NotNil(t, tx.OutboxRepo())
		require.NotNil(t, tx.ProcessedEventsRepo())
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresTransactionRunner_WithinTransaction_RollsBackOnCallbackError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	mock.ExpectBegin()
	mock.ExpectRollback()

	runner := NewPostgresTransactionRunner(sqlxDB, slog.Default())
	err = runner.WithinTransaction(context.Background(), func(Transaction) error {
		return errors.New("boom")
	})
	require.EqualError(t, err, "boom")
	require.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 4: Run the UOW tests and verify they fail**

Run:

```bash
rtk go test ./k8s-controller/service/uow -count=1
```

Expected: build failure because the explicit transaction types are not implemented yet.

- [ ] **Step 5: Implement the explicit k8s transaction runner**

Replace `k8s-controller/service/uow/uow.go` with:

```go
type Transaction interface {
	OutboxRepo() postgres.OutboxRepository
	ProcessedEventsRepo() postgres.ProcessedEventsRepository
}

type TransactionRunner interface {
	WithinTransaction(ctx context.Context, fn func(Transaction) error) error
}

type PostgresTransactionRunner struct {
	db     *sqlx.DB
	logger *slog.Logger
}

type postgresTransaction struct {
	tx     *sqlx.Tx
	logger *slog.Logger
}

func NewPostgresTransactionRunner(db *sqlx.DB, logger *slog.Logger) TransactionRunner {
	return &PostgresTransactionRunner{db: db, logger: logger}
}

func (r *PostgresTransactionRunner) WithinTransaction(ctx context.Context, fn func(Transaction) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		r.logger.Error("Failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	scope := &postgresTransaction{tx: tx, logger: r.logger}
	if err := fn(scope); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			r.logger.Error("Failed to rollback transaction", "error", rollbackErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		r.logger.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (tx *postgresTransaction) OutboxRepo() postgres.OutboxRepository {
	return postgres.NewOutboxRepositoryWithTx(tx.tx, tx.logger)
}

func (tx *postgresTransaction) ProcessedEventsRepo() postgres.ProcessedEventsRepository {
	return postgres.NewProcessedEventsRepositoryWithTx(tx.tx, tx.logger)
}
```

- [ ] **Step 6: Update k8s handler fakes**

Replace the old fake UOW in `check_status_handler_test.go` with:

```go
type fakeTransaction struct {
	outboxRepo          postgres.OutboxRepository
	processedEventsRepo postgres.ProcessedEventsRepository
}

func (tx *fakeTransaction) OutboxRepo() postgres.OutboxRepository { return tx.outboxRepo }
func (tx *fakeTransaction) ProcessedEventsRepo() postgres.ProcessedEventsRepository {
	return tx.processedEventsRepo
}

type fakeTransactionRunner struct {
	newTx func() uow.Transaction
}

func (r *fakeTransactionRunner) WithinTransaction(_ context.Context, fn func(uow.Transaction) error) error {
	return fn(r.newTx())
}
```

Update `newHandler` to accept `uow.TransactionRunner`.

Adapt `k8s-controller/test/fakes/unit_of_work.go` to expose the same runner/transaction split:

```go
type FakeTransactionRunner struct {
	Transaction uow.Transaction
	WithinTransactionFunc func(context.Context, func(uow.Transaction) error) error
}

func (f *FakeTransactionRunner) WithinTransaction(ctx context.Context, fn func(uow.Transaction) error) error {
	if f.WithinTransactionFunc != nil {
		return f.WithinTransactionFunc(ctx, fn)
	}
	return fn(f.Transaction)
}
```

- [ ] **Step 7: Refactor `CheckStatusHandler`**

Change the handler field and constructor to accept `uow.TransactionRunner`.

Move the SQL portion of `Handle()` into:

```go
return h.txRunner.WithinTransaction(ctx, func(tx uow.Transaction) error {
	if cmd.OutboxEntryID != nil {
		duplicate, err := tx.ProcessedEventsRepo().TryMarkProcessed(ctx, *cmd.OutboxEntryID)
		if err != nil {
			return fmt.Errorf("dedup check failed: %w", err)
		}
		if duplicate {
			h.logger.Info("Duplicate message — skipping",
				"outbox_entry_id", cmd.OutboxEntryID,
				"task_id", cmd.TaskID,
			)
			return nil
		}
	}

	cancelled, err := h.cancelledSchedules.Exists(ctx, cmd.ScheduleID)
	if err != nil {
		return fmt.Errorf("cancelled schedules check: %w", err)
	}
	if cancelled {
		h.logger.Info("Schedule cancelled — absorbing job result",
			"schedule_id", cmd.ScheduleID, "job_name", cmd.JobName, "status", result.Status)
		return nil
	}

	switch result.Status {
	case model.JobStatusSucceeded:
		return h.handleSucceeded(ctx, tx, cmd, result)
	case model.JobStatusFailed:
		if retryCount >= maxRetries {
			return h.handleFailedPermanent(ctx, tx, cmd, result, retryCount)
		}
		return h.handleFailedWithRetry(ctx, tx, cmd, result, retryCount, maxRetries)
	case model.JobStatusRunning:
		return h.handleRunning(ctx, tx, cmd)
	default:
		return h.handleUnknown(ctx, tx, cmd, result)
	}
})
```

Update helper signatures from `handleX(ctx, cmd, ...)` to `handleX(ctx, tx, cmd, ...)`, and replace `h.uow.OutboxRepo()` with `tx.OutboxRepo()`.

- [ ] **Step 8: Update k8s main wiring**

In `k8s-controller/main.go`, replace `uow.NewPostgresUnitOfWork(pgDB, logger)` with `uow.NewPostgresTransactionRunner(pgDB, logger)` and pass that runner into `NewCheckStatusHandler`.

- [ ] **Step 9: Run k8s tests**

Run:

```bash
rtk go test ./k8s-controller/... -count=1
```

Expected: all k8s-controller tests pass, including the new concurrent handler regression.

- [ ] **Step 10: Commit**

```bash
rtk git add k8s-controller
rtk git commit -m "refactor(k8s): make transactions explicit"
```

## Task 3: Reconcile docs and verify the full change

**Files:**
- Modify: `docs/arch/services/executor-controller.md`
- Modify: `docs/arch/services/k8s-controller.md`
- Regenerate if produced: `graphify-out/*`

- [ ] **Step 1: Update architecture docs**

Add the transaction-boundary behavior to the reliability sections.

For `docs/arch/services/executor-controller.md`, add:

```markdown
- **Explicit transaction boundary**: each inbound message runs its outbox insert inside a fresh transaction created by a transaction runner; parallel stream consumers may share one handler safely because transaction state is not stored on the handler or runner.
```

For `docs/arch/services/k8s-controller.md`, add:

```markdown
- **Explicit transaction boundary**: each status message gets its own transaction-scoped repositories for dedup and outbox writes; parallel `node.deployed:v1` and `check.k8s:v1` consumers can reuse one handler without sharing mutable transaction state.
```

- [ ] **Step 2: Run formatting**

Run:

```bash
rtk gofmt -w executor-controller k8s-controller
```

Expected: no output; files are normalized in place.

- [ ] **Step 3: Rebuild graph metadata**

Run:

```bash
rtk python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

Expected: graph metadata is regenerated if graphify is available in this worktree.

- [ ] **Step 4: Run targeted verification**

Run:

```bash
rtk go test ./executor-controller/... ./k8s-controller/... -count=1
```

Expected: all targeted tests pass.

- [ ] **Step 5: Run repository status and inspect the diff**

Run:

```bash
rtk git status --short
rtk git diff --stat
```

Expected: only the intended executor, k8s, docs, and generated graph files are modified.

- [ ] **Step 6: Commit**

If `graphify-out/` exists after the rebuild, include it in the commit; otherwise commit only the docs:

```bash
rtk git add docs/arch/services/executor-controller.md docs/arch/services/k8s-controller.md
if [ -d graphify-out ]; then rtk git add graphify-out; fi
rtk git commit -m "docs: document explicit transaction boundaries"
```

## Self-review

- **Spec coverage:** Task 1 implements explicit executor transactions and tx-bound executor repositories; Task 2 implements explicit k8s transactions and concurrent handler safety; Task 3 updates required architecture docs and performs verification.
- **Placeholder scan:** No `TBD`, `TODO`, “similar to,” or omitted implementation steps remain.
- **Type consistency:** The plan consistently uses `Transaction`, `TransactionRunner`, `PostgresTransactionRunner`, `WithinTransaction`, and transaction-scoped repo accessors across both services.
