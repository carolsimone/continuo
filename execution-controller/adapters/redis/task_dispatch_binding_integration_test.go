//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	executorredis "github.com/carolsimone/continuo/execution-controller/adapters/redis"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	executortest "github.com/carolsimone/continuo/execution-controller/test"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jobsHolding is a Job cache, and the Kubernetes API behind it, holding one
// Job, labelled with a task and an attempt.
type jobsHolding struct {
	name, taskID, attempt string
}

func (jobsHolding) Synced() bool { return true }
func (j jobsHolding) Observe(name string) model.JobObservation {
	if name != j.name || j.name == "" {
		return model.JobObservation{}
	}
	return model.JobObservation{JobPresent: true, JobTaskID: j.taskID, JobAttempt: j.attempt}
}
func (j jobsHolding) ObserveLive(_ context.Context, name string) (model.JobObservation, error) {
	return j.Observe(name), nil
}

type dispatchFlow struct {
	t       *testing.T
	db      *sqlx.DB
	runID   uuid.UUID
	taskID  uuid.UUID
	jobs    jobsHolding
	seq     int
	handler func(context.Context, goredis.XMessage) error
}

func newDispatchFlow(t *testing.T, db *sqlx.DB) *dispatchFlow {
	f := &dispatchFlow{t: t, db: db, runID: uuid.New(), taskID: uuid.New()}
	f.rebind()
	return f
}

func (f *dispatchFlow) rebind() {
	logger := testDispatchLogger()
	newUoW := func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(f.db, logger) }
	f.handler = executorredis.NewTaskDispatchBinding(newUoW, handlers.NewTaskDispatchHandler(f.jobs, f.jobs, logger), logger)
}

// dispatch delivers one task.dispatch:v1 entry of attempt n, as a new outbox
// row of state, with the given known status and payload edits.
func (f *dispatchFlow) dispatch(n int32, known pkgmodel.AttemptStatus, edit func(*pkgevents.TaskDispatch)) {
	f.t.Helper()
	f.seq++
	p := pkgevents.TaskDispatch{RunID: f.runID.String(), TaskID: f.taskID.String(), Attempt: n, KnownStatus: known,
		ScheduleName: "daily", NodeKey: "public.orders", ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		NodeType: "dbt-model", Operation: "run", ImageTag: "sha-abc", JobName: jobNameOf(f.runID, n), TimeoutSeconds: 3600}
	if edit != nil {
		edit(&p)
	}
	outboxID := uuid.New()
	fields, err := pkgevents.TaskDispatchFields(outboxID.String(), "default", "state", nowUTC(), p)
	require.NoError(f.t, err)
	values := map[string]any{"outbox_entry_id": outboxID.String()}
	for k, v := range fields {
		values[k] = v
	}
	require.NoError(f.t, f.handler(context.Background(), goredis.XMessage{ID: msgID(f.seq), Values: values}))
}

func jobNameOf(runID uuid.UUID, n int32) string {
	name := "dbt-public-orders-" + runID.String()[:8]
	if n > 0 {
		name += "-r" + string(rune('0'+n))
	}
	return name
}

type attemptRow struct {
	Attempt int    `db:"attempt"`
	Status  string `db:"status"`
	Reason  string `db:"reason"`
}

func (f *dispatchFlow) rows() []attemptRow {
	f.t.Helper()
	var rows []attemptRow
	require.NoError(f.t, f.db.Select(&rows, `SELECT attempt, status, COALESCE(facts->>'failure_reason', '') AS reason
		FROM deployments WHERE task_id = $1 ORDER BY attempt`, f.taskID))
	return rows
}

// reports are the task.status.updated:v2 payloads written for the task, in order.
func (f *dispatchFlow) reports() []pkgevents.TaskStatusReport {
	f.t.Helper()
	var payloads [][]byte
	require.NoError(f.t, f.db.Select(&payloads, `SELECT payload FROM execution_outbox
		WHERE aggregate_id = $1 AND stream_name = $2 ORDER BY created_at, id`, f.taskID, streams.TaskStatusUpdatedV2))
	out := make([]pkgevents.TaskStatusReport, 0, len(payloads))
	for _, p := range payloads {
		var r pkgevents.TaskStatusReport
		require.NoError(f.t, json.Unmarshal(p, &r))
		out = append(out, r)
	}
	return out
}

// A new dispatch is accepted once: queued for its Job and reported queued; a
// repeated dispatch is answered with the same report and creates nothing.
func TestTaskDispatch_AnAttemptIsAcceptedOnceAndARepeatIsAnsweredFromIt(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	f := newDispatchFlow(t, db)

	f.dispatch(0, "dispatched", nil)
	f.dispatch(0, "dispatched", nil)
	f.dispatch(0, "queued", nil)

	assert.Equal(t, []attemptRow{{Attempt: 0, Status: "pending"}}, f.rows())
	reps := f.reports()
	require.Len(t, reps, 3, "the first answer and two repeats")
	for _, r := range reps {
		assert.Equal(t, pkgmodel.AttemptStatusQueued, r.Status)
		assert.Equal(t, int32(0), r.Attempt)
		assert.Equal(t, reps[0].QueuedAt, r.QueuedAt, "a repeat is the stored report")
	}
}

// A dispatch of an attempt older than one held creates nothing.
func TestTaskDispatch_AnOlderAttemptCreatesNothing(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	f := newDispatchFlow(t, db)
	f.dispatch(1, "dispatched", nil)

	f.dispatch(0, "dispatched", nil)

	assert.Equal(t, []attemptRow{{Attempt: 1, Status: "pending"}}, f.rows())
	assert.Len(t, f.reports(), 1)
}

// A question about an attempt state knows as acknowledged, of which nothing is
// held, is answered attempt_lost, and the answer is kept.
func TestTaskDispatch_AnAcknowledgedAttemptWithNoTraceIsLost(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	f := newDispatchFlow(t, db)

	f.dispatch(0, "running", nil)
	f.dispatch(0, "running", nil)

	assert.Equal(t, []attemptRow{{Attempt: 0, Status: "failed", Reason: "attempt_lost"}}, f.rows())
	reps := f.reports()
	require.Len(t, reps, 2)
	for _, r := range reps {
		assert.Equal(t, pkgmodel.AttemptStatusFailed, r.Status)
		assert.Equal(t, "infra", r.FailureClass)
		assert.Equal(t, "attempt_lost", r.FailureReason)
	}
}

// With no record but the attempt's Job present, the record is rebuilt, holding
// a slot, and the observer follows the Job from there.
func TestTaskDispatch_TheJobOfTheAttemptRebuildsItsRecord(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	f := newDispatchFlow(t, db)
	f.jobs = jobsHolding{name: jobNameOf(f.runID, 1), taskID: f.taskID.String(), attempt: "1"}
	f.rebind()

	f.dispatch(1, "running", nil)

	assert.Equal(t, []attemptRow{{Attempt: 1, Status: "starting"}}, f.rows())
	require.Len(t, f.reports(), 1)
	assert.Equal(t, pkgmodel.AttemptStatusQueued, f.reports()[0].Status)
}

// A dispatch that cannot run is recorded failed with why; it is never retried.
func TestTaskDispatch_ADispatchThatCannotRunIsRecordedFailed(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	f := newDispatchFlow(t, db)

	f.dispatch(0, "dispatched", func(p *pkgevents.TaskDispatch) { p.NodeType = "dbt-macro" })

	assert.Equal(t, []attemptRow{{Attempt: 0, Status: "failed", Reason: "invalid_node_type"}}, f.rows())
	require.Len(t, f.reports(), 1)
	assert.Equal(t, "invalid", f.reports()[0].FailureClass)
}

// A dispatch refused while it carries the name of another task's running Job
// is answered from its own stored report when repeated, and the Job stays its
// owner's: both lookups by the name return the running deployment.
func TestTaskDispatch_ARefusedDispatchOfARunningJobsNameLeavesTheJobToItsOwner(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	f := newDispatchFlow(t, db)
	name := jobNameOf(f.runID, 0)
	repo := postgres.NewDeploymentsRepository(db, testDispatchLogger())
	owner := model.NewDeployment(command.DeployTask{TaskID: uuid.NewString(), ScheduleID: uuid.NewString(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public", TableName: "orders", JobName: name,
		NodeType: "dbt-model", Operation: "run", ImageTag: "sha-abc", TimeoutSeconds: 3600}, nil, time.Now().Add(-time.Minute))
	require.NoError(t, repo.Add(ctx, owner))
	require.NoError(t, owner.Reserve())
	require.NoError(t, owner.MarkStarted(time.Now().Add(-time.Minute)))
	_, err := owner.MarkRunning()
	require.NoError(t, err)
	require.NoError(t, repo.Save(ctx, owner))

	undeclared := func(p *pkgevents.TaskDispatch) { p.NodeType = "dbt-macro" }
	f.dispatch(0, "dispatched", undeclared)
	f.dispatch(0, "dispatched", undeclared)

	assert.Equal(t, []attemptRow{{Attempt: 0, Status: "failed", Reason: "invalid_node_type"}}, f.rows())
	reps := f.reports()
	require.Len(t, reps, 2)
	assert.Equal(t, reps[0], reps[1], "the repeat is answered from the stored report")
	assert.Equal(t, "invalid_node_type", reps[0].FailureReason)
	assert.Equal(t, name, reps[0].JobName, "the report names the dispatch's Job")
	got, err := repo.LoadByJobName(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, owner.ID(), got.ID())
	got, err = postgres.NewObservedDeployments(db, testDispatchLogger()).GetByJobName(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, owner.ID(), got.ID())
}

// Dispatches of a cancelled run are dropped.
func TestTaskDispatch_ACancelledRunsDispatchIsDropped(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	f := newDispatchFlow(t, db)
	_, err := db.Exec(`INSERT INTO cancelled_runs (run_id) VALUES ($1)`, f.runID)
	require.NoError(t, err)

	f.dispatch(0, "dispatched", nil)

	assert.Empty(t, f.rows())
	assert.Empty(t, f.reports())
}

func testDispatchLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func msgID(seq int) string { return fmt.Sprintf("%d-0", 1700000000000+seq) }

func nowUTC() time.Time { return time.Now().UTC() }

// setupPostgres connects to the externally migrated, empty executor test database.
func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	return executortest.SetupPostgres(t)
}

// countRows executes a COUNT(*) query and returns the result.
func countRows(t *testing.T, db *sqlx.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), query, args...).Scan(&n))
	return n
}

// dedupState returns the state of the single message_processing row a binding
// wrote under streamName.
func dedupState(t *testing.T, db *sqlx.DB, streamName string) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT state FROM message_processing WHERE stream_name = $1`, streamName).Scan(&state))
	return state
}
