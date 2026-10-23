//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	executorredis "github.com/carolsimone/continuo/execution-controller/adapters/redis"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// committedDeletes is a JobTerminator that reads, on another connection, the
// status of the row of each Job it deletes: what the fence committed.
type committedDeletes struct {
	t        *testing.T
	db       *sqlx.DB
	statuses map[string]string
}

func (c *committedDeletes) DeleteJob(_ context.Context, name string) error {
	var status string
	require.NoError(c.t, c.db.Get(&status, `SELECT status FROM deployments WHERE job_name = $1`, name))
	c.statuses[name] = status
	return nil
}

type noStopMetrics struct{}

func (noStopMetrics) AttemptStopped()  {}
func (noStopMetrics) JobReaped(string) {}

// countedStops is a ports.StopMetrics that counts the attempts reported
// stopped.
type countedStops struct{ stopped int }

func (c *countedStops) AttemptStopped() { c.stopped++ }
func (*countedStops) JobReaped(string)  {}

// runCancelledMessage is one run.cancelled:v1 entry of runID, as a new outbox
// row of state.
func runCancelledMessage(t *testing.T, id string, runID uuid.UUID, attempts ...pkgevents.CancelledAttempt) goredis.XMessage {
	t.Helper()
	outboxID := uuid.New()
	fields, err := pkgevents.RunCancelledFields(outboxID.String(), "default", "state", nowUTC(),
		pkgevents.RunCancelled{RunID: runID.String(), CancelledBy: "alice", Reason: "wrong window", Attempts: attempts})
	require.NoError(t, err)
	values := map[string]any{"outbox_entry_id": outboxID.String()}
	for k, v := range fields {
		values[k] = v
	}
	return goredis.XMessage{ID: id, Values: values}
}

// The binding deletes the Jobs of the rows the fence moved to terminating
// only after the fence committed, so a Job is never deleted for a stop that
// rolled back. A named attempt it never received is stored stopped under the
// message's dedup row. A duplicate delivery commits nothing new and deletes
// nothing.
func TestRunCancelledBinding_DeletesTheJobsAfterCommit(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	logger := testDispatchLogger()
	runID := uuid.New()
	cmd := command.DeployTask{TaskID: uuid.NewString(), ScheduleID: runID.String(), ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "orders", JobName: "dbt-public-orders-" + runID.String()[:8],
		NodeType: "dbt-model", ImageTag: "sha-abc", Operation: "run", TimeoutSeconds: 3600}
	repo := postgres.NewDeploymentsRepository(db, logger)
	d := model.NewDeployment(cmd, nil, time.Now().Add(-time.Minute))
	_, err := repo.AddAttempt(ctx, d)
	require.NoError(t, err)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(time.Now().Add(-30*time.Second)))
	require.NoError(t, repo.Save(ctx, d))
	term := &committedDeletes{t: t, db: db, statuses: map[string]string{}}
	newUoW := func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(db, logger) }
	bind := executorredis.NewRunCancelledBinding(newUoW,
		handlers.NewRunCancelledHandler(jobsHolding{}, jobsHolding{}, term, noStopMetrics{}, logger), logger)
	unseen := pkgevents.CancelledAttempt{TaskID: uuid.NewString(), Attempt: 0, JobName: "dbt-public-items-" + runID.String()[:8]}
	msg := runCancelledMessage(t, msgID(1), runID, unseen)

	require.NoError(t, bind(ctx, msg))
	require.NoError(t, bind(ctx, msg))

	assert.Equal(t, map[string]string{cmd.JobName: string(model.StatusTerminating)}, term.statuses,
		"the Job is deleted once, after the row's move to terminating committed")
	assert.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM cancelled_runs WHERE run_id = $1`, runID))
	assert.Equal(t, "completed", dedupState(t, db, streams.RunCancelledV1))
	assert.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM deployments d JOIN message_processing m
		ON m.id = d.message_processing_id WHERE d.task_id = $1 AND d.outcome = 'stopped'`, unseen.TaskID))
}

// A cancel that cannot be read is permanent: it is dead-lettered, and nothing
// is fenced.
func TestRunCancelledBinding_AnUnreadableCancelIsPermanent(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	logger := testDispatchLogger()
	newUoW := func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(db, logger) }
	bind := executorredis.NewRunCancelledBinding(newUoW,
		handlers.NewRunCancelledHandler(jobsHolding{}, jobsHolding{}, &committedDeletes{}, noStopMetrics{}, logger), logger)
	msg := runCancelledMessage(t, msgID(1), uuid.New())
	msg.Values["payload"] = `{"run_id":"not-a-uuid"}`

	err := bind(context.Background(), msg)

	require.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Zero(t, countRows(t, db, `SELECT count(*) FROM cancelled_runs`))
}
