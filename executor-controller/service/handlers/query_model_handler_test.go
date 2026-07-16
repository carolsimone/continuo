package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubCancelledRepo returns Exists()=true for any ID in ids.
type stubCancelledRepo struct {
	ids map[uuid.UUID]bool
}

func (r *stubCancelledRepo) Insert(_ context.Context, _ uuid.UUID) error { return nil }
func (r *stubCancelledRepo) Exists(_ context.Context, id uuid.UUID) (bool, error) {
	return r.ids[id], nil
}
func (r *stubCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func newFakeUoW(depl repository.DeploymentRepository, cancelled repository.CancelledSchedulesRepository) *uow.FakeUnitOfWork {
	return &uow.FakeUnitOfWork{Deployments: depl, Cancelled: cancelled, Outbox: &stubOutboxRepo{}}
}

// jobsPolicy is the shipped default: every record takes the Kubernetes Job path.
func jobsPolicy() routing.Policy {
	return routing.NewPolicy(model.ExecutionModeJobs, nil)
}

// workersPolicy enables a worker canary for the "finance" service only.
func workersPolicy() routing.Policy {
	return routing.NewPolicy(model.ExecutionModeJobs,
		map[string]model.ExecutionMode{"finance": model.ExecutionModeWorkers})
}

func completeRef() pkg_model.RuntimeManifestRef {
	return pkg_model.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://artifacts/finance/partial_parse.msgpack",
		RuntimeManifestSHA256:             "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
	}
}

// migratedEvent is a record for a node that carries its dbt identity and a
// complete runtime manifest pin — everything a worker needs to run it.
func migratedEvent() events.QueryModel {
	return events.QueryModel{
		TaskID: uuid.New(), ScheduleID: uuid.New(), ScheduleName: "daily",
		ServiceName: "finance", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: pkg_model.NodeTypeDbtModel,
		ImageTag: "sha-abc", DBTUniqueID: "model.finance.orders",
		RuntimeManifestRef: completeRef(),
	}
}

// TestQueryModelHandler_JobsDefaultRoutesEveryRecordToJobs pins that the shipped
// configuration is inert: even a fully migrated node keeps taking a Job.
func TestQueryModelHandler_JobsDefaultRoutesEveryRecordToJobs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	h := handlers.NewQueryModelHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, migratedEvent(), uuid.New()))
	require.Len(t, depl.added, 1)

	dep := depl.added[0]
	assert.Equal(t, model.ExecutionModeJobs, dep.ExecutionMode())
	assert.Empty(t, dep.PoolKey())
	assert.Equal(t, model.StatusPending, dep.Status())
}

// TestQueryModelHandler_HistoricalMessageRoutesToJobs pins compatibility: a
// message carrying neither a dbt identity nor a pin is not an error, it is old.
func TestQueryModelHandler_HistoricalMessageRoutesToJobs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := migratedEvent()
	evt.DBTUniqueID = ""
	evt.RuntimeManifestRef = pkg_model.RuntimeManifestRef{}

	h := handlers.NewQueryModelHandler(workersPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)
	assert.Equal(t, model.ExecutionModeJobs, depl.added[0].ExecutionMode())
}

// TestQueryModelHandler_PromoteSeedRoutesToJobs pins that a seed promoted into
// production keeps its own Job even for a service running a worker canary.
func TestQueryModelHandler_PromoteSeedRoutesToJobs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := migratedEvent()
	evt.Mode = pkgevents.ModePromoteSeed
	evt.DBTUniqueID = "seed.finance.currency"

	h := handlers.NewQueryModelHandler(workersPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)
	assert.Equal(t, model.ExecutionModeJobs, depl.added[0].ExecutionMode())
}

// TestQueryModelHandler_WorkerCanaryEnqueuesAPoolTask pins the worker path: a
// migrated node for a canary service waits to be claimed from the pool that
// serves its service, image and runtime manifest.
func TestQueryModelHandler_WorkerCanaryEnqueuesAPoolTask(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := migratedEvent()
	msgProcID := uuid.New()

	h := handlers.NewQueryModelHandler(workersPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))
	require.Len(t, depl.added, 1)

	dep := depl.added[0]
	assert.Equal(t, model.ExecutionModeWorkers, dep.ExecutionMode())
	assert.Equal(t, model.StatusPending, dep.Status())
	assert.Equal(t,
		pkg_model.WorkerPoolKey("finance", "sha-abc", evt.RuntimeManifestSHA256),
		dep.PoolKey())
	require.NotNil(t, dep.MessageProcessingID())
	assert.Equal(t, msgProcID, *dep.MessageProcessingID())

	cmd := dep.Command()
	assert.Equal(t, "model.finance.orders", cmd.DBTUniqueID,
		"a worker invokes one exact dbt node")
	assert.Equal(t, evt.RuntimeManifestRef, cmd.RuntimeManifestRef)
	assert.Nil(t, dep.ResolvedArgv(), "argv is pinned when a worker claims the task, not now")
	assert.Empty(t, dep.ExecutionPath())
}

// TestQueryModelHandler_ServiceOutsideTheCanaryStaysOnJobs pins that the
// override map, not the record, decides which services move.
func TestQueryModelHandler_ServiceOutsideTheCanaryStaysOnJobs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := migratedEvent()
	evt.ServiceName = "sales"

	h := handlers.NewQueryModelHandler(workersPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)
	assert.Equal(t, model.ExecutionModeJobs, depl.added[0].ExecutionMode())
}

// TestQueryModelHandler_IncompleteRefOnACanaryServiceIsRejected pins the
// explicit-failure branch. A migrated node routed to workers without a usable
// runtime manifest never falls back to a full-project parse: it is recorded as
// permanently failed and announced as FAILED, so the run advances instead of
// hanging on a node that will never report.
func TestQueryModelHandler_IncompleteRefOnACanaryServiceIsRejected(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	outboxRepo := &stubOutboxRepo{}
	u := &uow.FakeUnitOfWork{
		Deployments: depl,
		Cancelled:   &stubCancelledRepo{ids: map[uuid.UUID]bool{}},
		Outbox:      outboxRepo,
	}

	evt := migratedEvent()
	evt.RuntimeManifestSHA256 = ""

	h := handlers.NewQueryModelHandler(workersPolicy(), logger)
	// nil so the binding commits the audit row and the announcements, then ACKs:
	// a permanent defect must not be redelivered forever.
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.added, 1, "the rejection is audited next to the work it replaced")
	dep := depl.added[0]
	assert.Equal(t, model.StatusFailed, dep.Status())
	require.NotNil(t, dep.ErrorMessage())
	// The reason keeps the constant prefix and appends the specific defect, so an
	// operator reading error_message can tell which check rejected the node.
	assert.Contains(t, *dep.ErrorMessage(), "runtime manifest reference is incomplete")
	assert.Contains(t, *dep.ErrorMessage(), evt.DBTUniqueID,
		"the failing check's own message is recorded, not just the constant")

	streamNames := make([]string, 0, len(outboxRepo.entries))
	for _, e := range outboxRepo.entries {
		streamNames = append(streamNames, e.StreamName)
	}
	assert.ElementsMatch(t,
		[]string{streams.TaskStatusUpdatedV1, streams.NodeUpdatedV1}, streamNames)
}

func TestQueryModelHandler_EnqueuesDeployment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	taskID := uuid.New()
	scheduleID := uuid.New()
	evt := events.QueryModel{
		TaskID: taskID, ScheduleID: scheduleID, ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "sha-abc",
	}

	h := handlers.NewQueryModelHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)

	dep := depl.added[0]
	assert.Equal(t, model.StatusPending, dep.Status())

	cmd := dep.Command()
	assert.Equal(t, taskID.String(), cmd.TaskID)
	assert.Equal(t, scheduleID.String(), cmd.ScheduleID)
	assert.Equal(t, "dbt-public-orders", cmd.JobName)
	assert.Equal(t, 0, cmd.TaskRetryCount)
	assert.Equal(t, 2, cmd.TaskMaxRetries, "default task max retries off the retry stream")
	assert.True(t, dep.IsDeployable())
}

func TestQueryModelHandler_DropsWhenScheduleCancelled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	scheduleID := uuid.New()
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	u := newFakeUoW(depl, cancelled)

	evt := events.QueryModel{TaskID: uuid.New(), ScheduleID: scheduleID, NodeType: pkg_model.NodeTypeDbtModel}

	h := handlers.NewQueryModelHandler(jobsPolicy(), logger)
	// Cancelled-schedule path returns nil so the binding commits and ACKs the
	// message rather than leaving it pending for endless redelivery.
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	assert.Empty(t, depl.added, "no deployment enqueued when schedule is cancelled")
}

func TestQueryModelHandler_PropagatesMsgProcIDToDeployment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	msgProcID := uuid.New()
	evt := events.QueryModel{
		TaskID: uuid.New(), ScheduleID: uuid.New(),
		OutboxEntryID: uuid.New(), NodeType: pkg_model.NodeTypeDbtModel, JobName: "j",
	}

	h := handlers.NewQueryModelHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))
	require.Len(t, depl.added, 1)

	dep := depl.added[0]
	require.NotNil(t, dep.MessageProcessingID())
	assert.Equal(t, msgProcID, *dep.MessageProcessingID())
	assert.NotEqual(t, evt.OutboxEntryID, *dep.MessageProcessingID(),
		"orchestrator's OutboxEntryID must never be used as the executor's message_processing FK")
}
