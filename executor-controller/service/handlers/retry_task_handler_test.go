// executor-controller/service/handlers/retry_task_handler_test.go
package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryTaskHandler_PropagatesRetryFields(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
			NodeType:   pkg_model.NodeTypeDbtModel,
			JobName:    "j",
		},
		TaskRetryCount: 3,
		MaxRetries:     7,
	}

	h := handlers.NewRetryTaskHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)

	cmd := depl.added[0].Command()
	assert.Equal(t, 3, cmd.TaskRetryCount)
	assert.Equal(t, 7, cmd.TaskMaxRetries)
}

func TestRetryTaskHandler_DefaultsMaxRetriesWhenZero(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{}}
	u := newFakeUoW(depl, cancelled)

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID:     uuid.New(),
			ScheduleID: uuid.New(),
			NodeType:   pkg_model.NodeTypeDbtModel,
		},
		TaskRetryCount: 1,
		MaxRetries:     0,
	}

	h := handlers.NewRetryTaskHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)

	cmd := depl.added[0].Command()
	assert.Equal(t, 2, cmd.TaskMaxRetries, "max_retries=0 on the wire falls back to service default 2")
}

// retryPendingWorkerDeployment builds a worker task parked after a retryable
// failure — the state a same-row retry re-attempts.
func retryPendingWorkerDeployment(t *testing.T, taskID uuid.UUID) *model.Deployment {
	t.Helper()
	dep := model.NewWorkerDeployment(command.DeployTask{
		TaskID: taskID.String(), ScheduleID: uuid.New().String(), ServiceName: "finance",
		TableName: "orders", JobName: "dbt-public-orders-attempt-1", NodeType: "dbt-model",
		ImageTag: "sha-abc", DBTUniqueID: "model.finance.orders", TaskRetryCount: 0,
	}, uuid.New(), "pool-key", time.Now().Add(-time.Hour))
	require.NoError(t, dep.Claim(uuid.New(), "token-sha", "worker-1", "pod-1", "uid-1",
		time.Now().Add(-time.Hour), time.Now().Add(-30*time.Minute),
		[]string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, dep.MarkRetryPending(time.Now().Add(-time.Minute), 0))
	require.Equal(t, model.StatusRetryPending, dep.Status())
	return dep
}

// TestRetryTaskHandler_WorkerRetryReusesTheSameRow pins that a worker task's
// retry re-attempts the row it already has, so its lease history and attempt
// counter stay in one place instead of fragmenting across rows.
func TestRetryTaskHandler_WorkerRetryReusesTheSameRow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	taskID := uuid.New()
	dep := retryPendingWorkerDeployment(t, taskID)
	depl := &stubDeploymentsRepo{byID: map[uuid.UUID]*model.Deployment{dep.ID(): dep}}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID: taskID, ScheduleID: uuid.New(), NodeType: pkg_model.NodeTypeDbtModel,
			JobName: "dbt-public-orders-attempt-2",
		},
		TaskRetryCount:       1,
		MaxRetries:           3,
		ExecutorDeploymentID: dep.ID(),
	}

	h := handlers.NewRetryTaskHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	assert.Empty(t, depl.added, "a same-row retry enqueues no second deployment")
	require.Len(t, depl.saved, 1)

	saved := depl.saved[0]
	assert.Equal(t, dep.ID(), saved.ID())
	assert.Equal(t, model.StatusPending, saved.Status(), "the task is due for its next claim")
	assert.Equal(t, 1, saved.Attempt(), "the attempt counter survives the requeue")

	cmd := saved.Command()
	assert.Equal(t, 1, cmd.TaskRetryCount)
	assert.Equal(t, "dbt-public-orders-attempt-2", cmd.JobName)
}

// TestRetryTaskHandler_WorkerRetryRejectsARowThatIsNotParked pins the guard: a
// retry for a row that is running, or already terminal, must not resurrect it.
func TestRetryTaskHandler_WorkerRetryRejectsARowThatIsNotParked(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	taskID := uuid.New()
	dep := model.NewWorkerDeployment(command.DeployTask{
		TaskID: taskID.String(), ScheduleID: uuid.New().String(), ServiceName: "finance",
		JobName: "j", NodeType: "dbt-model", ImageTag: "sha-abc",
	}, uuid.New(), "pool-key", time.Now())
	require.Equal(t, model.StatusPending, dep.Status())

	depl := &stubDeploymentsRepo{byID: map[uuid.UUID]*model.Deployment{dep.ID(): dep}}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID: taskID, ScheduleID: uuid.New(), NodeType: pkg_model.NodeTypeDbtModel,
		},
		ExecutorDeploymentID: dep.ID(),
	}

	h := handlers.NewRetryTaskHandler(jobsPolicy(), logger)
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent,
		"redelivering cannot change the row's status, so the message must not be retried forever")
	assert.Empty(t, depl.saved)
	assert.Empty(t, depl.added)
}

// TestRetryTaskHandler_WorkerRetryForAMissingRowIsPermanent pins that a pointer
// at a row that does not exist fails rather than silently enqueueing new work.
func TestRetryTaskHandler_WorkerRetryForAMissingRowIsPermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{byID: map[uuid.UUID]*model.Deployment{}}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID: uuid.New(), ScheduleID: uuid.New(), NodeType: pkg_model.NodeTypeDbtModel,
		},
		ExecutorDeploymentID: uuid.New(),
	}

	h := handlers.NewRetryTaskHandler(jobsPolicy(), logger)
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent,
		"a pointer at a row that does not exist can never resolve")
	assert.Empty(t, depl.added)
}

// TestRetryTaskHandler_WithoutADeploymentPointerEnqueuesAFreshRow pins the
// unchanged Jobs-path behaviour.
func TestRetryTaskHandler_WithoutADeploymentPointerEnqueuesAFreshRow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	u := newFakeUoW(depl, &stubCancelledRepo{ids: map[uuid.UUID]bool{}})

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID: uuid.New(), ScheduleID: uuid.New(), NodeType: pkg_model.NodeTypeDbtModel, JobName: "j",
		},
		TaskRetryCount: 1,
		MaxRetries:     3,
	}

	h := handlers.NewRetryTaskHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	require.Len(t, depl.added, 1)
	assert.Empty(t, depl.saved)
	assert.Equal(t, model.StatusPending, depl.added[0].Status())
}

func TestRetryTaskHandler_DropsWhenScheduleCancelled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	depl := &stubDeploymentsRepo{}
	scheduleID := uuid.New()
	cancelled := &stubCancelledRepo{ids: map[uuid.UUID]bool{scheduleID: true}}
	u := newFakeUoW(depl, cancelled)

	evt := events.RetryTask{
		QueryModel: events.QueryModel{
			TaskID:     uuid.New(),
			ScheduleID: scheduleID,
			NodeType:   pkg_model.NodeTypeDbtModel,
		},
		TaskRetryCount: 2,
		MaxRetries:     5,
	}

	h := handlers.NewRetryTaskHandler(jobsPolicy(), logger)
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))
	assert.Empty(t, depl.added)
}
