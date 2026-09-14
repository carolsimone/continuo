package redis

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	"github.com/carolsimone/continuo/execution-controller/test/fakes"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/google/uuid"
)

// noopCancelledSchedulesRepo is a minimal repository.CancelledSchedulesRepository
// stand-in for binding tests that never exercise schedule cancellation.
type noopCancelledSchedulesRepo struct{}

func (noopCancelledSchedulesRepo) Insert(context.Context, uuid.UUID) error { return nil }
func (noopCancelledSchedulesRepo) Exists(context.Context, uuid.UUID) (bool, error) {
	return false, nil
}
func (noopCancelledSchedulesRepo) DeleteExpired(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

var _ repository.CancelledSchedulesRepository = (*noopCancelledSchedulesRepo)(nil)

// TestNodeDeployedBinding_DuplicateSkipsHandler proves a duplicate message is
// ACKed (binding returns nil) without invoking the K8s client: dedup short-
// circuits the handler before any business work runs. The duplicate path still
// commits the open transaction (it does not roll back).
func TestNodeDeployedBinding_DuplicateSkipsHandler(t *testing.T) {
	k8s := fakes.NewFakeK8sClient()
	cfg := &handlers.JobStatusConfig{K8sNamespace: "default", DefaultTaskMaxRetries: 3, ErrorMessageMaxLen: 4096, LogTailLines: 50}
	handler := handlers.NewJobStatusHandler(k8s, nil, cfg, noopCancelledSchedulesRepo{}, slog.Default())

	u := &fakes.FakeUnitOfWork{
		MessageProcessing: &fakes.FakeMessageProcessingRepository{
			InsertIfNotExistsFunc: func(context.Context, *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
				return uuid.New(), false, nil // not inserted → duplicate
			},
		},
	}
	binding := NewNodeDeployedBinding(func() uow.UnitOfWork { return u }, handler, slog.Default())

	err := binding(context.Background(), payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-x",
	}))
	if err != nil {
		t.Fatalf("binding returned error on duplicate: %v", err)
	}
	if k8s.CallCount != 0 {
		t.Fatalf("expected handler NOT to call K8s on duplicate, got %d calls", k8s.CallCount)
	}
	if u.CommitCalled != 1 || u.RollbackCalled != 0 {
		t.Fatalf("duplicate path should commit once and not rollback; got commits=%d rollbacks=%d", u.CommitCalled, u.RollbackCalled)
	}
}
