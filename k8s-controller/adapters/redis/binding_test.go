package redis

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func TestCheckAfterElapsed(t *testing.T) {
	now := time.Unix(1000, 0)

	if !checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{}}, now) {
		t.Fatal("missing check_after should be treated as ready")
	}
	if !checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{"check_after": "900"}}, now) {
		t.Fatal("past check_after should be ready")
	}
	if checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{"check_after": "1100"}}, now) {
		t.Fatal("future check_after should NOT be ready")
	}
	if !checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{"check_after": "bogus"}}, now) {
		t.Fatal("unparseable check_after should be treated as ready")
	}
}

type countingK8sClient struct{ calls int }

func (c *countingK8sClient) GetJobStatus(context.Context, string, string) (*model.K8sPodResult, error) {
	c.calls++
	return &model.K8sPodResult{Status: model.JobStatusSucceeded}, nil
}
func (c *countingK8sClient) GetPodLogs(context.Context, string, string, int64) (string, string, error) {
	return "", "", nil
}

type alwaysDupMPRepo struct{}

func (alwaysDupMPRepo) InsertIfNotExists(context.Context, *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	return uuid.New(), false, nil // not inserted → duplicate
}
func (alwaysDupMPRepo) GetByMessageIDAndStream(context.Context, string, string) (*messageprocessing.MessageProcessing, error) {
	return &messageprocessing.MessageProcessing{State: "completed"}, nil
}
func (alwaysDupMPRepo) GetByID(context.Context, uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	return &messageprocessing.MessageProcessing{State: "completed"}, nil
}
func (alwaysDupMPRepo) UpdateState(context.Context, uuid.UUID, string) error { return nil }

type dupUoW struct{}

func (dupUoW) OutboxRepo() pkgoutbox.Repository                    { return nil }
func (dupUoW) MessageProcessingRepo() messageprocessing.Repository { return alwaysDupMPRepo{} }
func (dupUoW) Begin(context.Context) error                         { return nil }
func (dupUoW) Commit() error                                       { return nil }
func (dupUoW) Rollback() error                                     { return nil }

var _ uow.UnitOfWork = (*dupUoW)(nil)

type noopCancelledRepoBinding struct{}

func (noopCancelledRepoBinding) Insert(context.Context, uuid.UUID) error         { return nil }
func (noopCancelledRepoBinding) Exists(context.Context, uuid.UUID) (bool, error) { return false, nil }
func (noopCancelledRepoBinding) DeleteExpired(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

var _ postgres.CancelledSchedulesRepository = (*noopCancelledRepoBinding)(nil)

// TestNodeDeployedBinding_DuplicateSkipsHandler proves a duplicate message is
// ACKed (binding returns nil) without invoking the K8s client: dedup short-
// circuits the handler before any business work runs.
func TestNodeDeployedBinding_DuplicateSkipsHandler(t *testing.T) {
	k8s := &countingK8sClient{}
	cfg := &handlers.HandlerConfig{K8sNamespace: "default", DefaultTaskMaxRetries: 3, ErrorMessageMaxLen: 4096, LogTailLines: 50}
	handler := handlers.NewCheckStatusHandler(k8s, nil, cfg, noopCancelledRepoBinding{}, slog.Default())

	binding := NewNodeDeployedBinding(func() uow.UnitOfWork { return dupUoW{} }, handler, slog.Default())

	err := binding(context.Background(), msgWith(map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"job_name":    "job-x",
	}))
	if err != nil {
		t.Fatalf("binding returned error on duplicate: %v", err)
	}
	if k8s.calls != 0 {
		t.Fatalf("expected handler NOT to call K8s on duplicate, got %d calls", k8s.calls)
	}
}
