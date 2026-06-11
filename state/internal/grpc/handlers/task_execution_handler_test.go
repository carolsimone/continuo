package handlers

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// fakeTaskExecutionRepo captures the pagination arguments the handler computed
// so clamping can be asserted without a database.
type fakeTaskExecutionRepo struct {
	gotPageSize int
	gotOffset   int
	total       int
}

func (f *fakeTaskExecutionRepo) Create(context.Context, *postgres.TaskExecution) error {
	return nil
}

func (f *fakeTaskExecutionRepo) CreateTx(context.Context, *sqlx.Tx, *postgres.TaskExecution) error {
	return nil
}

func (f *fakeTaskExecutionRepo) GetByID(context.Context, uuid.UUID) (*postgres.TaskExecution, error) {
	return nil, postgres.ErrNotFound
}

func (f *fakeTaskExecutionRepo) ListByScheduleID(_ context.Context, _ string, pageSize, pageOffset int) ([]*postgres.TaskExecution, int, error) {
	f.gotPageSize = pageSize
	f.gotOffset = pageOffset
	return nil, f.total, nil
}

func newTaskExecHandler(repo postgres.TaskExecutionRepository) *TaskExecutionHandler {
	return NewTaskExecutionHandler(repo, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func TestListTaskExecutions_ClampsPageSizeAndOffset(t *testing.T) {
	cases := []struct {
		name        string
		reqPageSize int32
		reqOffset   int32
		wantSize    int
		wantOffset  int
	}{
		{"unset defaults to 50", 0, 0, 50, 0},
		{"negative defaults to 50", -3, 0, 50, 0},
		{"within range passes through", 25, 10, 25, 10},
		{"oversized clamps to 200", 5000, 0, 200, 0},
		{"negative offset clamps to 0", 30, -7, 30, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeTaskExecutionRepo{total: 0}
			h := newTaskExecHandler(repo)
			_, err := h.ListTaskExecutions(context.Background(), &statev1.ListTaskExecutionsRequest{
				ScheduleId: "sched-1",
				PageSize:   tc.reqPageSize,
				PageOffset: tc.reqOffset,
			})
			if err != nil {
				t.Fatalf("ListTaskExecutions returned error: %v", err)
			}
			if repo.gotPageSize != tc.wantSize {
				t.Errorf("pageSize = %d, want %d", repo.gotPageSize, tc.wantSize)
			}
			if repo.gotOffset != tc.wantOffset {
				t.Errorf("offset = %d, want %d", repo.gotOffset, tc.wantOffset)
			}
		})
	}
}

func TestListTaskExecutions_RejectsEmptyScheduleID(t *testing.T) {
	h := newTaskExecHandler(&fakeTaskExecutionRepo{})
	_, err := h.ListTaskExecutions(context.Background(), &statev1.ListTaskExecutionsRequest{ScheduleId: ""})
	if err == nil {
		t.Fatal("expected error for empty schedule_id, got nil")
	}
}
