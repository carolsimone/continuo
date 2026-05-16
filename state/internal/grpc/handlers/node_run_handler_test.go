package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeNodeRunRepo struct {
	rows     []*projection.NodeRun
	err      error
	gotLimit int
}

func (f *fakeNodeRunRepo) List(_ context.Context, _, _, _ string, limit int) ([]*projection.NodeRun, error) {
	f.gotLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

func TestNodeRunHandler_ListNodeRuns_HappyPath(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeNodeRunRepo{
		rows: []*projection.NodeRun{
			{
				ScheduleID: uuid.New(), ScheduleName: "daily", Kind: "cron",
				TerminalStatus: "succeeded",
				TaskID: uuid.New(), TaskStatus: run.TaskStatusSucceeded,
				RetryCount: 0, ImageTag: "v1", ManifestVersion: "m1",
				CreatedAt: now,
			},
		},
	}
	h := NewNodeRunHandler(repo, nil)

	resp, err := h.ListNodeRuns(context.Background(), &statev1.ListNodeRunsRequest{
		ServiceName: "svc", SchemaName: "sch", TableName: "tbl", Limit: 50,
	})
	if err != nil {
		t.Fatalf("ListNodeRuns: %v", err)
	}
	if len(resp.Runs) != 1 {
		t.Fatalf("want 1 row, got %d", len(resp.Runs))
	}
	if resp.Runs[0].Kind != "cron" {
		t.Errorf("Kind = %q, want cron", resp.Runs[0].Kind)
	}
	if resp.Runs[0].ImageTag != "v1" {
		t.Errorf("ImageTag = %q, want v1", resp.Runs[0].ImageTag)
	}
}

func TestNodeRunHandler_ListNodeRuns_RejectsEmptyIdentity(t *testing.T) {
	h := NewNodeRunHandler(&fakeNodeRunRepo{}, nil)

	_, err := h.ListNodeRuns(context.Background(), &statev1.ListNodeRunsRequest{
		ServiceName: "", SchemaName: "sch", TableName: "tbl",
	})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestNodeRunHandler_ListNodeRuns_RepoError(t *testing.T) {
	h := NewNodeRunHandler(&fakeNodeRunRepo{err: errors.New("boom")}, nil)

	_, err := h.ListNodeRuns(context.Background(), &statev1.ListNodeRunsRequest{
		ServiceName: "svc", SchemaName: "sch", TableName: "tbl",
	})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if st, _ := status.FromError(err); st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
}
