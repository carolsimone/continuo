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

	// node-catalog (ListNodes) capture + canned return
	nodeRows      []*projection.NodeSummary
	nodeTotal     int
	nodeErr       error
	gotSearch     string
	gotService    string
	gotOperation  string
	gotNodeLimit  int
	gotNodeOffset int

	// node-names (ListNodeNames) capture + canned return
	nodeNames       []string
	nodeNamesErr    error
	gotNamesService string

	// node-services (ListNodeServices) canned return
	nodeServices    []string
	nodeServicesErr error
}

func (f *fakeNodeRunRepo) List(_ context.Context, _, _, _, _ string, limit int) ([]*projection.NodeRun, error) {
	f.gotLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

func (f *fakeNodeRunRepo) ListNodes(_ context.Context, search, service, operation string, limit, offset int) ([]*projection.NodeSummary, int, error) {
	f.gotSearch, f.gotService, f.gotOperation, f.gotNodeLimit, f.gotNodeOffset = search, service, operation, limit, offset
	if f.nodeErr != nil {
		return nil, 0, f.nodeErr
	}
	return f.nodeRows, f.nodeTotal, nil
}

func (f *fakeNodeRunRepo) ListNodeNames(_ context.Context, service string) ([]string, error) {
	f.gotNamesService = service
	return f.nodeNames, f.nodeNamesErr
}

func (f *fakeNodeRunRepo) ListNodeServices(_ context.Context) ([]string, error) {
	return f.nodeServices, f.nodeServicesErr
}

func TestNodeRunHandler_ListNodeRuns_HappyPath(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeNodeRunRepo{
		rows: []*projection.NodeRun{
			{
				ScheduleID: uuid.New(), ScheduleName: "daily", Kind: "cron",
				TerminalStatus: "succeeded",
				TaskID:         uuid.New(), TaskStatus: run.TaskStatusSucceeded,
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

func TestNodeRunHandler_ListNodes_MapsRowsAndClampsPaging(t *testing.T) {
	when := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	repo := &fakeNodeRunRepo{
		nodeTotal: 7,
		nodeRows: []*projection.NodeSummary{{
			ServiceName: "svc", SchemaName: "an", TableName: "fct_orders",
			RunCount: 48, SuccessRatePct: -1, AvgDurationSec: -1, P95DurationSec: 21,
			FlakyRatePct: 4, LastStatus: "succeeded", LastRunAt: when,
		}},
	}
	h := NewNodeRunHandler(repo, nil)

	resp, err := h.ListNodes(context.Background(), &statev1.ListNodesRequest{
		Search: "fct", ServiceName: "svc", Limit: 0, Offset: -5, // 0 -> default 50, -5 -> 0
	})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if resp.TotalCount != 7 {
		t.Errorf("TotalCount = %d, want 7", resp.TotalCount)
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("want 1 node, got %d", len(resp.Nodes))
	}
	if resp.Nodes[0].TableName != "fct_orders" {
		t.Errorf("TableName = %q, want fct_orders", resp.Nodes[0].TableName)
	}
	if resp.Nodes[0].SuccessRatePct != -1 {
		t.Errorf("SuccessRatePct = %d, want -1 (sentinel passthrough)", resp.Nodes[0].SuccessRatePct)
	}
	if resp.Nodes[0].LastRunAt != "2026-06-08T10:00:00Z" {
		t.Errorf("LastRunAt = %q, want RFC3339", resp.Nodes[0].LastRunAt)
	}
	if repo.gotNodeLimit != 50 {
		t.Errorf("limit passed = %d, want 50 (0 -> default)", repo.gotNodeLimit)
	}
	if repo.gotNodeOffset != 0 {
		t.Errorf("offset passed = %d, want 0 (neg -> 0)", repo.gotNodeOffset)
	}
	if repo.gotSearch != "fct" || repo.gotService != "svc" {
		t.Errorf("filters not forwarded: search=%q service=%q", repo.gotSearch, repo.gotService)
	}
}

func TestNodeRunHandler_ListNodes_ClampsLimitTo200(t *testing.T) {
	repo := &fakeNodeRunRepo{}
	h := NewNodeRunHandler(repo, nil)
	if _, err := h.ListNodes(context.Background(), &statev1.ListNodesRequest{Limit: 9999}); err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if repo.gotNodeLimit != 200 {
		t.Errorf("limit passed = %d, want 200 (clamp)", repo.gotNodeLimit)
	}
}

func TestNodeRunHandler_ListNodeNames(t *testing.T) {
	repo := &fakeNodeRunRepo{nodeNames: []string{"customers", "orders"}}
	h := NewNodeRunHandler(repo, nil)
	resp, err := h.ListNodeNames(context.Background(), &statev1.ListNodeNamesRequest{ServiceName: "svc"})
	if err != nil {
		t.Fatalf("ListNodeNames: %v", err)
	}
	if repo.gotNamesService != "svc" {
		t.Errorf("service not forwarded: %q", repo.gotNamesService)
	}
	if len(resp.TableNames) != 2 || resp.TableNames[0] != "customers" {
		t.Errorf("TableNames = %v", resp.TableNames)
	}
}

func TestNodeRunHandler_ListNodeServices(t *testing.T) {
	repo := &fakeNodeRunRepo{nodeServices: []string{"billing", "ledger"}}
	h := NewNodeRunHandler(repo, nil)
	resp, err := h.ListNodeServices(context.Background(), &statev1.ListNodeServicesRequest{})
	if err != nil {
		t.Fatalf("ListNodeServices: %v", err)
	}
	if len(resp.ServiceNames) != 2 || resp.ServiceNames[0] != "billing" {
		t.Errorf("ServiceNames = %v", resp.ServiceNames)
	}
}

func TestNodeRunHandler_ListNodeServices_Error(t *testing.T) {
	repo := &fakeNodeRunRepo{nodeServicesErr: errors.New("boom")}
	h := NewNodeRunHandler(repo, nil)
	if _, err := h.ListNodeServices(context.Background(), &statev1.ListNodeServicesRequest{}); err == nil {
		t.Fatal("expected error")
	}
}
