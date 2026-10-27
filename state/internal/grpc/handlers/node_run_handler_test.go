package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	// tenants records the tenant each request reads.
	tenants []string
}

// of is the node-run reads of tenantID.
func (f *fakeNodeRunRepo) of(tenantID string) postgres.NodeRunRepository {
	f.tenants = append(f.tenants, tenantID)
	return f
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
	class, reason := "task", "exit_code"
	repo := &fakeNodeRunRepo{
		rows: []*projection.NodeRun{
			{
				ScheduleID: uuid.New(), ScheduleName: "daily", Kind: "cron",
				TerminalStatus: "succeeded",
				TaskID:         uuid.New(), TaskStatus: run.TaskStatusSucceeded,
				Attempt: 2, ImageTag: "v1", CreatedAt: now,
				FailureClass: &class, FailureReason: &reason,
			},
		},
	}
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)

	resp, err := h.ListNodeRuns(requestCtx(), &statev1.ListNodeRunsRequest{
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
	if got := resp.Runs[0].GetAttempt(); got != 2 {
		t.Errorf("Attempt = %d, want 2", got)
	}
	if got := resp.Runs[0].GetFailureClass(); got != class {
		t.Errorf("FailureClass = %q, want %q", got, class)
	}
	if got := resp.Runs[0].GetFailureReason(); got != reason {
		t.Errorf("FailureReason = %q, want %q", got, reason)
	}
}

func TestNodeRunHandler_ListNodeRuns_RejectsEmptyIdentity(t *testing.T) {
	h := NewNodeRunHandler((&fakeNodeRunRepo{}).of, 24*time.Hour, nil)

	_, err := h.ListNodeRuns(requestCtx(), &statev1.ListNodeRunsRequest{
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
	h := NewNodeRunHandler((&fakeNodeRunRepo{err: errors.New("boom")}).of, 24*time.Hour, nil)

	_, err := h.ListNodeRuns(requestCtx(), &statev1.ListNodeRunsRequest{
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
	twoHours := 7200
	repo := &fakeNodeRunRepo{
		nodeTotal: 7,
		nodeRows: []*projection.NodeSummary{{
			ServiceName: "svc", SchemaName: "an", TableName: "fct_orders",
			RunCount: 48, SuccessRatePct: -1, AvgDurationSec: -1, P95DurationSec: 21,
			FlakyRatePct: 4, LastStatus: "succeeded", LastRunAt: when,
			TimeoutSeconds: &twoHours,
		}},
	}
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)

	resp, err := h.ListNodes(requestCtx(), &statev1.ListNodesRequest{
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
	if resp.Nodes[0].TimeoutSeconds != 7200 {
		t.Errorf("TimeoutSeconds = %d, want 7200", resp.Nodes[0].TimeoutSeconds)
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
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)
	if _, err := h.ListNodes(requestCtx(), &statev1.ListNodesRequest{Limit: 9999}); err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if repo.gotNodeLimit != 200 {
		t.Errorf("limit passed = %d, want 200 (clamp)", repo.gotNodeLimit)
	}
}

func TestNodeRunHandler_ListNodeNames(t *testing.T) {
	repo := &fakeNodeRunRepo{nodeNames: []string{"customers", "orders"}}
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)
	resp, err := h.ListNodeNames(requestCtx(), &statev1.ListNodeNamesRequest{ServiceName: "svc"})
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
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)
	resp, err := h.ListNodeServices(requestCtx(), &statev1.ListNodeServicesRequest{})
	if err != nil {
		t.Fatalf("ListNodeServices: %v", err)
	}
	if len(resp.ServiceNames) != 2 || resp.ServiceNames[0] != "billing" {
		t.Errorf("ServiceNames = %v", resp.ServiceNames)
	}
}

func TestNodeRunHandler_ListNodeServices_Error(t *testing.T) {
	repo := &fakeNodeRunRepo{nodeServicesErr: errors.New("boom")}
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)
	if _, err := h.ListNodeServices(requestCtx(), &statev1.ListNodeServicesRequest{}); err == nil {
		t.Fatal("expected error")
	}
}

// A node's tasks get its declared timeout, else the install default; the
// catalog says which.
func TestNodeRunHandler_ListNodes_TimeoutIsDeclaredOrTheInstallDefault(t *testing.T) {
	eightHours := 28800
	repo := &fakeNodeRunRepo{nodeRows: []*projection.NodeSummary{
		{ServiceName: "svc", SchemaName: "sch", TableName: "orders_full", TimeoutSeconds: &eightHours},
		{ServiceName: "svc", SchemaName: "sch", TableName: "fx_rates"},
	}}
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)

	resp, err := h.ListNodes(requestCtx(), &statev1.ListNodesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 2)
	assert.Equal(t, int32(28800), resp.Nodes[0].GetTimeoutSeconds())
	assert.True(t, resp.Nodes[0].GetTimeoutDeclared())
	assert.Equal(t, int32(86400), resp.Nodes[1].GetTimeoutSeconds(), "a node that declares none gets the install default")
	assert.False(t, resp.Nodes[1].GetTimeoutDeclared())
}

// A node's run whose task waits for the node's lease carries the reason, the
// holder's run and when it began waiting; a run that waits for nothing carries
// none of the three.
func TestNodeRunHandler_ListNodeRuns_ReportsALeaseWaitAndItsHolder(t *testing.T) {
	holderRun := uuid.New()
	since := time.Date(2026, 10, 24, 10, 5, 0, 0, time.UTC)
	repo := &fakeNodeRunRepo{rows: []*projection.NodeRun{
		{ScheduleID: uuid.New(), TaskID: uuid.New(), TaskStatus: run.TaskStatusPending, CreatedAt: since,
			WaitReason: run.WaitReasonNodeLease, WaitingOnRunID: &holderRun, WaitingSince: &since},
		{ScheduleID: holderRun, TaskID: uuid.New(), TaskStatus: run.TaskStatusRunning, CreatedAt: since},
	}}
	h := NewNodeRunHandler(repo.of, 24*time.Hour, nil)

	resp, err := h.ListNodeRuns(requestCtx(), &statev1.ListNodeRunsRequest{
		ServiceName: "svc", SchemaName: "sch", TableName: "tbl",
	})
	require.NoError(t, err)
	require.Len(t, resp.Runs, 2)
	assert.Equal(t, "node_lease", resp.Runs[0].GetWaitReason())
	assert.Equal(t, holderRun.String(), resp.Runs[0].GetWaitingOnRunId())
	assert.Equal(t, "2026-10-24T10:05:00Z", resp.Runs[0].GetWaitingSince())
	assert.Empty(t, resp.Runs[1].GetWaitReason())
	assert.Empty(t, resp.Runs[1].GetWaitingOnRunId())
	assert.Empty(t, resp.Runs[1].GetWaitingSince())
}
