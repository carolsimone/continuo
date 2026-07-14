package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	grpcadapter "github.com/carolsimone/continuo/orchestrator/adapters/grpc"
	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDriftAwareRuns satisfies grpcadapter.DriftAwareRunReader. Returns
// deterministic views so the handler's proto-mapping can be asserted.
type fakeDriftAwareRuns struct {
	runGraphView *queries.RunGraphView
	runGraphErr  error
	driftView    *queries.ActiveRunDriftView
	driftErr     error
}

func (f *fakeDriftAwareRuns) GetRunGraph(ctx context.Context, runID string) (*queries.RunGraphView, error) {
	if f.runGraphErr != nil {
		return nil, f.runGraphErr
	}
	return f.runGraphView, nil
}
func (f *fakeDriftAwareRuns) ListActiveRunDrifts(ctx context.Context) (*queries.ActiveRunDriftView, error) {
	if f.driftErr != nil {
		return nil, f.driftErr
	}
	return f.driftView, nil
}

// fakeScheduleAndRunLists satisfies grpcadapter.ScheduleAndRunListReader for
// the GetScheduleGraph and ListRuns RPCs. The drift-aware GetRunGraph path
// goes through DriftAwareRunReader instead. It records the limit/offset the
// handler computed so pagination clamping can be asserted, and replays a fixed
// page + total back.
type fakeScheduleAndRunLists struct {
	gotLimit  int
	gotOffset int
	runs      []*domain.RunSummary
	total     int
}

func (fakeScheduleAndRunLists) GetScheduleGraph(context.Context, string) (*domain.ScheduleGraph, error) {
	return &domain.ScheduleGraph{}, nil
}
func (f *fakeScheduleAndRunLists) ListRuns(_ context.Context, _ string, limit, offset int) ([]*domain.RunSummary, int, error) {
	f.gotLimit = limit
	f.gotOffset = offset
	return f.runs, f.total, nil
}
func (fakeScheduleAndRunLists) ListScheduleTopologies(context.Context) ([]*domain.ScheduleTopologySummary, error) {
	return nil, nil
}
func (fakeScheduleAndRunLists) GetNodeAncestry(context.Context, string, int) ([]*domain.NodeAncestor, error) {
	return nil, nil
}
func (fakeScheduleAndRunLists) GetNode(context.Context, string, string, string) (*domain.NodeMeta, error) {
	return nil, nil
}

func newHandler(rq *fakeDriftAwareRuns) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(&fakeScheduleAndRunLists{}, rq, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func newHandlerWithLists(lists *fakeScheduleAndRunLists) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(lists, &fakeDriftAwareRuns{}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func TestQueryHandler_GetRunGraph_PopulatesGenerations(t *testing.T) {
	rq := &fakeDriftAwareRuns{
		runGraphView: &queries.RunGraphView{
			Nodes:                    []*domain.TableNode{{TableName: "t"}},
			Edges:                    []*domain.GraphEdge{},
			RunTopologyGeneration:    5,
			LatestTopologyGeneration: 7,
		},
	}
	h := newHandler(rq)
	resp, err := h.GetRunGraph(context.Background(), &orchestratorv1.GetRunGraphRequest{RunId: "r1"})
	require.NoError(t, err)
	assert.Equal(t, int64(5), resp.RunTopologyGeneration)
	assert.Equal(t, int64(7), resp.LatestTopologyGeneration)
	assert.Len(t, resp.Nodes, 1)
}

func TestQueryHandler_GetRunGraph_RejectsEmptyRunID(t *testing.T) {
	h := newHandler(&fakeDriftAwareRuns{})
	_, err := h.GetRunGraph(context.Background(), &orchestratorv1.GetRunGraphRequest{RunId: ""})
	require.Error(t, err)
}

func TestQueryHandler_ListActiveRunDrifts_PopulatesView(t *testing.T) {
	rq := &fakeDriftAwareRuns{
		driftView: &queries.ActiveRunDriftView{
			ActiveRuns: []*domain.ActiveRun{
				{ScheduleName: "sched-a", RunID: "run-a", TopologyGeneration: 5},
			},
			LatestTopologyGeneration: 7,
		},
	}
	h := newHandler(rq)
	resp, err := h.ListActiveRunDrifts(context.Background(), &orchestratorv1.ListActiveRunDriftsRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(7), resp.LatestTopologyGeneration)
	require.Len(t, resp.ActiveRuns, 1)
	assert.Equal(t, "sched-a", resp.ActiveRuns[0].ScheduleName)
	assert.Equal(t, "run-a", resp.ActiveRuns[0].RunId)
	assert.Equal(t, int64(5), resp.ActiveRuns[0].RunTopologyGeneration)
}

func TestQueryHandler_ListRuns_ClampsPageSizeAndOffset(t *testing.T) {
	cases := []struct {
		name        string
		reqPageSize int32
		reqOffset   int32
		wantLimit   int
		wantOffset  int
	}{
		{"unset defaults to 50", 0, 0, 50, 0},
		{"negative defaults to 50", -1, 0, 50, 0},
		{"within range passes through", 25, 10, 25, 10},
		{"oversized clamps to 200", 1000, 0, 200, 0},
		{"negative offset clamps to 0", 30, -5, 30, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lists := &fakeScheduleAndRunLists{total: 3}
			h := newHandlerWithLists(lists)
			_, err := h.ListRuns(context.Background(), &orchestratorv1.ListRunsRequest{
				ScheduleName: "sched-a",
				PageSize:     tc.reqPageSize,
				PageOffset:   tc.reqOffset,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantLimit, lists.gotLimit, "limit")
			assert.Equal(t, tc.wantOffset, lists.gotOffset, "offset")
		})
	}
}

func TestQueryHandler_ListRuns_PopulatesTotalCount(t *testing.T) {
	lists := &fakeScheduleAndRunLists{
		total: 42,
		runs: []*domain.RunSummary{
			{RunID: "r1", ScheduleName: "sched-a", TerminalStatus: "succeeded"},
		},
	}
	h := newHandlerWithLists(lists)
	resp, err := h.ListRuns(context.Background(), &orchestratorv1.ListRunsRequest{ScheduleName: "sched-a"})
	require.NoError(t, err)
	assert.Equal(t, int32(42), resp.TotalCount)
	require.Len(t, resp.Runs, 1)
	assert.Equal(t, "r1", resp.Runs[0].RunId)
}

func TestQueryHandler_ListRuns_RejectsEmptyScheduleName(t *testing.T) {
	h := newHandlerWithLists(&fakeScheduleAndRunLists{})
	_, err := h.ListRuns(context.Background(), &orchestratorv1.ListRunsRequest{ScheduleName: ""})
	require.Error(t, err)
}
