package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	grpcadapter "github.com/carolsimone/continuo/orchestrator/adapters/grpc"
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
// goes through DriftAwareRunReader instead.
type fakeScheduleAndRunLists struct{}

func (fakeScheduleAndRunLists) GetScheduleGraph(context.Context, string) (*domain.ScheduleGraph, error) {
	return &domain.ScheduleGraph{}, nil
}
func (fakeScheduleAndRunLists) ListRuns(context.Context, string) ([]*domain.RunSummary, error) {
	return nil, nil
}
func (fakeScheduleAndRunLists) ListScheduleTopologies(context.Context) ([]*domain.ScheduleTopologySummary, error) {
	return nil, nil
}

func newHandler(rq *fakeDriftAwareRuns) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(fakeScheduleAndRunLists{}, rq, slog.New(slog.NewTextHandler(os.Stderr, nil)))
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
