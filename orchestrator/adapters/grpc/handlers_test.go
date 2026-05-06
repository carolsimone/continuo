// File: orchestrator/adapters/grpc/handlers_test.go
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

// fakeRunQueries satisfies the interface QueryHandler depends on for the
// drift-aware methods. It returns deterministic views.
type fakeRunQueries struct {
	runGraphView *queries.RunGraphView
	runGraphErr  error
	driftView    *queries.ActiveRunDriftView
	driftErr     error
}

func (f *fakeRunQueries) GetRunGraph(ctx context.Context, runID string) (*queries.RunGraphView, error) {
	if f.runGraphErr != nil {
		return nil, f.runGraphErr
	}
	return f.runGraphView, nil
}
func (f *fakeRunQueries) ListActiveRunDrifts(ctx context.Context) (*queries.ActiveRunDriftView, error) {
	if f.driftErr != nil {
		return nil, f.driftErr
	}
	return f.driftView, nil
}

// fakeReader satisfies the existing QueryReader (only used for ListRuns/GetScheduleGraph here).
type fakeReader struct{}

func (fakeReader) GetScheduleGraph(context.Context, string) (*domain.ScheduleGraph, error) {
	return &domain.ScheduleGraph{}, nil
}
func (fakeReader) ListRuns(context.Context, string) ([]*domain.RunSummary, error) {
	return nil, nil
}
func (fakeReader) GetRunGraph(context.Context, string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
	return nil, nil, nil
}

func newHandler(rq *fakeRunQueries) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(fakeReader{}, rq, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func TestQueryHandler_GetRunGraph_PopulatesGenerations(t *testing.T) {
	rq := &fakeRunQueries{
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
	h := newHandler(&fakeRunQueries{})
	_, err := h.GetRunGraph(context.Background(), &orchestratorv1.GetRunGraphRequest{RunId: ""})
	require.Error(t, err)
}

func TestQueryHandler_ListActiveRunDrifts_PopulatesView(t *testing.T) {
	rq := &fakeRunQueries{
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
