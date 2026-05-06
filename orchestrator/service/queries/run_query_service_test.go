// File: orchestrator/service/queries/run_query_service_test.go
package queries_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRunReader is a hand-rolled fake satisfying queries.RunReader.
type fakeRunReader struct {
	nodes       []*domain.TableNode
	edges       []*domain.GraphEdge
	runGen      int64
	activeRuns  []*domain.ActiveRun
	getGraphErr error
	getGenErr   error
	listErr     error
}

func (f *fakeRunReader) GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
	if f.getGraphErr != nil {
		return nil, nil, f.getGraphErr
	}
	return f.nodes, f.edges, nil
}
func (f *fakeRunReader) GetRunTopologyGeneration(ctx context.Context, runID string) (int64, error) {
	if f.getGenErr != nil {
		return 0, f.getGenErr
	}
	return f.runGen, nil
}
func (f *fakeRunReader) ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.activeRuns, nil
}

// fakeTopologyStateReader satisfies queries.TopologyStateReader.
type fakeTopologyStateReader struct {
	gen int64
	err error
}

func (f *fakeTopologyStateReader) GetGeneration(ctx context.Context) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.gen, nil
}

func newSvc(rr *fakeRunReader, ts *fakeTopologyStateReader) *queries.RunQueryService {
	return queries.NewRunQueryService(rr, ts, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func TestRunQueryService_GetRunGraph_LatestExceedsRun(t *testing.T) {
	rr := &fakeRunReader{
		nodes:  []*domain.TableNode{{TableName: "t1"}},
		edges:  []*domain.GraphEdge{{FromNodeID: "a", ToNodeID: "b"}},
		runGen: 5,
	}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	view, err := svc.GetRunGraph(context.Background(), "run-1")
	require.NoError(t, err)
	assert.Len(t, view.Nodes, 1)
	assert.Len(t, view.Edges, 1)
	assert.Equal(t, int64(5), view.RunTopologyGeneration)
	assert.Equal(t, int64(7), view.LatestTopologyGeneration)
}

func TestRunQueryService_GetRunGraph_RunGenZeroIsAllowed(t *testing.T) {
	// 0 means "drift unknown" — service propagates without complaint;
	// the contract is documented in proto + arch docs.
	rr := &fakeRunReader{runGen: 0}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	view, err := svc.GetRunGraph(context.Background(), "old-run")
	require.NoError(t, err)
	assert.Equal(t, int64(0), view.RunTopologyGeneration)
	assert.Equal(t, int64(7), view.LatestTopologyGeneration)
}

func TestRunQueryService_GetRunGraph_RunReaderGraphError(t *testing.T) {
	rr := &fakeRunReader{getGraphErr: errors.New("boom")}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	_, err := svc.GetRunGraph(context.Background(), "run-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestRunQueryService_GetRunGraph_RunReaderGenError(t *testing.T) {
	rr := &fakeRunReader{getGenErr: errors.New("gen-fail")}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	_, err := svc.GetRunGraph(context.Background(), "run-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gen-fail")
}

func TestRunQueryService_GetRunGraph_TopologyStateError(t *testing.T) {
	rr := &fakeRunReader{runGen: 5}
	ts := &fakeTopologyStateReader{err: errors.New("pg down")}
	svc := newSvc(rr, ts)

	_, err := svc.GetRunGraph(context.Background(), "run-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg down")
}

func TestRunQueryService_GetRunGraph_RunNotFound(t *testing.T) {
	// Empty graph + runGen 0 (the canonical "not found" signal from the adapter).
	rr := &fakeRunReader{nodes: nil, edges: nil, runGen: 0}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	view, err := svc.GetRunGraph(context.Background(), "nope")
	require.NoError(t, err)
	assert.Empty(t, view.Nodes)
	assert.Empty(t, view.Edges)
	assert.Equal(t, int64(0), view.RunTopologyGeneration)
}

// captureLogger lets tests assert on warning output for invariant checks.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})), buf
}

func TestRunQueryService_ListActiveRunDrifts_Empty(t *testing.T) {
	rr := &fakeRunReader{activeRuns: nil}
	ts := &fakeTopologyStateReader{gen: 9}
	svc := queries.NewRunQueryService(rr, ts, slog.Default())

	view, err := svc.ListActiveRunDrifts(context.Background())
	require.NoError(t, err)
	assert.Empty(t, view.ActiveRuns)
	assert.Equal(t, int64(9), view.LatestTopologyGeneration)
}

func TestRunQueryService_ListActiveRunDrifts_OnePerSchedule(t *testing.T) {
	rr := &fakeRunReader{
		activeRuns: []*domain.ActiveRun{
			{ScheduleName: "sched-a", RunID: "run-a", TopologyGeneration: 5},
			{ScheduleName: "sched-b", RunID: "run-b", TopologyGeneration: 7},
		},
	}
	ts := &fakeTopologyStateReader{gen: 9}
	svc := queries.NewRunQueryService(rr, ts, slog.Default())

	view, err := svc.ListActiveRunDrifts(context.Background())
	require.NoError(t, err)
	require.Len(t, view.ActiveRuns, 2)
	assert.Equal(t, int64(9), view.LatestTopologyGeneration)
	assert.Equal(t, "sched-a", view.ActiveRuns[0].ScheduleName)
	assert.Equal(t, int64(5), view.ActiveRuns[0].TopologyGeneration)
}

func TestRunQueryService_ListActiveRunDrifts_DuplicateScheduleLogsWarning(t *testing.T) {
	rr := &fakeRunReader{
		activeRuns: []*domain.ActiveRun{
			{ScheduleName: "sched-a", RunID: "run-a1", TopologyGeneration: 5},
			{ScheduleName: "sched-a", RunID: "run-a2", TopologyGeneration: 6},
		},
	}
	ts := &fakeTopologyStateReader{gen: 9}
	log, buf := captureLogger()
	svc := queries.NewRunQueryService(rr, ts, log)

	view, err := svc.ListActiveRunDrifts(context.Background())
	require.NoError(t, err)
	require.Len(t, view.ActiveRuns, 2) // both surfaced
	assert.True(t, strings.Contains(buf.String(), "multiple active runs"),
		"expected warning log; got: %s", buf.String())
}

func TestRunQueryService_ListActiveRunDrifts_RunReaderError(t *testing.T) {
	rr := &fakeRunReader{listErr: errors.New("neo4j down")}
	ts := &fakeTopologyStateReader{gen: 9}
	svc := queries.NewRunQueryService(rr, ts, slog.Default())

	_, err := svc.ListActiveRunDrifts(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "neo4j down")
}

func TestRunQueryService_ListActiveRunDrifts_TopologyStateError(t *testing.T) {
	rr := &fakeRunReader{activeRuns: []*domain.ActiveRun{{ScheduleName: "x", RunID: "y"}}}
	ts := &fakeTopologyStateReader{err: errors.New("pg down")}
	svc := queries.NewRunQueryService(rr, ts, slog.Default())

	_, err := svc.ListActiveRunDrifts(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg down")
}
