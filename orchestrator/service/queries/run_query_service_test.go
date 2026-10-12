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
	runSeq      int64
	activeRuns  []*domain.ActiveRun
	getGraphErr error
	getSeqErr   error
	listErr     error
}

func (f *fakeRunReader) GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
	if f.getGraphErr != nil {
		return nil, nil, f.getGraphErr
	}
	return f.nodes, f.edges, nil
}
func (f *fakeRunReader) GetRunPromotionSeq(ctx context.Context, runID string) (int64, error) {
	if f.getSeqErr != nil {
		return 0, f.getSeqErr
	}
	return f.runSeq, nil
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
		runSeq: 5,
	}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	view, err := svc.GetRunGraph(context.Background(), "run-1")
	require.NoError(t, err)
	assert.Len(t, view.Nodes, 1)
	assert.Len(t, view.Edges, 1)
	assert.Equal(t, int64(5), view.RunPromotionSeq)
	assert.Equal(t, int64(7), view.LatestPromotionSeq)
}

func TestRunQueryService_GetRunGraph_RunSeqZeroIsAllowed(t *testing.T) {
	// 0 means "drift unknown" — service propagates without complaint;
	// the contract is documented in proto + arch docs.
	rr := &fakeRunReader{runSeq: 0}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	view, err := svc.GetRunGraph(context.Background(), "old-run")
	require.NoError(t, err)
	assert.Equal(t, int64(0), view.RunPromotionSeq)
	assert.Equal(t, int64(7), view.LatestPromotionSeq)
}

func TestRunQueryService_GetRunGraph_RunReaderGraphError(t *testing.T) {
	rr := &fakeRunReader{getGraphErr: errors.New("boom")}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	_, err := svc.GetRunGraph(context.Background(), "run-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestRunQueryService_GetRunGraph_RunReaderSeqError(t *testing.T) {
	rr := &fakeRunReader{getSeqErr: errors.New("seq-fail")}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	_, err := svc.GetRunGraph(context.Background(), "run-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seq-fail")
}

func TestRunQueryService_GetRunGraph_TopologyStateError(t *testing.T) {
	rr := &fakeRunReader{runSeq: 5}
	ts := &fakeTopologyStateReader{err: errors.New("pg down")}
	svc := newSvc(rr, ts)

	_, err := svc.GetRunGraph(context.Background(), "run-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg down")
}

func TestRunQueryService_GetRunGraph_RunNotFound(t *testing.T) {
	// Empty graph + runSeq 0 (the canonical "not found" signal from the adapter).
	rr := &fakeRunReader{nodes: nil, edges: nil, runSeq: 0}
	ts := &fakeTopologyStateReader{gen: 7}
	svc := newSvc(rr, ts)

	view, err := svc.GetRunGraph(context.Background(), "nope")
	require.NoError(t, err)
	assert.Empty(t, view.Nodes)
	assert.Empty(t, view.Edges)
	assert.Equal(t, int64(0), view.RunPromotionSeq)
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
	assert.Equal(t, int64(9), view.LatestPromotionSeq)
}

func TestRunQueryService_ListActiveRunDrifts_OnePerSchedule(t *testing.T) {
	rr := &fakeRunReader{
		activeRuns: []*domain.ActiveRun{
			{ScheduleName: "sched-a", RunID: "run-a", PromotionSeq: 5},
			{ScheduleName: "sched-b", RunID: "run-b", PromotionSeq: 7},
		},
	}
	ts := &fakeTopologyStateReader{gen: 9}
	svc := queries.NewRunQueryService(rr, ts, slog.Default())

	view, err := svc.ListActiveRunDrifts(context.Background())
	require.NoError(t, err)
	require.Len(t, view.ActiveRuns, 2)
	assert.Equal(t, int64(9), view.LatestPromotionSeq)
	assert.Equal(t, "sched-a", view.ActiveRuns[0].ScheduleName)
	assert.Equal(t, int64(5), view.ActiveRuns[0].PromotionSeq)
}

func TestRunQueryService_ListActiveRunDrifts_DuplicateScheduleDedupsAndWarns(t *testing.T) {
	// ListActiveRuns returns newest-first per schedule, so run-a1 (slice head)
	// is the newest; PromotionSeq values are unrelated to recency here.
	rr := &fakeRunReader{
		activeRuns: []*domain.ActiveRun{
			{ScheduleName: "sched-a", RunID: "run-a1", PromotionSeq: 5},
			{ScheduleName: "sched-a", RunID: "run-a2", PromotionSeq: 6},
		},
	}
	ts := &fakeTopologyStateReader{gen: 9}
	log, buf := captureLogger()
	svc := queries.NewRunQueryService(rr, ts, log)

	view, err := svc.ListActiveRunDrifts(context.Background())
	require.NoError(t, err)
	require.Len(t, view.ActiveRuns, 1) // deduped to the head (newest) row per schedule
	assert.Equal(t, "run-a1", view.ActiveRuns[0].RunID)
	assert.True(t, strings.Contains(buf.String(), "extra active run"),
		"expected drop warning; got: %s", buf.String())
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

// A run stamped with a seq above the live one breaks the drift invariant. The
// view still returns both values unmodified, and the inversion is logged with
// both seqs.
func TestRunQueryService_GetRunGraph_RunSeqAboveLiveSeqWarns(t *testing.T) {
	rr := &fakeRunReader{runSeq: 8}
	ts := &fakeTopologyStateReader{gen: 7}
	log, buf := captureLogger()
	svc := queries.NewRunQueryService(rr, ts, log)

	view, err := svc.GetRunGraph(context.Background(), "run-1")
	require.NoError(t, err)
	assert.Equal(t, int64(8), view.RunPromotionSeq)
	assert.Equal(t, int64(7), view.LatestPromotionSeq)
	assert.Contains(t, buf.String(), "run promotion seq exceeds the live promotion seq")
	assert.Contains(t, buf.String(), "run_promotion_seq=8")
	assert.Contains(t, buf.String(), "latest_promotion_seq=7")
}
