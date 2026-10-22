package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

type fakeRunReader struct {
	graph   projection.RunGraph
	runs    []projection.RunSummary
	total   int
	latest  int64
	drifts  []projection.ActiveRunDrift
	err     error
	gotSize int
	gotOff  int
}

func (f *fakeRunReader) GetRunGraph(context.Context, uuid.UUID) (projection.RunGraph, error) {
	return f.graph, f.err
}

func (f *fakeRunReader) ListRuns(_ context.Context, _ string, size, off int) ([]projection.RunSummary, int, error) {
	f.gotSize, f.gotOff = size, off
	return f.runs, f.total, f.err
}

func (f *fakeRunReader) ListActiveRunDrifts(context.Context) (int64, []projection.ActiveRunDrift, error) {
	return f.latest, f.drifts, f.err
}

func TestRunReadHandler_GetRunGraph(t *testing.T) {
	taskID := uuid.New()
	h := NewRunReadHandler(&fakeRunReader{graph: projection.RunGraph{
		Nodes: []projection.RunGraphNode{{ServiceName: "svc", SchemaName: "raw", TableName: "b", ScheduleName: "daily",
			NodeType: "dbt-model", Status: run.TaskStatusRunning, TaskID: taskID}},
		Edges:              []projection.RunGraphEdge{{FromNodeID: "svc.raw.b", ToNodeID: "svc.raw.a"}},
		RunPromotionSeq:    5,
		LatestPromotionSeq: 6,
	}}, newTestLogger())

	resp, err := h.GetRunGraph(context.Background(), &statev1.GetRunGraphRequest{RunId: uuid.NewString()})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 1)
	assert.Equal(t, "running", resp.Nodes[0].Status)
	assert.Equal(t, taskID.String(), resp.Nodes[0].TaskId)
	assert.Equal(t, "svc.raw.b", resp.Edges[0].FromNodeId)
	assert.Equal(t, int64(5), resp.RunPromotionSeq)
	assert.Equal(t, int64(6), resp.LatestPromotionSeq)
}

func TestRunReadHandler_RefusesAMalformedRunID(t *testing.T) {
	h := NewRunReadHandler(&fakeRunReader{}, newTestLogger())
	for _, id := range []string{"", "not-a-uuid"} {
		_, err := h.GetRunGraph(context.Background(), &statev1.GetRunGraphRequest{RunId: id})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "run_id %q", id)
	}
}

func TestRunReadHandler_ListRuns(t *testing.T) {
	runID := uuid.New()
	at := time.Date(2026, 10, 14, 23, 0, 0, 0, time.UTC)
	f := &fakeRunReader{runs: []projection.RunSummary{{RunID: runID, ScheduleName: "daily",
		TerminalStatus: run.SchedulerStatusFailed, FailureReason: "stalled", CreatedAt: at, CompletedAt: at.Add(3 * time.Minute)}}, total: 7}
	h := NewRunReadHandler(f, newTestLogger())

	resp, err := h.ListRuns(context.Background(), &statev1.ListRunsRequest{ScheduleName: "daily", PageSize: 20, PageOffset: 40})
	require.NoError(t, err)
	assert.Equal(t, 20, f.gotSize)
	assert.Equal(t, 40, f.gotOff)
	assert.Equal(t, int32(7), resp.TotalCount)
	assert.Equal(t, &statev1.RunSummary{RunId: runID.String(), ScheduleName: "daily", TerminalStatus: "failed",
		CreatedAt: "2026-10-14T23:00:00Z", CompletedAt: "2026-10-14T23:03:00Z", FailureReason: "stalled"}, resp.Runs[0])

	_, err = h.ListRuns(context.Background(), &statev1.ListRunsRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRunReadHandler_ListActiveRunDrifts(t *testing.T) {
	runID := uuid.New()
	h := NewRunReadHandler(&fakeRunReader{latest: 9, drifts: []projection.ActiveRunDrift{
		{ScheduleName: "daily", RunID: runID, RunPromotionSeq: 7}}}, newTestLogger())

	resp, err := h.ListActiveRunDrifts(context.Background(), &statev1.ListActiveRunDriftsRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(9), resp.LatestPromotionSeq)
	assert.Equal(t, []*statev1.ActiveRunDrift{{ScheduleName: "daily", RunId: runID.String(), RunPromotionSeq: 7}}, resp.ActiveRuns)
}

func TestRunReadHandler_ReadFailureIsInternal(t *testing.T) {
	h := NewRunReadHandler(&fakeRunReader{err: errors.New("db down")}, newTestLogger())
	_, err := h.ListActiveRunDrifts(context.Background(), &statev1.ListActiveRunDriftsRequest{})
	assert.Equal(t, codes.Internal, status.Code(err))
}
