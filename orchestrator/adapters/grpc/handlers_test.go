package grpc_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	grpcadapter "github.com/carolsimone/continuo/orchestrator/adapters/grpc"
	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/carolsimone/continuo/orchestrator/service/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	return grpcadapter.NewQueryHandler(&fakeScheduleAndRunLists{}, rq, &fakeCodeVersionHistoryReader{}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func newHandlerWithLists(lists *fakeScheduleAndRunLists) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(lists, &fakeDriftAwareRuns{}, &fakeCodeVersionHistoryReader{}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func newHandlerWithCodeVersions(cv *fakeCodeVersionHistoryReader) *grpcadapter.QueryHandler {
	return grpcadapter.NewQueryHandler(&fakeScheduleAndRunLists{}, &fakeDriftAwareRuns{}, cv, slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

// fakeCodeVersionHistoryReader satisfies grpcadapter.CodeVersionHistoryReader.
// It records the args the handler passed down so depth/since threading can be
// asserted without duplicating the service's own clamping/defaulting tests.
type fakeCodeVersionHistoryReader struct {
	nodeVersions    []codeversion.VersionView
	nodeVersionsErr error

	diff    *codeversion.VersionDiff
	diffErr error

	upstreamChanges    []codeversion.UpstreamChange
	upstreamChangesErr error
	gotDepth           int32
	gotSince           time.Time

	unitVersions    []codeversion.UnitVersionView
	unitVersionsErr error
	gotUnitID       string
	gotNodeUniqueID string

	runHistory    []codeversion.RunExecution
	runHistoryErr error
}

func (f *fakeCodeVersionHistoryReader) GetNodeVersions(context.Context, string, int32) ([]codeversion.VersionView, error) {
	if f.nodeVersionsErr != nil {
		return nil, f.nodeVersionsErr
	}
	return f.nodeVersions, nil
}

func (f *fakeCodeVersionHistoryReader) GetNodeVersionDiff(context.Context, string, int64, int64) (*codeversion.VersionDiff, error) {
	if f.diffErr != nil {
		return nil, f.diffErr
	}
	return f.diff, nil
}

func (f *fakeCodeVersionHistoryReader) GetUpstreamChanges(_ context.Context, _ string, depth int32, since time.Time) ([]codeversion.UpstreamChange, error) {
	f.gotDepth = depth
	f.gotSince = since
	if f.upstreamChangesErr != nil {
		return nil, f.upstreamChangesErr
	}
	return f.upstreamChanges, nil
}

func (f *fakeCodeVersionHistoryReader) GetCodeUnitVersions(_ context.Context, unitID, uniqueID string, _ int32) ([]codeversion.UnitVersionView, error) {
	f.gotUnitID = unitID
	f.gotNodeUniqueID = uniqueID
	if f.unitVersionsErr != nil {
		return nil, f.unitVersionsErr
	}
	return f.unitVersions, nil
}

func (f *fakeCodeVersionHistoryReader) GetNodeRunHistory(context.Context, string, int32) ([]codeversion.RunExecution, error) {
	if f.runHistoryErr != nil {
		return nil, f.runHistoryErr
	}
	return f.runHistory, nil
}

// ---- GetNodeVersions ----

func TestQueryHandler_GetNodeVersions_MapsAllFields(t *testing.T) {
	promotedAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	cv := &fakeCodeVersionHistoryReader{
		nodeVersions: []codeversion.VersionView{
			{
				UniqueID: "svc.schema.tbl", VersionSeq: 5, ContentHash: "ch", SourceHash: "sh",
				SharedCodeHash: "sch", ConfigHash: "cfh", Runtime: "python", RawCode: "select 1",
				CompiledCode: "compiled", CompiledTruncated: true, ConfigJSON: `{"a":1}`,
				Repo: "acme/demo", CommitSHA: "abc123", ReleaseID: "rel-1", PromotedAt: promotedAt,
				Healed: true, Backfilled: true, IsCurrent: true,
			},
			{}, // zero-value version: exercises empty-time formatting
		},
	}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetNodeVersions(context.Background(), &orchestratorv1.GetNodeVersionsRequest{UniqueId: "svc.schema.tbl", Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Versions, 2)

	v := resp.Versions[0]
	assert.Equal(t, "svc.schema.tbl", v.UniqueId)
	assert.Equal(t, int64(5), v.VersionSeq)
	assert.Equal(t, "ch", v.ContentHash)
	assert.Equal(t, "sh", v.SourceHash)
	assert.Equal(t, "sch", v.SharedCodeHash)
	assert.Equal(t, "cfh", v.ConfigHash)
	assert.Equal(t, "python", v.Runtime)
	assert.Equal(t, "select 1", v.RawCode)
	assert.Equal(t, "compiled", v.CompiledCode)
	assert.True(t, v.CompiledTruncated)
	assert.Equal(t, `{"a":1}`, v.ConfigJson)
	assert.Equal(t, "acme/demo", v.Repo)
	assert.Equal(t, "abc123", v.CommitSha)
	assert.Equal(t, "rel-1", v.ReleaseId)
	assert.Equal(t, promotedAt.Format(time.RFC3339), v.PromotedAt)
	assert.True(t, v.Healed)
	assert.True(t, v.Backfilled)
	assert.True(t, v.IsCurrent)

	assert.Equal(t, "", resp.Versions[1].PromotedAt, "zero time formats as empty string")
}

func TestQueryHandler_GetNodeVersions_UnknownNode_NotFound(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{nodeVersionsErr: domain.ErrNodeNotFound}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetNodeVersions(context.Background(), &orchestratorv1.GetNodeVersionsRequest{UniqueId: "ghost"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestQueryHandler_GetNodeVersions_EmptyHistory_OK(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{nodeVersions: []codeversion.VersionView{}}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetNodeVersions(context.Background(), &orchestratorv1.GetNodeVersionsRequest{UniqueId: "known"})
	require.NoError(t, err)
	assert.Empty(t, resp.Versions)
}

func TestQueryHandler_GetNodeVersions_EmptyUniqueID_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetNodeVersions(context.Background(), &orchestratorv1.GetNodeVersionsRequest{UniqueId: ""})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// ---- GetNodeVersionDiff ----

func TestQueryHandler_GetNodeVersionDiff_MapsFields(t *testing.T) {
	diff := &codeversion.VersionDiff{
		UniqueID:    "n1",
		From:        codeversion.VersionView{VersionSeq: 1},
		To:          codeversion.VersionView{VersionSeq: 2},
		RawCodeDiff: "raw-diff", ConfigDiff: "config-diff",
		SourceChanged: true, SharedCodeChanged: true, ConfigChanged: true, Truncated: true,
	}
	cv := &fakeCodeVersionHistoryReader{diff: diff}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetNodeVersionDiff(context.Background(), &orchestratorv1.GetNodeVersionDiffRequest{UniqueId: "n1", FromSeq: 1, ToSeq: 2})
	require.NoError(t, err)
	require.NotNil(t, resp.Diff)
	assert.Equal(t, "n1", resp.Diff.UniqueId)
	require.NotNil(t, resp.Diff.From)
	require.NotNil(t, resp.Diff.To)
	assert.Equal(t, int64(1), resp.Diff.From.VersionSeq)
	assert.Equal(t, int64(2), resp.Diff.To.VersionSeq)
	assert.Equal(t, "raw-diff", resp.Diff.RawCodeDiff)
	assert.Equal(t, "config-diff", resp.Diff.ConfigDiff)
	assert.True(t, resp.Diff.SourceChanged)
	assert.True(t, resp.Diff.SharedCodeChanged)
	assert.True(t, resp.Diff.ConfigChanged)
	assert.True(t, resp.Diff.Truncated)
}

func TestQueryHandler_GetNodeVersionDiff_UnknownNode_NotFound(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{diffErr: domain.ErrNodeNotFound}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetNodeVersionDiff(context.Background(), &orchestratorv1.GetNodeVersionDiffRequest{UniqueId: "n1", FromSeq: 1, ToSeq: 2})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestQueryHandler_GetNodeVersionDiff_EmptyUniqueID_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetNodeVersionDiff(context.Background(), &orchestratorv1.GetNodeVersionDiffRequest{UniqueId: "", FromSeq: 1, ToSeq: 2})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestQueryHandler_GetNodeVersionDiff_SameSeq_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetNodeVersionDiff(context.Background(), &orchestratorv1.GetNodeVersionDiffRequest{UniqueId: "n1", FromSeq: 5, ToSeq: 5})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// ---- GetUpstreamChanges ----

func TestQueryHandler_GetUpstreamChanges_MapsFields(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{
		upstreamChanges: []codeversion.UpstreamChange{
			{UniqueID: "up-1", Depth: 2, Diff: codeversion.VersionDiff{UniqueID: "up-1", SourceChanged: true}},
		},
	}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "n1", Depth: 4, Since: "2026-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.Len(t, resp.Changes, 1)
	assert.Equal(t, "up-1", resp.Changes[0].UniqueId)
	assert.Equal(t, int32(2), resp.Changes[0].Depth)
	require.NotNil(t, resp.Changes[0].Diff)
	assert.Equal(t, "up-1", resp.Changes[0].Diff.UniqueId)
	assert.True(t, resp.Changes[0].Diff.SourceChanged)

	assert.Equal(t, int32(4), cv.gotDepth)
	wantSince := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, cv.gotSince.Equal(wantSince))
}

func TestQueryHandler_GetUpstreamChanges_UnknownNode_NotFound(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{upstreamChangesErr: domain.ErrNodeNotFound}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "ghost"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestQueryHandler_GetUpstreamChanges_EmptyChanges_OK(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{upstreamChanges: []codeversion.UpstreamChange{}}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "n1"})
	require.NoError(t, err)
	assert.Empty(t, resp.Changes)
}

func TestQueryHandler_GetUpstreamChanges_EmptyUniqueID_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: ""})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestQueryHandler_GetUpstreamChanges_DepthOver10_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "n1", Depth: 11})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestQueryHandler_GetUpstreamChanges_DepthExactly10_Allowed(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "n1", Depth: 10})
	require.NoError(t, err)
	assert.Equal(t, int32(10), cv.gotDepth)
}

func TestQueryHandler_GetUpstreamChanges_NonPositiveDepthPassesThrough(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "n1", Depth: 0})
	require.NoError(t, err)
	assert.Equal(t, int32(0), cv.gotDepth, "the service, not the handler, defaults a non-positive depth")
}

func TestQueryHandler_GetUpstreamChanges_MalformedSince_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "n1", Since: "not-a-date"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestQueryHandler_GetUpstreamChanges_EmptySince_NoTimeFilter(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetUpstreamChanges(context.Background(), &orchestratorv1.GetUpstreamChangesRequest{UniqueId: "n1", Since: ""})
	require.NoError(t, err)
	assert.True(t, cv.gotSince.IsZero())
}

// ---- GetCodeUnitVersions ----

func TestQueryHandler_GetCodeUnitVersions_MapsFields(t *testing.T) {
	promotedAt := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	cv := &fakeCodeVersionHistoryReader{
		unitVersions: []codeversion.UnitVersionView{
			{UnitID: "macro-a", Checksum: "c1", Source: "src", Repo: "acme/demo", CommitSHA: "sha1", ReleaseID: "rel-1", PromotedAt: promotedAt, IsCurrent: true},
		},
	}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetCodeUnitVersions(context.Background(), &orchestratorv1.GetCodeUnitVersionsRequest{UnitId: "macro-a", Limit: 5})
	require.NoError(t, err)
	require.Len(t, resp.Versions, 1)
	v := resp.Versions[0]
	assert.Equal(t, "macro-a", v.UnitId)
	assert.Equal(t, "c1", v.Checksum)
	assert.Equal(t, "src", v.Source)
	assert.Equal(t, "acme/demo", v.Repo)
	assert.Equal(t, "sha1", v.CommitSha)
	assert.Equal(t, "rel-1", v.ReleaseId)
	assert.Equal(t, promotedAt.Format(time.RFC3339), v.PromotedAt)
	assert.True(t, v.IsCurrent)
	assert.Equal(t, "macro-a", cv.gotUnitID)
}

func TestQueryHandler_GetCodeUnitVersions_UnknownUnit_NotFound(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{unitVersionsErr: domain.ErrNodeNotFound}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetCodeUnitVersions(context.Background(), &orchestratorv1.GetCodeUnitVersionsRequest{UnitId: "ghost"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestQueryHandler_GetCodeUnitVersions_EmptyHistory_OK(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{unitVersions: []codeversion.UnitVersionView{}}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetCodeUnitVersions(context.Background(), &orchestratorv1.GetCodeUnitVersionsRequest{UnitId: "macro-a"})
	require.NoError(t, err)
	assert.Empty(t, resp.Versions)
}

func TestQueryHandler_GetCodeUnitVersions_BothSet_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetCodeUnitVersions(context.Background(), &orchestratorv1.GetCodeUnitVersionsRequest{UnitId: "macro-a", UniqueId: "n1"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestQueryHandler_GetCodeUnitVersions_NeitherSet_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetCodeUnitVersions(context.Background(), &orchestratorv1.GetCodeUnitVersionsRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestQueryHandler_GetCodeUnitVersions_ByUniqueID(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{unitVersions: []codeversion.UnitVersionView{{UnitID: "macro-b"}}}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetCodeUnitVersions(context.Background(), &orchestratorv1.GetCodeUnitVersionsRequest{UniqueId: "n1"})
	require.NoError(t, err)
	require.Len(t, resp.Versions, 1)
	assert.Equal(t, "n1", cv.gotNodeUniqueID)
	assert.Equal(t, "", cv.gotUnitID)
}

// ---- GetNodeRunHistory ----

func TestQueryHandler_GetNodeRunHistory_MapsFields(t *testing.T) {
	createdAt := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	completedAt := time.Date(2026, 5, 1, 9, 5, 0, 0, time.UTC)
	cv := &fakeCodeVersionHistoryReader{
		runHistory: []codeversion.RunExecution{
			{RunID: "run-1", TaskID: "task-1", Status: "succeeded", ScheduleName: "sched-a", Operation: "run", ImageTag: "abc", ContentHash: "ch1", CreatedAt: createdAt, CompletedAt: completedAt},
			{RunID: "run-2", CreatedAt: createdAt}, // still running: zero CompletedAt, empty ContentHash
		},
	}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetNodeRunHistory(context.Background(), &orchestratorv1.GetNodeRunHistoryRequest{UniqueId: "n1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, resp.Runs, 2)

	r := resp.Runs[0]
	assert.Equal(t, "run-1", r.RunId)
	assert.Equal(t, "task-1", r.TaskId)
	assert.Equal(t, "succeeded", r.Status)
	assert.Equal(t, "sched-a", r.ScheduleName)
	assert.Equal(t, "run", r.Operation)
	assert.Equal(t, "abc", r.ImageTag)
	assert.Equal(t, "ch1", r.ContentHash)
	assert.Equal(t, createdAt.Format(time.RFC3339), r.CreatedAt)
	assert.Equal(t, completedAt.Format(time.RFC3339), r.CompletedAt)

	r2 := resp.Runs[1]
	assert.Equal(t, "", r2.CompletedAt, "unfinished run: empty completed_at")
	assert.Equal(t, "", r2.ContentHash, "run predating the stamp: empty content_hash")
}

func TestQueryHandler_GetNodeRunHistory_UnknownNode_NotFound(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{runHistoryErr: domain.ErrNodeNotFound}
	h := newHandlerWithCodeVersions(cv)
	_, err := h.GetNodeRunHistory(context.Background(), &orchestratorv1.GetNodeRunHistoryRequest{UniqueId: "ghost"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestQueryHandler_GetNodeRunHistory_EmptyHistory_OK(t *testing.T) {
	cv := &fakeCodeVersionHistoryReader{runHistory: []codeversion.RunExecution{}}
	h := newHandlerWithCodeVersions(cv)
	resp, err := h.GetNodeRunHistory(context.Background(), &orchestratorv1.GetNodeRunHistoryRequest{UniqueId: "n1"})
	require.NoError(t, err)
	assert.Empty(t, resp.Runs)
}

func TestQueryHandler_GetNodeRunHistory_EmptyUniqueID_InvalidArgument(t *testing.T) {
	h := newHandlerWithCodeVersions(&fakeCodeVersionHistoryReader{})
	_, err := h.GetNodeRunHistory(context.Background(), &orchestratorv1.GetNodeRunHistoryRequest{UniqueId: ""})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
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
