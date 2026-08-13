package grpc

import (
	"context"
	"log/slog"
	"testing"
	"time"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeAncestryReader struct {
	result []*domain.NodeAncestor
	err    error
	gotUID string
	gotMax int
}

func (f *fakeAncestryReader) GetScheduleGraph(context.Context, string) (*domain.ScheduleGraph, error) {
	return nil, nil
}
func (f *fakeAncestryReader) ListRuns(context.Context, string, int, int) ([]*domain.RunSummary, int, error) {
	return nil, 0, nil
}
func (f *fakeAncestryReader) ListScheduleTopologies(context.Context) ([]*domain.ScheduleTopologySummary, error) {
	return nil, nil
}
func (f *fakeAncestryReader) GetNodeAncestry(_ context.Context, uid string, maxDepth int) ([]*domain.NodeAncestor, error) {
	f.gotUID, f.gotMax = uid, maxDepth
	return f.result, f.err
}
func (f *fakeAncestryReader) GetNode(context.Context, string, string, string) (*domain.NodeMeta, error) {
	return nil, nil
}

func newAncestryHandler(r *fakeAncestryReader) *QueryHandler {
	return NewQueryHandler(r, nil, nil, slog.Default())
}

func TestGetNodeAncestry_EmptyUID_InvalidArgument(t *testing.T) {
	h := newAncestryHandler(&fakeAncestryReader{})
	_, err := h.GetNodeAncestry(context.Background(), &orchestratorv1.GetNodeAncestryRequest{NodeUniqueId: ""})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetNodeAncestry_NegativeMaxDepth_InvalidArgument(t *testing.T) {
	h := newAncestryHandler(&fakeAncestryReader{})
	_, err := h.GetNodeAncestry(context.Background(), &orchestratorv1.GetNodeAncestryRequest{NodeUniqueId: "a", MaxDepth: -1})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetNodeAncestry_NotFound(t *testing.T) {
	h := newAncestryHandler(&fakeAncestryReader{err: domain.ErrNodeNotFound})
	_, err := h.GetNodeAncestry(context.Background(), &orchestratorv1.GetNodeAncestryRequest{NodeUniqueId: "x"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetNodeAncestry_MapsDomainToProto(t *testing.T) {
	changed := time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)
	reader := &fakeAncestryReader{result: []*domain.NodeAncestor{
		{UniqueID: "c", Depth: 0, FilePath: "models/c.sql", LastCommitSHA: "c2", LastRepo: "acme/demo", LastChangedAt: &changed, LastReleaseID: "r4"},
		{UniqueID: "z", Depth: 1}, // unknown provenance
	}}
	h := newAncestryHandler(reader)
	resp, err := h.GetNodeAncestry(context.Background(), &orchestratorv1.GetNodeAncestryRequest{NodeUniqueId: "c", MaxDepth: 2})
	require.NoError(t, err)
	require.Equal(t, "c", reader.gotUID)
	require.Equal(t, 2, reader.gotMax)
	require.Len(t, resp.Ancestors, 2)

	c := resp.Ancestors[0]
	assert.Equal(t, "c", c.UniqueId)
	assert.Equal(t, "models/c.sql", c.FilePath)
	assert.Equal(t, "c2", c.LastCommitSha)
	require.NotNil(t, c.LastChangedAt)
	assert.Equal(t, changed.Unix(), c.LastChangedAt.AsTime().Unix())

	z := resp.Ancestors[1]
	assert.Nil(t, z.LastChangedAt, "unknown provenance -> unset Timestamp")
	assert.Equal(t, "", z.LastCommitSha)
}
