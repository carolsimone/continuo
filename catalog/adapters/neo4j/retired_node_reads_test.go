//go:build integration

package neo4jinfra

import (
	"context"
	"testing"
	"time"

	grpcadapter "github.com/carolsimone/continuo/catalog/adapters/grpc"
	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/service/queries"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The tests of this file read a node a release dropped. A retired node keeps
// its :CURRENT edge as the record of its last version, and the readers keep
// the proto's contract: no version of a retired node is current, it runs no
// shared code, it has no source location, and GetNode finds it only with
// include_inactive. Each test reads the node while it is live first, so it
// shows that retirement alone changes the answer.

const (
	retiredUp   = "analytics.table_up"
	retiredX    = "analytics.table_x"
	retiredUnit = "svc:macro_x"
)

// retiredNodeFixture is one tenant whose releases r1 and r2 carry table_up and
// table_x, with table_x's two versions x-1 and x-2 (x-2 current, using the
// shared unit retiredUnit), read through the gRPC handler over the Neo4j
// readers.
type retiredNodeFixture struct {
	*swapFixture
	ctx      context.Context
	handler  *grpcadapter.QueryHandler
	versions *CodeVersionQueryRepository
}

func newRetiredNodeFixture(t *testing.T) *retiredNodeFixture {
	t.Helper()
	f := &retiredNodeFixture{swapFixture: newSwapFixture(t)}
	f.ctx = identity.WithTenant(context.Background(), f.tenant)
	f.versions = NewCodeVersionQueryRepository(f.client, quietLogger())
	f.handler = grpcadapter.NewQueryHandler(
		NewNodeQueryRepository(f.client, quietLogger()),
		queries.NewCodeVersionQueryService(f.versions),
		queries.NewPrecedentQueryService(NewPrecedentQueryRepository(f.client, quietLogger())),
		quietLogger())

	unit := codeversion.UnitRef{UnitID: retiredUnit, Checksum: "u-1"}
	for _, step := range []struct {
		rel  string
		seq  int64
		hash string
	}{{"r1", 1, "x-1"}, {"r2", 2, "x-2"}} {
		f.promoteAt(step.rel, step.seq, releaseOf(modelNode(retiredUp), modelNode(retiredX, retiredUp)), promotedAtOf(step.seq))
		f.writeBatch(withUnits(batchOf(step.rel, step.seq, versionOf(retiredX, step.hash, unit)),
			codeversion.CodeUnitVersion{UnitID: retiredUnit, Checksum: "u-1", Source: "macro"}))
	}
	require.Equal(t, "x-2", f.current(retiredX))
	return f
}

// retire promotes r3, which drops table_x, and checks the swap kept the node
// and its :CURRENT edge.
func (f *retiredNodeFixture) retire() {
	f.t.Helper()
	f.promoteAt("r3", 3, releaseOf(modelNode(retiredUp)), promotedAtOf(3))
	require.Equal(f.t, false, f.node(retiredX)["active"], "r3 retires table_x")
	require.Equal(f.t, "x-2", f.current(retiredX), "a retired node keeps its :CURRENT as the record of its last version")
}

// currentHashes returns the content_hash of every version GetNodeVersions
// lists for table_x, newest first, and of those it marks current.
func (f *retiredNodeFixture) currentHashes(currentOnly bool) (listed, current []string) {
	f.t.Helper()
	resp, err := f.handler.GetNodeVersions(f.ctx, &catalogv1.GetNodeVersionsRequest{
		UniqueId: retiredX, Limit: 10, CurrentOnly: currentOnly,
	})
	require.NoError(f.t, err)
	listed, current = []string{}, []string{}
	for _, v := range resp.Versions {
		listed = append(listed, v.ContentHash)
		if v.IsCurrent {
			current = append(current, v.ContentHash)
		}
	}
	return listed, current
}

func TestGetNodeVersions_RetiredNodeHasNoCurrent(t *testing.T) {
	f := newRetiredNodeFixture(t)
	listed, current := f.currentHashes(false)
	require.Equal(t, []string{"x-2", "x-1"}, listed)
	require.Equal(t, []string{"x-2"}, current, "a live node's :CURRENT version is current")

	f.retire()

	listed, current = f.currentHashes(false)
	assert.Equal(t, []string{"x-2", "x-1"}, listed, "a retired node still lists every version")
	assert.Empty(t, current, "no version of a retired node is current")
}

func TestGetNodeVersions_CurrentOnlyEmptyForRetired(t *testing.T) {
	f := newRetiredNodeFixture(t)
	listed, _ := f.currentHashes(true)
	require.Equal(t, []string{"x-2"}, listed)

	f.retire()

	listed, _ = f.currentHashes(true)
	assert.Empty(t, listed, "current_only of a retired node is an empty list, not NOT_FOUND")
}

func TestGetNodeVersionDiff_RetiredNodeHasNoCurrent(t *testing.T) {
	f := newRetiredNodeFixture(t)
	diff := func() *catalogv1.VersionDiff {
		resp, err := f.handler.GetNodeVersionDiff(f.ctx, &catalogv1.GetNodeVersionDiffRequest{UniqueId: retiredX, FromSeq: 1, ToSeq: 2})
		require.NoError(t, err)
		return resp.Diff
	}
	require.True(t, diff().To.IsCurrent)

	f.retire()

	d := diff()
	assert.Equal(t, "x-2", d.To.ContentHash)
	assert.False(t, d.From.IsCurrent)
	assert.False(t, d.To.IsCurrent, "no version of a retired node is current")
}

func TestUnitsForNode_RetiredNodeHasNone(t *testing.T) {
	f := newRetiredNodeFixture(t)
	units, err := f.versions.UnitsForNode(f.ctx, f.tenant, retiredX)
	require.NoError(t, err)
	require.Equal(t, []string{retiredUnit}, units)

	f.retire()

	units, err = f.versions.UnitsForNode(f.ctx, f.tenant, retiredX)
	require.NoError(t, err)
	assert.Empty(t, units, "a retired node runs no shared code")

	resp, err := f.handler.GetCodeUnitVersions(f.ctx, &catalogv1.GetCodeUnitVersionsRequest{UniqueId: retiredX})
	require.NoError(t, err, "a retired node is known, so its units are an empty list, not NOT_FOUND")
	assert.Empty(t, resp.Versions)
}

func TestGetNode_IncludeInactiveReturnsDroppedNode(t *testing.T) {
	f := newRetiredNodeFixture(t)
	f.retire()

	resp, err := f.handler.GetNode(f.ctx, &catalogv1.GetNodeRequest{
		ServiceName: "svc", SchemaName: "analytics", TableName: retiredX, IncludeInactive: true,
	})
	require.NoError(t, err)
	assert.True(t, resp.Inactive)
	assert.Equal(t, "dbt-model", resp.NodeType)

	_, err = f.handler.GetNode(f.ctx, &catalogv1.GetNodeRequest{ServiceName: "svc", SchemaName: "analytics", TableName: retiredX})
	assert.Equal(t, codes.NotFound, status.Code(err), "without include_inactive a dropped node is NOT_FOUND")
}

func TestGetNodeLocation_RetiredNodeIsNotFound(t *testing.T) {
	f := newRetiredNodeFixture(t)
	_, err := f.handler.GetNodeLocation(f.ctx, &catalogv1.GetNodeLocationRequest{UniqueId: retiredX})
	require.NoError(t, err)

	f.retire()

	_, err = f.handler.GetNodeLocation(f.ctx, &catalogv1.GetNodeLocationRequest{UniqueId: retiredX})
	assert.Equal(t, codes.NotFound, status.Code(err), "a retired node has no source location")
	_, err = f.handler.GetNodeLocation(f.ctx, &catalogv1.GetNodeLocationRequest{UniqueId: retiredUp})
	assert.NoError(t, err, "a live node keeps its location")
}

// A precedent's resolving version is current only while its node is live.
func TestGetPrecedents_ResolvingVersionOfRetiredNodeIsNotCurrent(t *testing.T) {
	f := newRetiredNodeFixture(t)
	// Rejected between x-1 and x-2, so the rejection back-links to x-2.
	f.recordRejection(rejectionOf("r-failed", retiredX, "sig-retired", promotedAtOf(1).Add(30*time.Minute)))
	resolvingIsCurrent := func() bool {
		resp, err := f.handler.GetPrecedents(f.ctx, &catalogv1.GetPrecedentsRequest{Signature: "sig-retired"})
		require.NoError(t, err)
		require.Len(t, resp.Precedents, 1)
		require.NotNil(t, resp.Precedents[0].ResolvingVersion)
		require.Equal(t, "x-2", resp.Precedents[0].ResolvingVersion.ContentHash)
		return resp.Precedents[0].ResolvingVersion.IsCurrent
	}
	require.True(t, resolvingIsCurrent())

	f.retire()

	assert.False(t, resolvingIsCurrent(), "the resolving version of a retired node is not current")
}
