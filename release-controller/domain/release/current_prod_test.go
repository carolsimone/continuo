package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var prodRef = release.TopologyRef{URI: "s3://b/tenants/default/topologies/r1/topology.json.gz", SHA256: "abc", NodeCount: 3}

func TestCurrentProd_NewIsEmpty(t *testing.T) {
	cp := release.NewCurrentProd()
	assert.Empty(t, cp.ReleaseID())
	assert.True(t, cp.Topology().IsZero())
	assert.Equal(t, int64(0), cp.PromotionSeq())
}

func TestCurrentProd_Update(t *testing.T) {
	cp := release.NewCurrentProd()
	require.NoError(t, cp.Update("r1", prodRef, 4, time.Unix(10, 0)))
	assert.Equal(t, "r1", cp.ReleaseID())
	assert.Equal(t, prodRef, cp.Topology())
	assert.Equal(t, int64(4), cp.PromotionSeq())
	assert.Equal(t, time.Unix(10, 0).UTC(), cp.UpdatedAt().UTC())
}

// The promotion seq only moves forward: a seq at or below the recorded one is
// refused and leaves current_prod as it was.
func TestCurrentProd_UpdateRefusesASeqThatDoesNotMoveForward(t *testing.T) {
	cp := release.RehydrateCurrentProd("r1", prodRef, 5, time.Unix(10, 0))
	for _, seq := range []int64{5, 4} {
		require.Error(t, cp.Update("r2", release.TopologyRef{URI: "other"}, seq, time.Unix(20, 0)), "seq %d", seq)
	}
	assert.Equal(t, "r1", cp.ReleaseID())
	assert.Equal(t, prodRef, cp.Topology())
	assert.Equal(t, int64(5), cp.PromotionSeq())
}

// A re-announcement records a fresh seq and nothing else: the release, its
// topology and the time current_prod last moved stay as they were.
func TestCurrentProd_ReannounceRecordsTheSeqOnly(t *testing.T) {
	cp := release.RehydrateCurrentProd("r1", prodRef, 5, time.Unix(10, 0))
	require.NoError(t, cp.Reannounce(9))
	assert.Equal(t, "r1", cp.ReleaseID())
	assert.Equal(t, prodRef, cp.Topology())
	assert.Equal(t, int64(9), cp.PromotionSeq())
	assert.Equal(t, time.Unix(10, 0), cp.UpdatedAt())
	assert.Error(t, cp.Reannounce(9), "the same seq twice is refused")
}

func TestCurrentProd_RecordArtifact(t *testing.T) {
	cp := release.RehydrateCurrentProd("r1", release.TopologyRef{}, 0, time.Unix(10, 0))
	cp.RecordArtifact(prodRef)
	assert.Equal(t, prodRef, cp.Topology())
	assert.Equal(t, "r1", cp.ReleaseID())
}
