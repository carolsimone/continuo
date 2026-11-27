package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var prodRef = release.TopologyRef{URI: "s3://b/tenants/default/topologies/r1/topology.json.gz", SHA256: "abc", NodeCount: 3}

func TestLiveRelease_NewIsEmpty(t *testing.T) {
	live := release.NewLiveRelease()
	assert.Empty(t, live.ReleaseID())
	assert.True(t, live.Topology().IsZero())
	assert.Equal(t, int64(0), live.PromotionSeq())
}

func TestLiveRelease_Update(t *testing.T) {
	live := release.NewLiveRelease()
	require.NoError(t, live.Update("r1", prodRef, 4, time.Unix(10, 0)))
	assert.Equal(t, "r1", live.ReleaseID())
	assert.Equal(t, prodRef, live.Topology())
	assert.Equal(t, int64(4), live.PromotionSeq())
	assert.Equal(t, time.Unix(10, 0).UTC(), live.UpdatedAt().UTC())
}

// The promotion seq only moves forward: a seq at or below the recorded one is
// refused and leaves the live release as it was.
func TestLiveRelease_UpdateRefusesASeqThatDoesNotMoveForward(t *testing.T) {
	live := release.RehydrateLiveRelease("r1", prodRef, 5, time.Unix(10, 0))
	for _, seq := range []int64{5, 4} {
		require.Error(t, live.Update("r2", release.TopologyRef{URI: "other"}, seq, time.Unix(20, 0)), "seq %d", seq)
	}
	assert.Equal(t, "r1", live.ReleaseID())
	assert.Equal(t, prodRef, live.Topology())
	assert.Equal(t, int64(5), live.PromotionSeq())
}

// A re-announcement records a fresh seq and nothing else: the release, its
// topology and the time the live release last moved stay as they were.
func TestLiveRelease_ReannounceRecordsTheSeqOnly(t *testing.T) {
	live := release.RehydrateLiveRelease("r1", prodRef, 5, time.Unix(10, 0))
	require.NoError(t, live.Reannounce(9))
	assert.Equal(t, "r1", live.ReleaseID())
	assert.Equal(t, prodRef, live.Topology())
	assert.Equal(t, int64(9), live.PromotionSeq())
	assert.Equal(t, time.Unix(10, 0), live.UpdatedAt())
	assert.Error(t, live.Reannounce(9), "the same seq twice is refused")
}
