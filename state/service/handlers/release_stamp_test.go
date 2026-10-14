package handlers

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orderedTopology records the order of the topology calls liveStamp makes.
type orderedTopology struct {
	uow.LiveReleaseOnly
	calls *[]string
}

func (o *orderedTopology) LockTopology(ctx context.Context) error {
	*o.calls = append(*o.calls, "lock")
	return o.LiveReleaseOnly.LockTopology(ctx)
}

func (o *orderedTopology) LivePointer(ctx context.Context, tenant string) (topology.Pointer, error) {
	*o.calls = append(*o.calls, "live")
	return o.LiveReleaseOnly.LivePointer(ctx, tenant)
}

// The topology lock is taken before the live release is read, so the release a
// run names cannot be pruned before the run commits.
func TestLiveStamp_LocksTheTopologyBeforeReadingTheLiveRelease(t *testing.T) {
	var calls []string
	topo := &orderedTopology{
		LiveReleaseOnly: uow.LiveReleaseOnly{Live: topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}},
		calls:           &calls,
	}
	u := &uow.FakeUnitOfWork{TopologyRepo: topo}

	got, err := liveStamp(context.Background(), u)

	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, got)
	assert.Equal(t, []string{"lock", "live"}, calls)
}
