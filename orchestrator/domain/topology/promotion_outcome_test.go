package topology_test

import (
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/stretchr/testify/assert"
)

func TestDecidePromotion(t *testing.T) {
	for _, tc := range []struct {
		name        string
		liveSeq     int64
		liveRelease string
		seq         int64
		release     string
		wantOutcome topology.PromotionOutcome
	}{
		{"first promotion on an empty graph", 0, "", 1, "r1", topology.PromotionApplied},
		{"a graph from before promotion seqs takes the first one", 0, "legacy", 1, "legacy", topology.PromotionApplied},
		{"newer seq", 4, "r4", 5, "r5", topology.PromotionApplied},
		{"newer seq re-announcing the live release", 4, "r4", 5, "r4", topology.PromotionApplied},
		{"same seq and release is a redelivery", 5, "r5", 5, "r5", topology.PromotionRedelivered},
		{"older seq is stale", 5, "r5", 4, "r4", topology.PromotionStale},
		{"same seq for another release is stale", 5, "r5", 5, "r9", topology.PromotionStale},
		{"seq zero never applies over an empty graph", 0, "", 0, "r1", topology.PromotionStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantOutcome, topology.DecidePromotion(tc.liveSeq, tc.liveRelease, tc.seq, tc.release))
		})
	}
}
