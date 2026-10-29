package redis

import (
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func releasePromotedMessage(t *testing.T, tenantID string, p pkgevents.ReleasePromoted) goredis.XMessage {
	t.Helper()
	fields, err := pkgevents.ReleasePromotedFields(tenantID, "release-controller", time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC), p)
	require.NoError(t, err)
	return goredis.XMessage{ID: "1-0", Values: fields}
}

func TestParseReleasePromoted_ReadsTheEnvelopeAndThePayload(t *testing.T) {
	msg := releasePromotedMessage(t, "default", pkgevents.ReleasePromoted{
		ReleaseID: "rel-1", PromotionSeq: 4, TopologyURI: "s3://continuo/k", TopologySHA256: "abc",
		ChangedNodeIDs: []string{"raw.orders"}, Bootstrap: true,
	})

	evt, err := ParseReleasePromoted(msg)
	require.NoError(t, err)
	assert.Equal(t, events.ReleasePromoted{
		TenantID: "default", ReleaseID: "rel-1", PromotionSeq: 4, TopologyURI: "s3://continuo/k",
		TopologySHA256: "abc", ChangedNodeIDs: []string{"raw.orders"}, Bootstrap: true,
	}, evt)
}

// An entry state cannot key or read is dead-lettered.
func TestParseReleasePromoted_RejectsAnUnreadableEntryAsPermanent(t *testing.T) {
	cases := map[string]goredis.XMessage{
		"no envelope": {ID: "1-0", Values: map[string]interface{}{"e2e_dead_letter_probe": "x"}},
		"no tenant": releasePromotedMessage(t, "", pkgevents.ReleasePromoted{
			ReleaseID: "rel-1", PromotionSeq: 1, TopologyURI: "s3://continuo/k", TopologySHA256: "abc"}),
		"no promotion seq": releasePromotedMessage(t, "default", pkgevents.ReleasePromoted{
			ReleaseID: "rel-1", TopologyURI: "s3://continuo/k", TopologySHA256: "abc"}),
		"no artifact": releasePromotedMessage(t, "default", pkgevents.ReleasePromoted{ReleaseID: "rel-1", PromotionSeq: 1}),
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseReleasePromoted(msg)
			assert.ErrorIs(t, err, pkgevents.ErrPermanent)
		})
	}
}
