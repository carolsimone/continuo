package redis

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func releasePromotedV2Message(t *testing.T, p events.ReleasePromoted) goredis.XMessage {
	t.Helper()
	fields, err := events.ReleasePromotedFields("default", "release-controller", time.Now().UTC(), p)
	require.NoError(t, err)
	fields["outbox_entry_id"] = "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10"
	return goredis.XMessage{ID: "1-0", Values: fields}
}

func TestParseReleasePromotedV2_MapsThePayload(t *testing.T) {
	promotedAt := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	in, err := ParseReleasePromotedV2(releasePromotedV2Message(t, events.ReleasePromoted{
		ReleaseID: "rel-7", PromotedAt: promotedAt, PromotionSeq: 7,
		TopologyURI:    "s3://continuo/tenants/default/topologies/rel-7/topology.json.gz",
		TopologySHA256: "ab12", ChangedNodeIDs: []string{"analytics.orders"},
		CandidateSchema: "candidate_rel_7", CodeBundleURI: "s3://continuo/code-bundles/rel-7/bundle.json",
		Repo: "org/svc", CommitSHA: "deadbeef", Bootstrap: true,
	}))
	require.NoError(t, err)
	assert.Equal(t, "rel-7", in.ReleaseID)
	assert.Equal(t, int64(7), in.PromotionSeq)
	assert.Equal(t, "s3://continuo/tenants/default/topologies/rel-7/topology.json.gz", in.TopologyURI)
	assert.Equal(t, "ab12", in.TopologySHA256)
	assert.Equal(t, []string{"analytics.orders"}, in.ChangedNodeIDs)
	assert.Equal(t, "s3://continuo/code-bundles/rel-7/bundle.json", in.CodeBundleURI)
	assert.Equal(t, "org/svc", in.Repo)
	assert.Equal(t, "deadbeef", in.CommitSHA)
	assert.True(t, in.PromotedAt.Equal(promotedAt))
	assert.True(t, in.Bootstrap)
}

func TestParseReleasePromotedV2_MissingPayloadIsPermanent(t *testing.T) {
	_, err := ParseReleasePromotedV2(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"schema_version": "1"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseReleasePromotedV2_UnknownSchemaVersionIsPermanent(t *testing.T) {
	msg := releasePromotedV2Message(t, events.ReleasePromoted{
		ReleaseID: "rel-7", PromotionSeq: 7, TopologyURI: "s3://b/k", TopologySHA256: "ab",
	})
	msg.Values["schema_version"] = "99"
	_, err := ParseReleasePromotedV2(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseReleasePromotedV2_SeqBelowOneIsPermanent(t *testing.T) {
	_, err := ParseReleasePromotedV2(releasePromotedV2Message(t, events.ReleasePromoted{
		ReleaseID: "rel-7", PromotionSeq: 0, TopologyURI: "s3://b/k", TopologySHA256: "ab",
	}))
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
