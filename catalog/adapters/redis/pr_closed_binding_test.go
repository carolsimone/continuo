package redis

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePRClosed_HappyPath(t *testing.T) {
	payload := `{
		"proposal_id": "prop-1",
		"release_id": "rel-1",
		"node_id": "analytics.revenue",
		"resolved_node_ids": ["analytics.revenue", "analytics.margin"],
		"service": "core",
		"pr_url": "https://github.com/org/repo/pull/42",
		"pr_number": 42,
		"outcome": "merged",
		"closed_at": "2026-08-14T11:30:00Z",
		"edits": [
			{"path": "models/revenue.sql", "target_node_id": "analytics.revenue", "amended": true, "diff": "@@ -1 +1 @@"},
			{"path": "models/margin.sql", "target_node_id": "analytics.margin", "amended": false}
		]
	}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}

	evt, err := ParsePRClosed(msg)
	require.NoError(t, err)
	assert.Equal(t, "prop-1", evt.ProposalID)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Equal(t, "analytics.revenue", evt.NodeID)
	assert.Equal(t, []string{"analytics.revenue", "analytics.margin"}, evt.ResolvedNodeIDs)
	assert.Equal(t, "core", evt.Service)
	assert.Equal(t, "https://github.com/org/repo/pull/42", evt.PrURL)
	assert.Equal(t, 42, evt.PrNumber)
	assert.Equal(t, "merged", evt.Outcome)
	assert.Equal(t, "2026-08-14T11:30:00Z", evt.ClosedAt)
	require.Len(t, evt.Edits, 2)
	assert.Equal(t, "models/revenue.sql", evt.Edits[0].Path)
	assert.Equal(t, "analytics.revenue", evt.Edits[0].TargetNodeID)
	assert.True(t, evt.Edits[0].Amended)
	assert.Equal(t, "@@ -1 +1 @@", evt.Edits[0].Diff)
	assert.False(t, evt.Edits[1].Amended)
}

func TestParsePRClosed_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := ParsePRClosed(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePRClosed_EmptyPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": ""}}
	_, err := ParsePRClosed(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePRClosed_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json"}}
	_, err := ParsePRClosed(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
