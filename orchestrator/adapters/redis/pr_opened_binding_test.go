package redis

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePROpened_HappyPath(t *testing.T) {
	payload := `{
		"proposal_id": "prop-1",
		"release_id": "rel-1",
		"node_id": "analytics.revenue",
		"pr_url": "https://github.com/org/repo/pull/42",
		"pr_number": 42,
		"opened_by": "agent-remediation",
		"opened_at": "2026-08-12T09:05:00Z"
	}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}

	evt, err := ParsePROpened(msg)
	require.NoError(t, err)
	assert.Equal(t, "prop-1", evt.ProposalID)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Equal(t, "analytics.revenue", evt.NodeID)
	assert.Equal(t, "https://github.com/org/repo/pull/42", evt.PrURL)
	assert.Equal(t, 42, evt.PrNumber)
	assert.Equal(t, "agent-remediation", evt.OpenedBy)
	assert.Equal(t, "2026-08-12T09:05:00Z", evt.OpenedAt)
}

func TestParsePROpened_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := ParsePROpened(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePROpened_EmptyPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": ""}}
	_, err := ParsePROpened(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePROpened_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json"}}
	_, err := ParsePROpened(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
