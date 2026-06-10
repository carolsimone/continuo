package redis

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRunFinalized_HappyPath(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id": "abc-123",
		"status":      "success",
	}}
	evt, err := ParseRunFinalized(msg)
	require.NoError(t, err)
	assert.Equal(t, "abc-123", evt.ScheduleID)
	assert.Equal(t, "success", evt.Status)
}

func TestParseRunFinalized_MissingScheduleID(t *testing.T) {
	_, err := ParseRunFinalized(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"status": "success",
	}})
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseRunFinalized_MissingStatus(t *testing.T) {
	_, err := ParseRunFinalized(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id": "abc-123",
	}})
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
