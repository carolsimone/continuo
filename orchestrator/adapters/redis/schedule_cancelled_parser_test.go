package redis

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseScheduleCancelled_HappyPath(t *testing.T) {
	id := uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id":   id.String(),
		"schedule_name": "test-schedule",
	}}
	evt, err := ParseScheduleCancelled(msg)
	require.NoError(t, err)
	assert.Equal(t, id, evt.ScheduleID)
}

func TestParseScheduleCancelled_CarriesCancelledBy(t *testing.T) {
	id := uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id":  id.String(),
		"cancelled_by": "okta|alice",
	}}
	evt, err := ParseScheduleCancelled(msg)
	require.NoError(t, err)
	assert.Equal(t, "okta|alice", evt.CancelledBy)
}

func TestParseScheduleCancelled_MissingCancelledByBecomesSystem(t *testing.T) {
	id := uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id": id.String(),
	}}
	evt, err := ParseScheduleCancelled(msg)
	require.NoError(t, err)
	assert.Equal(t, "system", evt.CancelledBy)
}

func TestParseScheduleCancelled_MissingScheduleID(t *testing.T) {
	_, err := ParseScheduleCancelled(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseScheduleCancelled_BadUUID(t *testing.T) {
	_, err := ParseScheduleCancelled(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"schedule_id": "not-a-uuid"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
