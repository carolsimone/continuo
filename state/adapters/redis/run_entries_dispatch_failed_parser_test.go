package redis

import (
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRunEntriesDispatchFailed_HappyPath(t *testing.T) {
	scheduleID := uuid.New()
	payload := `{"schedule_id":"` + scheduleID.String() + `","schedule_name":"sched","reason":"target_not_found"}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}
	evt, err := ParseRunEntriesDispatchFailed(msg)
	require.NoError(t, err)
	assert.Equal(t, scheduleID, evt.ScheduleID)
	assert.Equal(t, "sched", evt.ScheduleName)
	assert.Equal(t, "target_not_found", string(evt.Reason))
}

func TestParseRunEntriesDispatchFailed_MissingPayload(t *testing.T) {
	_, err := ParseRunEntriesDispatchFailed(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	require.Error(t, err)
}

func TestParseRunEntriesDispatchFailed_BadScheduleID(t *testing.T) {
	_, err := ParseRunEntriesDispatchFailed(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": `{"schedule_id":"x"}`}})
	require.Error(t, err)
}

func TestParseRunEntriesDispatchFailed_NoTestsIsBenign(t *testing.T) {
	scheduleID := uuid.New()
	payload := `{"schedule_id":"` + scheduleID.String() + `","schedule_name":"sched","reason":"no_tests"}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}
	evt, err := ParseRunEntriesDispatchFailed(msg)
	require.NoError(t, err)
	assert.True(t, evt.Benign, "no_tests must be classified benign")
}

func TestParseRunEntriesDispatchFailed_EmptyProjectionNotBenign(t *testing.T) {
	scheduleID := uuid.New()
	payload := `{"schedule_id":"` + scheduleID.String() + `","schedule_name":"sched","reason":"empty_projection"}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}
	evt, err := ParseRunEntriesDispatchFailed(msg)
	require.NoError(t, err)
	assert.False(t, evt.Benign, "empty_projection must NOT be benign")
}
