package redis_test

import (
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nodeUpdatedValues(taskID, scheduleID uuid.UUID) map[string]interface{} {
	return map[string]interface{}{
		"task_id":       taskID.String(),
		"schedule_id":   scheduleID.String(),
		"schedule_name": "daily_sales",
		"service_name":  "service-1",
		"schema_name":   "analytics",
		"table_name":    "orders",
		"status":        "success",
	}
}

func TestParseNodeCompleted_Valid(t *testing.T) {
	taskID := uuid.New()
	scheduleID := uuid.New()
	cmd, err := redis.ParseNodeCompleted(goredis.XMessage{ID: "1-0", Values: nodeUpdatedValues(taskID, scheduleID)})
	require.NoError(t, err)
	assert.Equal(t, taskID, cmd.TaskID)
	assert.Equal(t, scheduleID, cmd.ScheduleID)
	assert.Equal(t, "daily_sales", cmd.ScheduleName)
	assert.Equal(t, "service-1", cmd.ServiceName)
	assert.Equal(t, "analytics", cmd.SchemaName)
	assert.Equal(t, "orders", cmd.TableName)
	assert.Equal(t, "success", cmd.Status)
}

func TestParseNodeCompleted_MissingFieldIsPermanent(t *testing.T) {
	for _, missing := range []string{
		"task_id", "schedule_id", "schedule_name",
		"service_name", "schema_name", "table_name", "status",
	} {
		t.Run("missing_"+missing, func(t *testing.T) {
			values := nodeUpdatedValues(uuid.New(), uuid.New())
			delete(values, missing)
			_, err := redis.ParseNodeCompleted(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.True(t, errors.Is(err, events.ErrPermanent),
				"missing %s must yield a permanent error", missing)
		})
	}
}

func TestParseNodeCompleted_NonStringFieldIsPermanent(t *testing.T) {
	values := nodeUpdatedValues(uuid.New(), uuid.New())
	values["status"] = 42 // not a string — would panic under a type assertion
	_, err := redis.ParseNodeCompleted(goredis.XMessage{ID: "1-0", Values: values})
	require.Error(t, err)
	assert.True(t, errors.Is(err, events.ErrPermanent))
}

func TestParseNodeCompleted_MalformedUUIDIsPermanent(t *testing.T) {
	for _, field := range []string{"task_id", "schedule_id"} {
		t.Run(field, func(t *testing.T) {
			values := nodeUpdatedValues(uuid.New(), uuid.New())
			values[field] = "not-a-uuid"
			_, err := redis.ParseNodeCompleted(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.True(t, errors.Is(err, events.ErrPermanent))
		})
	}
}
