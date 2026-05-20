package event_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/stretchr/testify/assert"
)

func TestNodeUpdated_ToMap(t *testing.T) {
	e := event.NodeUpdated{
		TaskID:       "t1",
		ScheduleID:   "s1",
		ScheduleName: "daily",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "orders",
		Status:       "FAILED",
	}
	m := e.ToMap()
	assert.Equal(t, "t1", m["task_id"])
	assert.Equal(t, "s1", m["schedule_id"])
	assert.Equal(t, "daily", m["schedule_name"])
	assert.Equal(t, "dbt", m["service_name"])
	assert.Equal(t, "public", m["schema_name"])
	assert.Equal(t, "orders", m["table_name"])
	assert.Equal(t, "FAILED", m["status"])
}
