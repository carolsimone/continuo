package redis

import (
	"testing"

	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRunEntriesDispatched_HappyPath(t *testing.T) {
	scheduleID := uuid.New()
	taskID := uuid.New()
	payload := `{
		"schedule_id": "` + scheduleID.String() + `",
		"total_task_count": 1,
		"all_tasks": [{
			"task_id": "` + taskID.String() + `",
			"service_name": "svc",
			"schema_name": "sch",
			"table_name": "t",
			"status": "pending",
			"max_retries": 3,
			"manifest_version": "m1",
			"image_tag": "v1"
		}]
	}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}
	evt, err := ParseRunEntriesDispatched(msg)
	require.NoError(t, err)
	assert.Equal(t, scheduleID, evt.ScheduleID)
	assert.Equal(t, int32(1), evt.TotalTaskCount)
	require.Len(t, evt.AllTasks, 1)
	assert.Equal(t, taskID, evt.AllTasks[0].TaskID)
	assert.Equal(t, model.TaskStatusPending, evt.AllTasks[0].Status)
	assert.Nil(t, evt.AllTasks[0].InheritedFromTaskID)
}

func TestParseRunEntriesDispatched_InheritedTask(t *testing.T) {
	scheduleID := uuid.New()
	taskID := uuid.New()
	inheritedFrom := uuid.New()
	payload := `{
		"schedule_id": "` + scheduleID.String() + `",
		"total_task_count": 1,
		"all_tasks": [{
			"task_id": "` + taskID.String() + `",
			"service_name": "s", "schema_name": "s", "table_name": "t",
			"status": "succeeded",
			"max_retries": 3,
			"inherited_from_task_id": "` + inheritedFrom.String() + `"
		}]
	}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}
	evt, err := ParseRunEntriesDispatched(msg)
	require.NoError(t, err)
	require.NotNil(t, evt.AllTasks[0].InheritedFromTaskID)
	assert.Equal(t, inheritedFrom, *evt.AllTasks[0].InheritedFromTaskID)
	assert.Equal(t, model.TaskStatusSucceeded, evt.AllTasks[0].Status)
}

func TestParseRunEntriesDispatched_MissingPayload(t *testing.T) {
	_, err := ParseRunEntriesDispatched(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	require.Error(t, err)
}

func TestParseRunEntriesDispatched_BadScheduleID(t *testing.T) {
	payload := `{"schedule_id":"not-a-uuid","total_task_count":0,"all_tasks":[]}`
	_, err := ParseRunEntriesDispatched(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}})
	require.Error(t, err)
}

func TestParseRunEntriesDispatched_BadTaskStatus(t *testing.T) {
	payload := `{"schedule_id":"` + uuid.New().String() + `","total_task_count":1,"all_tasks":[{"task_id":"` + uuid.New().String() + `","service_name":"s","schema_name":"s","table_name":"t","status":"bogus","max_retries":1}]}`
	_, err := ParseRunEntriesDispatched(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}})
	require.Error(t, err)
}
