// executor-controller/adapters/redis/query_model_parser_test.go
package redis

import (
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQueryModel_HappyPath(t *testing.T) {
	taskID := uuid.New()
	scheduleID := uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":       taskID.String(),
		"schedule_id":   scheduleID.String(),
		"schedule_name": "daily",
		"service_name":  "dbt",
		"schema_name":   "public",
		"table_name":    "orders",
		"job_name":      "dbt-public-orders",
		"node_type":     "dbt-model",
		"image_tag":     "sha-abc",
	}}
	evt, err := ParseQueryModel(msg)
	require.NoError(t, err)
	assert.Equal(t, taskID, evt.TaskID)
	assert.Equal(t, scheduleID, evt.ScheduleID)
	assert.Equal(t, "daily", evt.ScheduleName)
	assert.Equal(t, "dbt", evt.ServiceName)
	assert.Equal(t, "public", evt.SchemaName)
	assert.Equal(t, "orders", evt.TableName)
	assert.Equal(t, "dbt-public-orders", evt.JobName)
	assert.Equal(t, pkg_model.NodeTypeDbtModel, evt.NodeType)
	assert.Equal(t, "sha-abc", evt.ImageTag)
}

func TestParseQueryModel_MissingTaskID(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.Error(t, err)
}

func TestParseQueryModel_InvalidScheduleID(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": "not-a-uuid",
		"node_type":   "dbt-model",
	}})
	require.Error(t, err)
}

func TestParseQueryModel_UnknownNodeType(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "no_such_type",
	}})
	require.Error(t, err)
}
