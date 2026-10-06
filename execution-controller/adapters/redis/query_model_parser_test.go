// execution-controller/adapters/redis/query_model_parser_test.go
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
	outboxEntryID := uuid.New()
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"outbox_entry_id": outboxEntryID.String(),
		"task_id":         taskID.String(),
		"schedule_id":     scheduleID.String(),
		"schedule_name":   "daily",
		"service_name":    "dbt",
		"schema_name":     "public",
		"table_name":      "orders",
		"job_name":        "dbt-public-orders",
		"node_type":       "dbt-model",
		"image_tag":       "sha-abc",
	}}
	evt, err := ParseQueryModel(msg)
	require.NoError(t, err)
	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
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

func TestParseQueryModel_OutboxEntryIDAbsentIsNilUUID(t *testing.T) {
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID,
		"absent outbox_entry_id is uuid.Nil so dedup degrades to (msg.ID, stream_name)")
}

func TestParseQueryModel_OutboxEntryIDInvalidIsPermanentError(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"outbox_entry_id": "not-a-uuid",
		"task_id":         uuid.New().String(),
		"schedule_id":     uuid.New().String(),
		"node_type":       "dbt-model",
	}})
	require.Error(t, err)
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

func TestParseQueryModel_OperationTestParses(t *testing.T) {
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
		"operation":   "test",
	}})
	require.NoError(t, err)
	assert.Equal(t, pkg_model.OperationTest, evt.Operation)
}

func TestParseQueryModel_OperationAbsentDefaultsToRun(t *testing.T) {
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.NoError(t, err)
	assert.Equal(t, pkg_model.OperationRun, evt.Operation)
}

func TestParseQueryModel_InvalidOperationIsPermanentError(t *testing.T) {
	_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
		"operation":   "no_such_operation",
	}})
	require.Error(t, err)
}

func TestParseQueryModel_SecretRef(t *testing.T) {
	base := map[string]interface{}{
		"outbox_entry_id": uuid.New().String(),
		"task_id":         uuid.New().String(),
		"schedule_id":     uuid.New().String(),
		"schedule_name":   "daily",
		"service_name":    "svc",
		"schema_name":     "analytics",
		"table_name":      "fx",
		"job_name":        "j",
		"node_type":       "python-api",
		"image_tag":       "ghcr.io/acme/py:1",
	}
	withRef := map[string]interface{}{"secret_ref": "continuo-api-fx"}
	for k, v := range base {
		withRef[k] = v
	}
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: withRef})
	require.NoError(t, err)
	assert.Equal(t, "continuo-api-fx", evt.SecretRef)

	evt, err = ParseQueryModel(goredis.XMessage{ID: "1-0", Values: base})
	require.NoError(t, err)
	assert.Equal(t, "", evt.SecretRef)
}

func TestParseQueryModel_MaxRetries(t *testing.T) {
	base := map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}
	evt, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: base})
	require.NoError(t, err)
	assert.Equal(t, int32(0), evt.MaxRetries, "absent max_retries leaves the budget unset")

	withBudget := map[string]interface{}{"max_retries": "5"}
	for k, v := range base {
		withBudget[k] = v
	}
	evt, err = ParseQueryModel(goredis.XMessage{ID: "1-0", Values: withBudget})
	require.NoError(t, err)
	assert.Equal(t, int32(5), evt.MaxRetries)
}

func TestParseQueryModel_InvalidMaxRetriesIsPermanentError(t *testing.T) {
	for _, bad := range []string{"two", "-1", "99999999999"} {
		values := map[string]interface{}{
			"task_id":     uuid.New().String(),
			"schedule_id": uuid.New().String(),
			"node_type":   "dbt-model",
			"max_retries": bad,
		}
		_, err := ParseQueryModel(goredis.XMessage{ID: "1-0", Values: values})
		require.Error(t, err, "max_retries=%q", bad)
	}
}
