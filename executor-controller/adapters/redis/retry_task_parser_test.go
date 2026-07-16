// executor-controller/adapters/redis/retry_task_parser_test.go
package redis

import (
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRetryTask_HappyPath(t *testing.T) {
	taskID := uuid.New()
	scheduleID := uuid.New()
	msg := goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"task_id":          taskID.String(),
		"schedule_id":      scheduleID.String(),
		"schedule_name":    "hourly",
		"service_name":     "svc",
		"schema_name":      "s",
		"table_name":       "t",
		"job_name":         "j",
		"node_type":        "dbt-model",
		"image_tag":        "sha-xyz",
		"task_retry_count": "2",
		"max_retries":      "5",
	}}
	evt, err := ParseRetryTask(msg)
	require.NoError(t, err)
	assert.Equal(t, taskID, evt.TaskID)
	assert.Equal(t, pkg_model.NodeTypeDbtModel, evt.NodeType)
	assert.Equal(t, 2, evt.TaskRetryCount)
	assert.Equal(t, 5, evt.MaxRetries)
}

func TestParseRetryTask_MissingTaskID(t *testing.T) {
	_, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"schedule_id":      uuid.New().String(),
		"node_type":        "dbt-model",
		"task_retry_count": "1",
		"max_retries":      "3",
	}})
	require.Error(t, err)
}

func TestParseRetryTask_DefaultsZeroWhenFieldsAbsent(t *testing.T) {
	evt, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.NoError(t, err)
	assert.Equal(t, 0, evt.TaskRetryCount)
	assert.Equal(t, 0, evt.MaxRetries)
}

// TestParseRetryTask_OperationThreadsThrough guards the false-green retry bug:
// ParseRetryTask delegates the shared fields to ParseQueryModel, which must
// carry "operation" through so a retried `dbt test` Job's deployment command
// stays `dbt test` instead of defaulting to `dbt run`.
func TestParseRetryTask_OperationThreadsThrough(t *testing.T) {
	evt, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
		"operation":   "test",
	}})
	require.NoError(t, err)
	assert.Equal(t, pkg_model.OperationTest, evt.Operation)
}

// TestParseRetryTask_ExecutorDeploymentIDParses pins the same-row retry pointer:
// a worker task's retry names the deployment row it re-attempts instead of
// enqueueing a second one.
func TestParseRetryTask_ExecutorDeploymentIDParses(t *testing.T) {
	deploymentID := uuid.New()
	evt, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"task_id":                uuid.New().String(),
		"schedule_id":            uuid.New().String(),
		"node_type":              "dbt-model",
		"executor_deployment_id": deploymentID.String(),
	}})
	require.NoError(t, err)
	assert.Equal(t, deploymentID, evt.ExecutorDeploymentID)
}

// TestParseRetryTask_ExecutorDeploymentIDAbsentIsNilUUID pins that a Jobs-path
// retry, which carries no pointer, keeps enqueueing a fresh deployment.
func TestParseRetryTask_ExecutorDeploymentIDAbsentIsNilUUID(t *testing.T) {
	evt, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"node_type":   "dbt-model",
	}})
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, evt.ExecutorDeploymentID)
}

func TestParseRetryTask_ExecutorDeploymentIDInvalidIsPermanentError(t *testing.T) {
	_, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"task_id":                uuid.New().String(),
		"schedule_id":            uuid.New().String(),
		"node_type":              "dbt-model",
		"executor_deployment_id": "not-a-uuid",
	}})
	require.Error(t, err)
}

// TestParseRetryTask_RuntimeManifestPinThreadsThrough pins that a retried worker
// task keeps the pin its original dispatch carried, so it re-runs against the
// same artifact.
func TestParseRetryTask_RuntimeManifestPinThreadsThrough(t *testing.T) {
	values := map[string]interface{}{
		"task_id":       uuid.New().String(),
		"schedule_id":   uuid.New().String(),
		"node_type":     "dbt-model",
		"dbt_unique_id": "model.finance.orders",
	}
	for k, v := range runtimeManifestFields() {
		values[k] = v
	}
	evt, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: values})
	require.NoError(t, err)
	assert.Equal(t, "model.finance.orders", evt.DBTUniqueID)
	assert.True(t, evt.Complete())
}

func TestParseRetryTask_NonNumericRetryCount(t *testing.T) {
	_, err := ParseRetryTask(goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"task_id":          uuid.New().String(),
		"schedule_id":      uuid.New().String(),
		"node_type":        "dbt-model",
		"task_retry_count": "not-a-number",
	}})
	require.Error(t, err)
}
