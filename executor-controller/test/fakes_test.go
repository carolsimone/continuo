package test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/test/fakes"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFakeK8sClient_CreateQueryJob tests the fake K8s client
func TestFakeK8sClient_CreateQueryJob(t *testing.T) {
	ctx := context.Background()
	fakeK8s := fakes.NewFakeK8sClient()

	params := k8s.JobParams{
		JobName:     "test-job",
		TaskID:      uuid.New().String(),
		ScheduleID:  uuid.New().String(),
		ServiceName: "dbt",
		SchemaName:  "public",
		TableName:   "users",
		Namespace:   "default",
	}

	// First call should create job
	err := fakeK8s.CreateQueryJob(ctx, params)
	require.NoError(t, err)

	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 1)
	assert.Contains(t, jobs, "default/test-job")

	// Second call should be idempotent
	err = fakeK8s.CreateQueryJob(ctx, params)
	require.NoError(t, err)

	jobs = fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 1, "Should still have only one job (idempotent)")
}

// TestFakeK8sClient_CreateJobError tests error handling
func TestFakeK8sClient_CreateJobError(t *testing.T) {
	ctx := context.Background()
	fakeK8s := fakes.NewFakeK8sClient()

	expectedErr := errors.New("k8s error")
	fakeK8s.SetCreateJobError(expectedErr)

	params := k8s.JobParams{
		JobName:   "test-job",
		Namespace: "default",
	}

	err := fakeK8s.CreateQueryJob(ctx, params)
	assert.Equal(t, expectedErr, err)

	// No jobs should be created
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 0)
}

// TestFakeStateClient_UpdateTaskStatus tests the fake state client
func TestFakeStateClient_UpdateTaskStatus(t *testing.T) {
	ctx := context.Background()
	fakeState := fakes.NewFakeStateClient()

	taskID := uuid.New()

	// Update task status
	err := fakeState.UpdateTaskStatus(ctx, taskID, statev1.TaskStatus_TASK_STATUS_RUNNING)
	require.NoError(t, err)

	// Verify update was recorded
	updates := fakeState.GetTaskUpdates()
	require.Len(t, updates, 1)
	assert.Equal(t, taskID, updates[0].TaskID)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_RUNNING, updates[0].Status)

	// Get last update
	status, err := fakeState.GetLastTaskUpdate(taskID)
	require.NoError(t, err)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_RUNNING, status)
}

// TestFakeStateClient_MultipleUpdates tests multiple status updates
func TestFakeStateClient_MultipleUpdates(t *testing.T) {
	ctx := context.Background()
	fakeState := fakes.NewFakeStateClient()

	taskID1 := uuid.New()
	taskID2 := uuid.New()

	// Update multiple tasks
	_ = fakeState.UpdateTaskStatus(ctx, taskID1, statev1.TaskStatus_TASK_STATUS_RUNNING)
	_ = fakeState.UpdateTaskStatus(ctx, taskID2, statev1.TaskStatus_TASK_STATUS_RUNNING)
	_ = fakeState.UpdateTaskStatus(ctx, taskID1, statev1.TaskStatus_TASK_STATUS_SUCCEEDED)

	// Verify all updates
	updates := fakeState.GetTaskUpdates()
	assert.Len(t, updates, 3)

	// Get last update for taskID1
	status, err := fakeState.GetLastTaskUpdate(taskID1)
	require.NoError(t, err)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_SUCCEEDED, status)
}

// TestFakeStateClient_Error tests error handling
func TestFakeStateClient_Error(t *testing.T) {
	ctx := context.Background()
	fakeState := fakes.NewFakeStateClient()

	expectedErr := errors.New("state service error")
	fakeState.SetUpdateTaskStatusError(expectedErr)

	taskID := uuid.New()
	err := fakeState.UpdateTaskStatus(ctx, taskID, statev1.TaskStatus_TASK_STATUS_RUNNING)
	assert.Equal(t, expectedErr, err)

	// No updates should be recorded
	updates := fakeState.GetTaskUpdates()
	assert.Len(t, updates, 0)
}

// TestFakeRedisProducer_Publish tests the fake Redis producer
func TestFakeRedisProducer_Publish(t *testing.T) {
	ctx := context.Background()
	fakeProducer := fakes.NewFakeRedisProducer()

	values := map[string]interface{}{
		"task_id":  "123",
		"job_name": "test-job",
		"status":   "running",
	}

	msgID, err := fakeProducer.Publish(ctx, values)
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)

	// Verify message was recorded
	msgs := fakeProducer.GetPublishedMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, "123", msgs[0].Values["task_id"])
	assert.Equal(t, "test-job", msgs[0].Values["job_name"])
	assert.Equal(t, "running", msgs[0].Values["status"])
}

// TestFakeRedisProducer_MultipleMessages tests multiple publishes
func TestFakeRedisProducer_MultipleMessages(t *testing.T) {
	ctx := context.Background()
	fakeProducer := fakes.NewFakeRedisProducer()

	// Publish multiple messages
	for i := 0; i < 3; i++ {
		values := map[string]interface{}{
			"index": i,
		}
		_, err := fakeProducer.Publish(ctx, values)
		require.NoError(t, err)
	}

	// Verify all messages
	msgs := fakeProducer.GetPublishedMessages()
	assert.Len(t, msgs, 3)
	assert.Equal(t, 3, fakeProducer.GetMessageCount())

	// Verify values
	for i, msg := range msgs {
		assert.Equal(t, i, msg.Values["index"])
	}
}

// TestFakeRedisProducer_Error tests error handling
func TestFakeRedisProducer_Error(t *testing.T) {
	ctx := context.Background()
	fakeProducer := fakes.NewFakeRedisProducer()

	expectedErr := errors.New("redis error")
	fakeProducer.SetPublishError(expectedErr)

	values := map[string]interface{}{
		"test": "value",
	}

	msgID, err := fakeProducer.Publish(ctx, values)
	assert.Equal(t, expectedErr, err)
	assert.Empty(t, msgID)

	// No messages should be recorded
	msgs := fakeProducer.GetPublishedMessages()
	assert.Len(t, msgs, 0)
}

// TestFakes_Reset tests the Reset functionality
func TestFakes_Reset(t *testing.T) {
	ctx := context.Background()

	// Setup fakes with some state
	fakeK8s := fakes.NewFakeK8sClient()
	fakeState := fakes.NewFakeStateClient()
	fakeProducer := fakes.NewFakeRedisProducer()

	// Add some state
	params := k8s.JobParams{JobName: "test", Namespace: "default"}
	_ = fakeK8s.CreateQueryJob(ctx, params)
	_ = fakeState.UpdateTaskStatus(ctx, uuid.New(), statev1.TaskStatus_TASK_STATUS_RUNNING)
	_, _ = fakeProducer.Publish(ctx, map[string]interface{}{"test": "value"})

	// Verify state exists
	assert.Len(t, fakeK8s.GetCreatedJobs(), 1)
	assert.Len(t, fakeState.GetTaskUpdates(), 1)
	assert.Len(t, fakeProducer.GetPublishedMessages(), 1)

	// Reset
	fakeK8s.Reset()
	fakeState.Reset()
	fakeProducer.Reset()

	// Verify state is cleared
	assert.Len(t, fakeK8s.GetCreatedJobs(), 0)
	assert.Len(t, fakeState.GetTaskUpdates(), 0)
	assert.Len(t, fakeProducer.GetPublishedMessages(), 0)
}
