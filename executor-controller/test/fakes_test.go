package test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/test/fakes"
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

// TestFakeRedisProducer_Publish tests the fake Redis producer
func TestFakeRedisProducer_Publish(t *testing.T) {
	ctx := context.Background()
	fakeProducer := fakes.NewFakeRedisProducer()

	values := map[string]interface{}{
		"task_id":  "123",
		"job_name": "test-job",
		"status":   "running",
	}

	msgID, err := fakeProducer.Publish(ctx, "test-stream", values)
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)

	// Verify message was recorded
	msgs := fakeProducer.GetPublishedMessages()
	require.Len(t, msgs, 1)
	assert.Equal(t, "test-stream", msgs[0].Stream)
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
		_, err := fakeProducer.Publish(ctx, "test-stream", values)
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

	msgID, err := fakeProducer.Publish(ctx, "test-stream", values)
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
	fakeProducer := fakes.NewFakeRedisProducer()

	// Add some state
	params := k8s.JobParams{JobName: "test", Namespace: "default"}
	_ = fakeK8s.CreateQueryJob(ctx, params)
	_, _ = fakeProducer.Publish(ctx, "test-stream", map[string]interface{}{"test": "value"})

	// Verify state exists
	assert.Len(t, fakeK8s.GetCreatedJobs(), 1)
	assert.Len(t, fakeProducer.GetPublishedMessages(), 1)

	// Reset
	fakeK8s.Reset()
	fakeProducer.Reset()

	// Verify state is cleared
	assert.Len(t, fakeK8s.GetCreatedJobs(), 0)
	assert.Len(t, fakeProducer.GetPublishedMessages(), 0)
}
