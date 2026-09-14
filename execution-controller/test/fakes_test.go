package test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/test/fakes"
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
