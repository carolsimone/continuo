package fakes

import (
	"context"
	"fmt"
	"sync"

	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FakeK8sClient is a fake implementation of K8sClient for testing
type FakeK8sClient struct {
	mu            sync.RWMutex
	jobs          map[string]*batchv1.Job // key: namespace/jobName
	createJobErr  error
	jobExistsErr  error
	createJobFunc func(ctx context.Context, job *batchv1.Job) error
}

// NewFakeK8sClient creates a new FakeK8sClient
func NewFakeK8sClient() *FakeK8sClient {
	return &FakeK8sClient{
		jobs: make(map[string]*batchv1.Job),
	}
}

// SetCreateJobError sets an error to be returned by CreateJob
func (f *FakeK8sClient) SetCreateJobError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createJobErr = err
}

// SetJobExistsError sets an error to be returned by JobExists
func (f *FakeK8sClient) SetJobExistsError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobExistsErr = err
}

// SetCreateJobFunc allows setting a custom function for CreateJob
func (f *FakeK8sClient) SetCreateJobFunc(fn func(ctx context.Context, job *batchv1.Job) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createJobFunc = fn
}

// JobExists checks if a job exists in the fake store
func (f *FakeK8sClient) JobExists(ctx context.Context, namespace, jobName string) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.jobExistsErr != nil {
		return false, f.jobExistsErr
	}

	key := fmt.Sprintf("%s/%s", namespace, jobName)
	_, exists := f.jobs[key]
	return exists, nil
}

// CreateJob stores a job in the fake store
func (f *FakeK8sClient) CreateJob(ctx context.Context, job *batchv1.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createJobErr != nil {
		return f.createJobErr
	}

	if f.createJobFunc != nil {
		return f.createJobFunc(ctx, job)
	}

	key := fmt.Sprintf("%s/%s", job.Namespace, job.Name)
	f.jobs[key] = job
	return nil
}

// CreateQueryJob implements the CreateQueryJob method
func (f *FakeK8sClient) CreateQueryJob(ctx context.Context, params k8s.JobParams) error {
	// Check if job exists (idempotent)
	exists, err := f.JobExists(ctx, params.Namespace, params.JobName)
	if err != nil {
		return err
	}

	if exists {
		return nil // Already exists, idempotent success
	}

	// Create job
	backoffLimit := int32(3)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.JobName,
			Namespace: params.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
		},
	}

	return f.CreateJob(ctx, job)
}

// GetCreatedJobs returns all created jobs
func (f *FakeK8sClient) GetCreatedJobs() map[string]*batchv1.Job {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Return a copy
	jobs := make(map[string]*batchv1.Job)
	for k, v := range f.jobs {
		jobs[k] = v
	}
	return jobs
}

// Reset clears all jobs and errors
func (f *FakeK8sClient) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs = make(map[string]*batchv1.Job)
	f.createJobErr = nil
	f.jobExistsErr = nil
	f.createJobFunc = nil
}
