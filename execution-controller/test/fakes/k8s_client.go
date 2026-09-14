package fakes

import (
	"context"
	"fmt"
	"sync"

	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ ports.JobObserver = (*FakeK8sClient)(nil)

// FakeK8sClient stands in for the Kubernetes adapter on both sides: it stores
// created Jobs in memory and answers status, log and metadata reads through
// the optional Func hooks (defaulting to a succeeded Job with two log lines).
// The zero value is usable.
type FakeK8sClient struct {
	mu            sync.RWMutex
	jobs          map[string]*batchv1.Job // key: namespace/jobName
	createJobErr  error
	jobExistsErr  error
	createJobFunc func(ctx context.Context, job *batchv1.Job) error

	GetJobStatusFunc func(ctx context.Context, namespace, jobName string) (*model.JobResult, error)
	GetPodLogsFunc   func(ctx context.Context, namespace, jobName string, tailLines int64) (string, string, error)
	GetJobMetaFunc   func(ctx context.Context, namespace, jobName string) (labels, annotations map[string]string, err error)
	CallCount        int
	LastNamespace    string
	LastJobName      string
}

func NewFakeK8sClient() *FakeK8sClient {
	return &FakeK8sClient{jobs: make(map[string]*batchv1.Job)}
}

func (f *FakeK8sClient) SetCreateJobError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createJobErr = err
}

func (f *FakeK8sClient) SetJobExistsError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobExistsErr = err
}

func (f *FakeK8sClient) SetCreateJobFunc(fn func(ctx context.Context, job *batchv1.Job) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createJobFunc = fn
}

func (f *FakeK8sClient) JobExists(ctx context.Context, namespace, jobName string) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.jobExistsErr != nil {
		return false, f.jobExistsErr
	}
	_, exists := f.jobs[namespace+"/"+jobName]
	return exists, nil
}

func (f *FakeK8sClient) CreateJob(ctx context.Context, job *batchv1.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createJobErr != nil {
		return f.createJobErr
	}
	if f.createJobFunc != nil {
		return f.createJobFunc(ctx, job)
	}
	if f.jobs == nil {
		f.jobs = make(map[string]*batchv1.Job)
	}
	f.jobs[fmt.Sprintf("%s/%s", job.Namespace, job.Name)] = job
	return nil
}

func (f *FakeK8sClient) CreateQueryJob(ctx context.Context, params k8s.JobParams) error {
	exists, err := f.JobExists(ctx, params.Namespace, params.JobName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	backoffLimit := int32(3)
	return f.CreateJob(ctx, &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: params.JobName, Namespace: params.Namespace},
		Spec:       batchv1.JobSpec{BackoffLimit: &backoffLimit},
	})
}

func (f *FakeK8sClient) GetCreatedJobs() map[string]*batchv1.Job {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]*batchv1.Job, len(f.jobs))
	for k, v := range f.jobs {
		out[k] = v
	}
	return out
}

func (f *FakeK8sClient) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs = make(map[string]*batchv1.Job)
	f.createJobErr = nil
	f.jobExistsErr = nil
	f.createJobFunc = nil
}

func (f *FakeK8sClient) GetJobStatus(ctx context.Context, namespace, jobName string) (*model.JobResult, error) {
	f.CallCount++
	f.LastNamespace = namespace
	f.LastJobName = jobName
	if f.GetJobStatusFunc != nil {
		return f.GetJobStatusFunc(ctx, namespace, jobName)
	}
	return &model.JobResult{Status: model.JobStatusSucceeded}, nil
}

func (f *FakeK8sClient) GetPodLogs(ctx context.Context, namespace, jobName string, tailLines int64) (string, string, error) {
	if f.GetPodLogsFunc != nil {
		return f.GetPodLogsFunc(ctx, namespace, jobName, tailLines)
	}
	return "full log line 1\nfull log line 2", "full log line 2", nil
}

func (f *FakeK8sClient) GetJobMeta(ctx context.Context, namespace, jobName string) (labels, annotations map[string]string, err error) {
	if f.GetJobMetaFunc != nil {
		return f.GetJobMetaFunc(ctx, namespace, jobName)
	}
	return nil, nil, nil
}
