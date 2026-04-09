package fakes

import (
	"context"

	"github.com/carolsimone/continuo/k8s-controller/domain/model"
)

// FakeK8sClient is a fake implementation of K8sStatusChecker for testing
type FakeK8sClient struct {
	GetJobStatusFunc func(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error)
	GetPodLogsFunc   func(ctx context.Context, namespace, jobName string, tailLines int64) (string, string, error)
	CallCount        int
	LastNamespace    string
	LastJobName      string
}

// GetJobStatus implements K8sStatusChecker
func (f *FakeK8sClient) GetJobStatus(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error) {
	f.CallCount++
	f.LastNamespace = namespace
	f.LastJobName = jobName
	if f.GetJobStatusFunc != nil {
		return f.GetJobStatusFunc(ctx, namespace, jobName)
	}
	return &model.K8sPodResult{Status: model.JobStatusSucceeded}, nil
}

// GetPodLogs implements K8sStatusChecker
func (f *FakeK8sClient) GetPodLogs(ctx context.Context, namespace, jobName string, tailLines int64) (string, string, error) {
	if f.GetPodLogsFunc != nil {
		return f.GetPodLogsFunc(ctx, namespace, jobName, tailLines)
	}
	return "full log line 1\nfull log line 2", "full log line 2", nil
}
