package ports

import (
	"context"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
)

// JobObserver reads a Kubernetes Job's state. The job-status handler polls a
// Job through it until the Job is terminal, then reads the Job's metadata to
// route the result and its pod logs to persist them.
type JobObserver interface {
	GetJobStatus(ctx context.Context, namespace, jobName string) (*model.JobResult, error)
	GetPodLogs(ctx context.Context, namespace, jobName string, tailLines int64) (fullLog, tail string, err error)
	GetJobMeta(ctx context.Context, namespace, jobName string) (labels, annotations map[string]string, err error)
}
