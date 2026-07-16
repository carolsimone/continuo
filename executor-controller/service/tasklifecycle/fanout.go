// Package tasklifecycle writes the announcements a production task owes the
// rest of the system as it reaches a terminal state.
package tasklifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// Execution is one worker's run of a task: the lease that ran it, when its dbt
// process started, and when the executor recorded its outcome. StartedAt is nil
// for a run whose worker failed before reporting a start; CompletedAt is nil
// when the caller has no recorded time for the outcome. Callers assemble it
// before the aggregate transition their report drives, because a transition may
// drop the lease it describes.
type Execution struct {
	ID          uuid.UUID
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// Fanout emits a production task's lifecycle announcements onto the outbox. It
// holds no state: every call takes the outbox repository bound to the caller's
// transaction, so the announcements commit with the state change that caused
// them.
type Fanout struct{}

// DispatchRejected announces a production task the executor will never run.
// Both rows go onto the caller's transaction: task.status.updated:v1 FAILED
// settles the task for the UI, and node.updated:v1 FAILED lets the orchestrator
// advance the schedule. Without them the run would wait forever on a node that
// has no dispatch to report.
func (Fanout) DispatchRejected(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	reason string,
) error {
	cmd := dep.Command()
	retryCount, err := num.Int32(cmd.TaskRetryCount, "task_retry_count")
	if err != nil {
		return fmt.Errorf("announce rejected task %s (%s): %w", cmd.TaskID, reason, err)
	}
	announcements := []struct {
		eventType string
		stream    string
		payload   interface{}
	}{
		{"task_status_updated", streams.TaskStatusUpdatedV1, pkgevents.TaskStatusUpdated{
			TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, Status: "FAILED", RetryCount: retryCount,
		}},
		{"node_updated", streams.NodeUpdatedV1, event.NodeUpdated{
			TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
			ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
			Status: "FAILED",
		}},
	}
	for _, a := range announcements {
		if err := create(ctx, repo, dep, a.eventType, a.stream, a.payload); err != nil {
			return fmt.Errorf("announce rejected task %s (%s): %w", cmd.TaskID, reason, err)
		}
	}
	return nil
}

// Started announces that a claimed task's dbt process is running, so the UI
// stops showing it as queued. A start settles nothing, so it announces status
// only.
func (Fanout) Started(ctx context.Context, repo outbox.Repository, dep *model.Deployment) error {
	cmd := dep.Command()
	if err := writeTaskStatus(ctx, repo, dep, "RUNNING", cmd.TaskRetryCount); err != nil {
		return fmt.Errorf("announce running task %s: %w", cmd.TaskID, err)
	}
	return nil
}

// Succeeded announces a worker's successful terminal report: the task settles
// for the UI, the execution is recorded, and node.updated:v1 lets the
// orchestrator advance the schedule.
//
// exec identifies and times the worker run being announced. Callers capture it
// before the aggregate transition, because a transition may drop the lease.
func (Fanout) Succeeded(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	exec Execution,
	result model.WorkerResult,
) error {
	cmd := dep.Command()
	if err := writeTerminal(ctx, repo, dep, exec, result, "SUCCEEDED"); err != nil {
		return fmt.Errorf("announce succeeded task %s: %w", cmd.TaskID, err)
	}
	return nil
}

// PermanentFailure announces a worker failure the executor will not retry —
// either its error class is unfixable by rerunning or its retry budget is spent.
// It settles the node so the schedule advances, and asks for no retry.
//
// exec identifies and times the worker run being announced. Callers capture it
// before the aggregate transition, because a transition may drop the lease.
func (Fanout) PermanentFailure(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	exec Execution,
	result model.WorkerResult,
) error {
	cmd := dep.Command()
	if err := writeTerminal(ctx, repo, dep, exec, result, "FAILED"); err != nil {
		return fmt.Errorf("announce failed task %s: %w", cmd.TaskID, err)
	}
	return nil
}

// RetryableFailure announces a worker failure the executor will re-attempt. The
// attempt is announced FAILED and recorded, and retry.task:v1 asks for the next
// one. node.updated:v1 is deliberately not written: the node has not settled, and
// announcing it failed would advance the schedule past a task that is about to
// run again.
//
// exec identifies and times the worker run being announced. Callers capture it
// before the aggregate transition, because parking a task for retry drops its
// lease — by the time this runs there is no lease on the aggregate to read.
func (Fanout) RetryableFailure(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	exec Execution,
	result model.WorkerResult,
) error {
	cmd := dep.Command()
	if err := writeAttemptFailure(ctx, repo, dep, exec, result); err != nil {
		return fmt.Errorf("announce retryable task %s: %w", cmd.TaskID, err)
	}
	nextRetryCount := cmd.TaskRetryCount + 1
	retry := pkgevents.TaskRetry{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
		ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
		JobName:              nextRetryJobName(cmd.JobName, nextRetryCount),
		ImageTag:             cmd.ImageTag,
		RetryCount:           nextRetryCount,
		MaxRetries:           cmd.TaskMaxRetries,
		NodeType:             cmd.NodeType,
		Operation:            cmd.Operation,
		ExecutorDeploymentID: dep.ID().String(),
		DBTUniqueID:          cmd.DBTUniqueID,
		RuntimeManifestRef:   cmd.RuntimeManifestRef,
	}
	if err := create(ctx, repo, dep, "task_retry", streams.RetryTaskV1, retry); err != nil {
		return fmt.Errorf("announce retryable task %s: %w", cmd.TaskID, err)
	}
	return nil
}

// writeTerminal announces a settled task: its status, its execution, and the
// node status the orchestrator advances the schedule on.
func writeTerminal(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	exec Execution,
	result model.WorkerResult,
	status string,
) error {
	cmd := dep.Command()
	if err := writeTaskStatus(ctx, repo, dep, status, cmd.TaskRetryCount); err != nil {
		return err
	}
	if err := writeExecution(ctx, repo, dep, exec, result); err != nil {
		return err
	}
	return create(ctx, repo, dep, "node_updated", streams.NodeUpdatedV1, event.NodeUpdated{
		TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
		ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
		Status: status,
	})
}

// writeAttemptFailure announces a failed attempt without settling its node: the
// rows a retryable failure shares with a permanent one.
func writeAttemptFailure(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	exec Execution,
	result model.WorkerResult,
) error {
	if err := writeTaskStatus(ctx, repo, dep, "FAILED", dep.Command().TaskRetryCount); err != nil {
		return err
	}
	return writeExecution(ctx, repo, dep, exec, result)
}

// writeTaskStatus announces a task's status for the run's read model.
func writeTaskStatus(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	status string,
	retryCount int,
) error {
	cmd := dep.Command()
	count, err := num.Int32(retryCount, "task_retry_count")
	if err != nil {
		return err
	}
	return create(ctx, repo, dep, "task_status_updated", streams.TaskStatusUpdatedV1,
		pkgevents.TaskStatusUpdated{
			TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, Status: status, RetryCount: count,
		})
}

// writeExecution records what one lease's run of a task did, identified by that
// lease and timed by it. A run with no recorded start or completion omits that
// timestamp, which the run's read model treats as unknown.
func writeExecution(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	exec Execution,
	result model.WorkerResult,
) error {
	cmd := dep.Command()
	return create(ctx, repo, dep, "task_execution_recorded", streams.TaskExecutionRecordedV1,
		pkgevents.TaskExecutionRecorded{
			ExecutionID:      exec.ID.String(),
			TaskID:           cmd.TaskID,
			JobName:          cmd.JobName,
			StartedAt:        wireTime(exec.StartedAt),
			CompletedAt:      wireTime(exec.CompletedAt),
			ExecutionSeconds: result.ExecutionSeconds,
			ErrorMessage:     result.ErrorMessage,
			LogS3Key:         logObjectKey(result.LogS3URI),
		})
}

// wireTime renders an execution timestamp the way the run's read model parses
// it: RFC3339 in UTC. An absent timestamp renders empty, which the payload omits.
func wireTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// logObjectKey extracts the object key from a worker's s3:// log URI, which is
// what the run's read model stores. A worker whose log upload failed reports no
// URI and its execution records no key: the warehouse work happened, so a missing
// log is recorded as missing rather than treated as a failed execution.
func logObjectKey(uri string) string {
	rest, found := strings.CutPrefix(uri, "s3://")
	if !found {
		return ""
	}
	_, key, found := strings.Cut(rest, "/")
	if !found {
		return ""
	}
	return key
}

// nextRetryJobName names the Kubernetes Job the next attempt runs as, matching
// the Jobs path's convention. The base name is truncated so the suffix fits
// within the 63-character limit Kubernetes imposes on a Job name.
func nextRetryJobName(baseJobName string, retryCount int) string {
	suffix := fmt.Sprintf("-r%d", retryCount)
	maxBase := 63 - len(suffix)
	if len(baseJobName) > maxBase {
		baseJobName = baseJobName[:maxBase]
	}
	return baseJobName + suffix
}

// create writes one outbox row for dep, keyed on the task aggregate so a task's
// announcements publish in the order they were written.
//
// A row that recovered only a partial identity can carry a task_id that is not a
// UUID (an unparseable or empty job_params task_id). Such a row still owes the
// system its terminal announcements, so an unparseable task_id degrades the
// aggregate key to uuid.Nil rather than failing the write: refusing to write
// would leave the caller unable to settle the row.
func create(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	eventType, stream string,
	payload interface{},
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", eventType, err)
	}
	aggregateID, _ := uuid.Parse(dep.Command().TaskID)
	return repo.Create(ctx, &outbox.Entry{
		MessageProcessingID: dep.MessageProcessingID(),
		AggregateType:       "task",
		AggregateID:         aggregateID,
		EventType:           eventType,
		Payload:             body,
		StreamName:          stream,
		MaxRetries:          3,
	})
}
