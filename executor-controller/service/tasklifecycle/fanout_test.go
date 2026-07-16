package tasklifecycle_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/tasklifecycle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingOutboxRepo captures Create calls without a database.
type recordingOutboxRepo struct {
	pkgoutbox.Repository
	entries []*pkgoutbox.Entry
	err     error
}

func (r *recordingOutboxRepo) Create(_ context.Context, e *pkgoutbox.Entry) error {
	if r.err != nil {
		return r.err
	}
	r.entries = append(r.entries, e)
	return nil
}

func (r *recordingOutboxRepo) byStream(stream string) *pkgoutbox.Entry {
	for _, e := range r.entries {
		if e.StreamName == stream {
			return e
		}
	}
	return nil
}

// streamNames lists the streams written, in the order they were written.
func (r *recordingOutboxRepo) streamNames() []string {
	out := make([]string, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e.StreamName)
	}
	return out
}

// workerCmd is a production task routed to a worker pool.
func workerCmd(taskID, scheduleID uuid.UUID) command.DeployTask {
	return command.DeployTask{
		TaskID: taskID.String(), ScheduleID: scheduleID.String(), ScheduleName: "daily",
		ServiceName: "finance", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
		Operation: "test", DBTUniqueID: "model.finance.orders",
		TaskRetryCount: 1, TaskMaxRetries: 3,
	}
}

// leased builds a worker deployment a worker has claimed and started, and
// returns it with the execution the lease that runs it describes.
func leased(t *testing.T, taskID, scheduleID uuid.UUID) (*model.Deployment, tasklifecycle.Execution) {
	t.Helper()
	dep := model.NewWorkerDeployment(workerCmd(taskID, scheduleID), uuid.New(), "dbt:sha-abc", time.Unix(10, 0))
	leaseID := uuid.New()
	require.NoError(t, dep.Claim(leaseID, "digest", "worker-1", "pod-a", "uid-a",
		time.Unix(20, 0), time.Unix(80, 0), []string{"dbt", "test"}, model.ExecutionPathNative))
	_, err := dep.AcknowledgeStart(leaseID, "digest", startedAt)
	require.NoError(t, err)
	started, completed := startedAt, completedAt
	return dep, tasklifecycle.Execution{ID: leaseID, StartedAt: &started, CompletedAt: &completed}
}

// startedAt and completedAt are when the worker's dbt process started and when
// the executor recorded its outcome.
var (
	startedAt   = time.Unix(21, 0).UTC()
	completedAt = time.Unix(93, 0).UTC()
)

func succeededResult() model.WorkerResult {
	return model.WorkerResult{
		Succeeded: true, ExecutionSeconds: 12.5,
		LogS3URI: "s3://continuo/dbt-runs/daily/task-1/lease-1/dbt.log",
	}
}

// TestFanout_StartedAnnouncesRunning pins the row a worker's start report owes
// the UI: without it a claimed task shows as queued for its whole run.
func TestFanout_StartedAnnouncesRunning(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, _ := leased(t, taskID, scheduleID)
	repo := &recordingOutboxRepo{}

	require.NoError(t, tasklifecycle.Fanout{}.Started(context.Background(), repo, dep))
	require.Len(t, repo.entries, 1, "a start announces status only — it settles nothing")

	entry := repo.byStream(streams.TaskStatusUpdatedV1)
	require.NotNil(t, entry)
	assert.Equal(t, "task_status_updated", entry.EventType)
	assert.Equal(t, taskID, entry.AggregateID)

	var status pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(entry.Payload, &status))
	assert.Equal(t, "RUNNING", status.Status)
	assert.Equal(t, taskID.String(), status.TaskID)
	assert.Equal(t, int32(1), status.RetryCount)
}

// TestFanout_SucceededSettlesTaskAndNode pins the three rows a successful worker
// report owes: the task settles for the UI, the execution is recorded, and the
// schedule advances.
func TestFanout_SucceededSettlesTaskAndNode(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	repo := &recordingOutboxRepo{}

	require.NoError(t, tasklifecycle.Fanout{}.Succeeded(
		context.Background(), repo, dep, exec, succeededResult()))

	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.NodeUpdatedV1,
	}, repo.streamNames(), "a success writes no retry")

	var status pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskStatusUpdatedV1).Payload, &status))
	assert.Equal(t, "SUCCEEDED", status.Status)

	var execEvent pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskExecutionRecordedV1).Payload, &execEvent))
	assert.Equal(t, exec.ID.String(), execEvent.ExecutionID, "the lease that ran it identifies the execution")
	assert.Equal(t, taskID.String(), execEvent.TaskID)
	assert.Equal(t, "dbt-public-orders", execEvent.JobName)
	assert.InDelta(t, 12.5, execEvent.ExecutionSeconds, 0.001)
	assert.Equal(t, "dbt-runs/daily/task-1/lease-1/dbt.log", execEvent.LogS3Key,
		"state stores the object key, not the s3:// URI")
	assert.Equal(t, startedAt.Format(time.RFC3339), execEvent.StartedAt,
		"the execution is timed by the lease that ran it")
	assert.Equal(t, completedAt.Format(time.RFC3339), execEvent.CompletedAt)

	var node map[string]string
	require.NoError(t, json.Unmarshal(repo.byStream(streams.NodeUpdatedV1).Payload, &node))
	assert.Equal(t, "SUCCEEDED", node["status"])
	assert.Equal(t, "orders", node["table_name"])
}

// TestFanout_SucceededWithoutUploadedLogStillSettles pins that a warehouse
// result is never rerun because its artifacts did not upload. The run happened;
// re-materializing the model to recover a log would be worse than losing the log.
func TestFanout_SucceededWithoutUploadedLogStillSettles(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	repo := &recordingOutboxRepo{}

	// The worker's dbt run succeeded but its log upload failed, so it reports no
	// log URI.
	result := model.WorkerResult{Succeeded: true, ExecutionSeconds: 12.5}
	require.NoError(t, tasklifecycle.Fanout{}.Succeeded(context.Background(), repo, dep, exec, result))

	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.NodeUpdatedV1,
	}, repo.streamNames(), "a missing log never turns a success into a retry")

	var status pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskStatusUpdatedV1).Payload, &status))
	assert.Equal(t, "SUCCEEDED", status.Status)

	var execEvent pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskExecutionRecordedV1).Payload, &execEvent))
	assert.Empty(t, execEvent.LogS3Key, "no uploaded log means no key, not a failed execution")
}

// TestFanout_RetryableFailureRequeuesWithoutSettlingNode pins that a retryable
// failure never announces node.updated: the node has not settled, and telling
// the orchestrator it failed would advance the schedule past a task that is about
// to run again.
func TestFanout_RetryableFailureRequeuesWithoutSettlingNode(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	repo := &recordingOutboxRepo{}
	result := model.WorkerResult{
		Succeeded: false, Retryable: true, ErrorClass: "warehouse_unavailable",
		ErrorMessage: "connection reset", ExecutionSeconds: 3,
		LogS3URI: "s3://continuo/dbt-runs/daily/task-1/lease-1/dbt.log",
	}

	require.NoError(t, tasklifecycle.Fanout{}.RetryableFailure(
		context.Background(), repo, dep, exec, result))

	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.RetryTaskV1,
	}, repo.streamNames(), "the node has not settled, so it is not announced")

	var status pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskStatusUpdatedV1).Payload, &status))
	assert.Equal(t, "FAILED", status.Status, "the attempt failed even though the task will run again")
	assert.Equal(t, int32(1), status.RetryCount, "the attempt that failed, not the one to come")

	var execEvent pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskExecutionRecordedV1).Payload, &execEvent))
	assert.Equal(t, exec.ID.String(), execEvent.ExecutionID)
	assert.Equal(t, "connection reset", execEvent.ErrorMessage)

	retry := repo.byStream(streams.RetryTaskV1)
	assert.Equal(t, "task_retry", retry.EventType)
	var got pkgevents.TaskRetry
	require.NoError(t, json.Unmarshal(retry.Payload, &got))
	assert.Equal(t, dep.ID().String(), got.ExecutorDeploymentID,
		"a worker task retries in place so its attempt counter stays on one row")
	assert.Equal(t, 2, got.RetryCount, "the next attempt's number")
	assert.Equal(t, 3, got.MaxRetries)
	assert.Equal(t, "dbt-public-orders-r2", got.JobName)
	assert.Equal(t, "test", got.Operation, "the retry reruns the same dbt verb")
	assert.Equal(t, "model.finance.orders", got.DBTUniqueID)
	assert.Equal(t, "finance", got.ServiceName)
	assert.Equal(t, "sha-abc", got.ImageTag)
}

// TestFanout_RetryableFailureAfterLeaseDropped pins that the fan-out does not
// read the lease off the aggregate. MarkRetryPending drops the lease as part of
// parking the task, and the reaper parks an expired lease the same way, so by the
// time this runs there is no lease to read — the caller passes the one that ran.
func TestFanout_RetryableFailureAfterLeaseDropped(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	require.NoError(t, dep.MarkRetryPending(time.Unix(90, 0), 30*time.Second))
	require.Nil(t, dep.ActiveLease(), "the parked task carries no lease")
	repo := &recordingOutboxRepo{}

	result := model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"}
	require.NoError(t, tasklifecycle.Fanout{}.RetryableFailure(
		context.Background(), repo, dep, exec, result))

	var execEvent pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskExecutionRecordedV1).Payload, &execEvent))
	assert.Equal(t, exec.ID.String(), execEvent.ExecutionID,
		"the execution is identified by the lease the caller captured before the transition")
}

// TestFanout_PermanentFailureSettlesNode pins that an exhausted or unfixable
// failure settles the node so the schedule advances, and asks for no retry.
func TestFanout_PermanentFailureSettlesNode(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	repo := &recordingOutboxRepo{}
	result := model.WorkerResult{
		Succeeded: false, ErrorClass: "dbt_unique_id_not_found",
		ErrorMessage: "node orders not in manifest", ExecutionSeconds: 1,
	}

	require.NoError(t, tasklifecycle.Fanout{}.PermanentFailure(
		context.Background(), repo, dep, exec, result))

	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.NodeUpdatedV1,
	}, repo.streamNames(), "a permanent failure asks for no retry")

	var status pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskStatusUpdatedV1).Payload, &status))
	assert.Equal(t, "FAILED", status.Status)

	var node map[string]string
	require.NoError(t, json.Unmarshal(repo.byStream(streams.NodeUpdatedV1).Payload, &node))
	assert.Equal(t, "FAILED", node["status"])

	var execEvent pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskExecutionRecordedV1).Payload, &execEvent))
	assert.Equal(t, "node orders not in manifest", execEvent.ErrorMessage)
}

// TestFanout_ExecutionTimestampsAreRenderedForTheReadModel pins the wire form of
// an execution's timing: RFC3339 in UTC, which is what the run's read model
// parses. A timestamp in another zone or format is discarded there.
func TestFanout_ExecutionTimestampsAreRenderedForTheReadModel(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	zone := time.FixedZone("CEST", 2*60*60)
	started := startedAt.In(zone)
	completed := completedAt.In(zone)
	exec.StartedAt, exec.CompletedAt = &started, &completed
	repo := &recordingOutboxRepo{}

	require.NoError(t, tasklifecycle.Fanout{}.PermanentFailure(context.Background(), repo, dep, exec,
		model.WorkerResult{Succeeded: false, ErrorMessage: "boom"}))

	var execEvent pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(repo.byStream(streams.TaskExecutionRecordedV1).Payload, &execEvent))
	assert.Equal(t, "1970-01-01T00:00:21Z", execEvent.StartedAt, "a local time is rendered in UTC")
	assert.Equal(t, "1970-01-01T00:01:33Z", execEvent.CompletedAt)

	parsedStart, err := time.Parse(time.RFC3339, execEvent.StartedAt)
	require.NoError(t, err, "the read model parses the execution's start")
	assert.True(t, parsedStart.Equal(startedAt), "and reads back the instant the worker started")
}

// TestFanout_ExecutionWithoutTimestampsOmitsThem pins that an execution the
// caller could not time announces no timing rather than a zero one: a worker
// that failed before reporting a start has no start, and a zero time would show
// in the read model as a run in 1970.
func TestFanout_ExecutionWithoutTimestampsOmitsThem(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	exec.StartedAt, exec.CompletedAt = nil, nil
	repo := &recordingOutboxRepo{}

	require.NoError(t, tasklifecycle.Fanout{}.PermanentFailure(context.Background(), repo, dep, exec,
		model.WorkerResult{Succeeded: false, ErrorMessage: "boom"}))

	entry := repo.byStream(streams.TaskExecutionRecordedV1)
	require.NotNil(t, entry)
	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(entry.Payload, &raw))
	assert.NotContains(t, raw, "started_at", "an untimed start is absent, not zero")
	assert.NotContains(t, raw, "completed_at")
}

func TestFanout_TerminalVariantsPropagateOutboxErrors(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep, exec := leased(t, taskID, scheduleID)
	failed := model.WorkerResult{Succeeded: false, ErrorMessage: "boom"}

	for name, write := range map[string]func(repo pkgoutbox.Repository) error{
		"started": func(repo pkgoutbox.Repository) error {
			return tasklifecycle.Fanout{}.Started(context.Background(), repo, dep)
		},
		"succeeded": func(repo pkgoutbox.Repository) error {
			return tasklifecycle.Fanout{}.Succeeded(context.Background(), repo, dep, exec, succeededResult())
		},
		"retryable": func(repo pkgoutbox.Repository) error {
			return tasklifecycle.Fanout{}.RetryableFailure(context.Background(), repo, dep, exec, failed)
		},
		"permanent": func(repo pkgoutbox.Repository) error {
			return tasklifecycle.Fanout{}.PermanentFailure(context.Background(), repo, dep, exec, failed)
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, write(&recordingOutboxRepo{err: assert.AnError}))
		})
	}
}

func rejectedDeployment(t *testing.T, taskID, scheduleID uuid.UUID) *model.Deployment {
	t.Helper()
	msgProcID := uuid.New()
	dep := model.NewDeployment(command.DeployTask{
		TaskID: taskID.String(), ScheduleID: scheduleID.String(), ScheduleName: "daily",
		ServiceName: "finance", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
		TaskRetryCount: 2,
	}, &msgProcID, time.Now())
	require.NoError(t, dep.RejectBeforeExecution("runtime manifest reference is incomplete", time.Now()))
	return dep
}

// TestFanout_DispatchRejectedAnnouncesTerminalFailure pins that a permanently
// rejected record still reaches run state: without these two rows the run would
// wait forever on a node that will never report.
func TestFanout_DispatchRejectedAnnouncesTerminalFailure(t *testing.T) {
	taskID, scheduleID := uuid.New(), uuid.New()
	dep := rejectedDeployment(t, taskID, scheduleID)
	repo := &recordingOutboxRepo{}

	require.NoError(t, tasklifecycle.Fanout{}.DispatchRejected(
		context.Background(), repo, dep, "runtime manifest reference is incomplete"))
	require.Len(t, repo.entries, 2)

	taskStatus := repo.byStream(streams.TaskStatusUpdatedV1)
	require.NotNil(t, taskStatus, "run state advances on task.status.updated:v1")
	assert.Equal(t, "task_status_updated", taskStatus.EventType)
	assert.Equal(t, taskID, taskStatus.AggregateID)
	assert.Equal(t, dep.MessageProcessingID(), taskStatus.MessageProcessingID)

	var status pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(taskStatus.Payload, &status))
	assert.Equal(t, taskID.String(), status.TaskID)
	assert.Equal(t, scheduleID.String(), status.ScheduleID)
	assert.Equal(t, "FAILED", status.Status)
	assert.Equal(t, int32(2), status.RetryCount)

	nodeUpdated := repo.byStream(streams.NodeUpdatedV1)
	require.NotNil(t, nodeUpdated, "the schedule advances on node.updated:v1")
	assert.Equal(t, "node_updated", nodeUpdated.EventType)
	assert.Equal(t, taskID, nodeUpdated.AggregateID)

	var node map[string]string
	require.NoError(t, json.Unmarshal(nodeUpdated.Payload, &node))
	assert.Equal(t, "FAILED", node["status"])
	assert.Equal(t, taskID.String(), node["task_id"])
	assert.Equal(t, scheduleID.String(), node["schedule_id"])
	assert.Equal(t, "finance", node["service_name"])
	assert.Equal(t, "public", node["schema_name"])
	assert.Equal(t, "orders", node["table_name"])
}

func TestFanout_DispatchRejectedPropagatesOutboxErrors(t *testing.T) {
	dep := rejectedDeployment(t, uuid.New(), uuid.New())
	repo := &recordingOutboxRepo{err: assert.AnError}

	err := tasklifecycle.Fanout{}.DispatchRejected(
		context.Background(), repo, dep, "runtime manifest reference is incomplete")
	require.Error(t, err)
}
