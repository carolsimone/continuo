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
