package event_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/event"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
)

// TestDeployTask_ToNodeDeployedEvent verifies the executor maps its outbox
// DeployTask onto the typed node.deployed:v1 wire event, carrying the task-level
// retry count and max-retries forward.
func TestDeployTask_ToNodeDeployedEvent(t *testing.T) {
	d := event.DeployTask{
		TaskID:         "t1",
		ScheduleID:     "s1",
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "public",
		TableName:      "orders",
		JobName:        "job-1",
		NodeType:       "dbt-model",
		ImageTag:       "sha-abc",
		TaskRetryCount: 1,
		TaskMaxRetries: 4,
	}

	got := d.ToNodeDeployedEvent()

	assert.Equal(t, pkgevents.NodeDeployed{
		TaskID:         "t1",
		ScheduleID:     "s1",
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "public",
		TableName:      "orders",
		JobName:        "job-1",
		NodeType:       "dbt-model",
		ImageTag:       "sha-abc",
		TaskRetryCount: 1,
		MaxRetries:     4,
	}, got)
}
