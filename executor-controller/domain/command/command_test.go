package command_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/stretchr/testify/assert"
)

func TestDeployTask_ToJobSpec(t *testing.T) {
	c := command.DeployTask{
		TaskID: "t1", ScheduleID: "s1", ScheduleName: "daily", ServiceName: "dbt",
		SchemaName: "public", TableName: "orders", JobName: "dbt-public-orders",
		NodeType: "dbt-model", ImageTag: "sha-abc", TaskRetryCount: 2, TaskMaxRetries: 5,
	}
	spec := c.ToJobSpec()
	assert.Equal(t, "dbt-public-orders", spec.JobName)
	assert.Equal(t, "t1", spec.TaskID)
	assert.Equal(t, "s1", spec.ScheduleID)
	assert.Equal(t, "daily", spec.ScheduleName)
	assert.Equal(t, "dbt", spec.ServiceName)
	assert.Equal(t, "public", spec.SchemaName)
	assert.Equal(t, "orders", spec.TableName)
	assert.Equal(t, "dbt-model", spec.NodeType)
	assert.Equal(t, "sha-abc", spec.ImageTag)
}
