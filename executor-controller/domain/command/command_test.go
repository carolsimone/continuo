package command_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestValidationDeployTask_SatisfiesCommand(t *testing.T) {
	var c command.Command = command.ValidationDeployTask{}
	assert.NotNil(t, c)
}

func TestValidationDeployTask_JSONRoundTrip(t *testing.T) {
	orig := command.ValidationDeployTask{
		ReleaseID:       "rel_123",
		NodeID:          "node_456",
		ServiceName:     "dbt",
		SchemaName:      "public",
		TableName:       "orders",
		NodeType:        "dbt-model",
		ImageTag:        "sha-abc",
		JobName:         "validate-public-orders",
		CandidateSchema: "_candidate_rel_123",
		DeferStateURI:   "s3://continuo/releases/prev/manifests/",
	}

	raw, err := json.Marshal(orig)
	require.NoError(t, err)

	var got command.ValidationDeployTask
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, orig, got)
}
