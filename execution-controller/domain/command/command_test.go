package command_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
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
		ReleaseID:            "rel_123",
		NodeID:               "node_456",
		ServiceName:          "dbt",
		SchemaName:           "public",
		TableName:            "orders",
		NodeType:             "dbt-model",
		ImageTag:             "sha-abc",
		JobName:              "validate-public-orders",
		CandidateSchema:      "_candidate_rel_123",
		CandidateArtifactURI: "s3://continuo-artifacts/candidate-sql/rel_123/node_456.sql",
		UpstreamNodeIDs:      []string{"model.shop.upstream_a", "model.shop.upstream_b"},
	}

	raw, err := json.Marshal(orig)
	require.NoError(t, err)

	var got command.ValidationDeployTask
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, orig, got)
}

func TestValidationDeployTask_ToValidationJobSpec(t *testing.T) {
	c := command.ValidationDeployTask{
		ReleaseID:            "rel_123",
		NodeID:               "node_456",
		ServiceName:          "svc",
		SchemaName:           "public",
		TableName:            "orders",
		NodeType:             "dbt-model",
		ImageTag:             "sha-abc",
		JobName:              "validate-public-orders",
		CandidateSchema:      "_candidate_rel_123",
		CandidateArtifactURI: "s3://continuo-artifacts/candidate-sql/rel_123/node_456.sql",
		UpstreamNodeIDs:      []string{"model.shop.upstream"},
	}
	spec := c.ToValidationJobSpec()
	assert.Equal(t, c.JobName, spec.JobName)
	assert.Equal(t, c.ReleaseID, spec.ReleaseID)
	assert.Equal(t, c.NodeID, spec.NodeID)
	assert.Equal(t, c.ServiceName, spec.ServiceName)
	assert.Equal(t, c.SchemaName, spec.SchemaName)
	assert.Equal(t, c.TableName, spec.TableName)
	assert.Equal(t, c.NodeType, spec.NodeType)
	assert.Equal(t, c.ImageTag, spec.ImageTag)
	assert.Equal(t, c.CandidateSchema, spec.CandidateSchema)
	assert.Equal(t, c.CandidateArtifactURI, spec.CandidateArtifactURI)
}

func TestValidationDeployTask_ToValidationJobSpec_CarriesParseCacheURIs(t *testing.T) {
	c := command.ValidationDeployTask{
		ReleaseID:           "rel_123",
		NodeID:              "svc",
		ServiceName:         "svc",
		ImageTag:            "sha-abc",
		JobName:             "compile-svc",
		CandidateSchema:     "_candidate_rel_123",
		ParseProdS3URI:      "s3://b/svc/parse-cache/sha-abc/partial_parse.msgpack",
		ParseCandidateS3URI: "s3://b/svc/rel_123/partial_parse.candidate.msgpack",
	}
	spec := c.ToValidationJobSpec()
	assert.Equal(t, c.ParseProdS3URI, spec.ParseProdS3URI)
	assert.Equal(t, c.ParseCandidateS3URI, spec.ParseCandidateS3URI)
}

func TestValidationDeployTask_ToValidationJobSpec_CarriesOp(t *testing.T) {
	cmd := command.ValidationDeployTask{
		ReleaseID: "r", NodeID: "model.a", ServiceName: "s", SchemaName: "sc",
		TableName: "a", NodeType: "dbt-model", ImageTag: "t", JobName: "j",
		CandidateSchema: "_candidate_r", CandidateArtifactURI: "s3://b/a.sql",
		ValidationOp: "clone_from_prod", ProdSchema: "analytics",
	}
	spec := cmd.ToValidationJobSpec()
	assert.Equal(t, "clone_from_prod", spec.ValidationOp)
	assert.Equal(t, "analytics", spec.ProdSchema)
}

func TestDeployTask_NextAttemptNamesTheRetryJobAndKeepsTheTask(t *testing.T) {
	c := command.DeployTask{ //nolint:gosec // G101: secret_ref names a Kubernetes Secret, not a credential
		TaskID: "t1", ScheduleID: "7f3e1a2b-0000-0000-0000-000000000000", ScheduleName: "daily",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e",
		JobName: "service-1-e2e-schema-ftable-e-7f3e1a2b", NodeType: "python-api", ImageTag: "reg/img:v1",
		SecretRef: "continuo-api-fx", Operation: "build", TaskRetryCount: 0, TaskMaxRetries: 2,
	}
	next, err := c.NextAttempt()
	require.NoError(t, err)
	assert.Equal(t, "service-1-e2e-schema-ftable-e-7f3e1a2b-r1", next.JobName)
	assert.Equal(t, 1, next.TaskRetryCount)
	assert.Equal(t, 2, next.TaskMaxRetries)
	assert.Equal(t, "continuo-api-fx", next.SecretRef, "the retry keeps the Secret")
	assert.Equal(t, "build", next.Operation, "the retry keeps the operation")
	assert.Equal(t, "reg/img:v1", next.ImageTag)

	third, err := next.NextAttempt()
	require.NoError(t, err)
	assert.Equal(t, "service-1-e2e-schema-ftable-e-7f3e1a2b-r2", third.JobName, "named from the task, not from the previous Job")
}
