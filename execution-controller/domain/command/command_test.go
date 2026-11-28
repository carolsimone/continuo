package command_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeployTask_ToJobSpec(t *testing.T) {
	c := command.DeployTask{
		TaskID: "t1", RunID: "s1", ScheduleName: "daily", ProjectName: "dbt",
		SchemaName: "public", TableName: "orders", JobName: "dbt-public-orders",
		NodeType: "dbt-model", ImageTag: "sha-abc", Attempt: 2, ContentHash: "sha256:91d4c0",
	}
	spec := c.ToJobSpec()
	assert.Equal(t, "dbt-public-orders", spec.JobName)
	assert.Equal(t, "t1", spec.TaskID)
	assert.Equal(t, "s1", spec.RunID)
	assert.Equal(t, "daily", spec.ScheduleName)
	assert.Equal(t, "dbt", spec.ProjectName)
	assert.Equal(t, "public", spec.SchemaName)
	assert.Equal(t, "orders", spec.TableName)
	assert.Equal(t, "dbt-model", spec.NodeType)
	assert.Equal(t, "sha-abc", spec.ImageTag)
	assert.Equal(t, "sha256:91d4c0", spec.ContentHash)
}

func TestDeployTask_ToJobSpecCarriesTheDependentsCheck(t *testing.T) {
	pf := &seedreload.DependentsCheck{Mode: seedreload.ReportMode, Family: warehouse.LateBinding, Relation: "analytics.seed_fx_rates_eur"}

	assert.Equal(t, pf, command.DeployTask{NodeType: "dbt-seed", DependentsCheck: pf}.ToJobSpec().DependentsCheck)
	assert.Nil(t, command.DeployTask{NodeType: "dbt-seed"}.ToJobSpec().DependentsCheck)
}

func TestValidationDeployTask_SatisfiesCommand(t *testing.T) {
	var c command.Command = command.ValidationDeployTask{}
	assert.NotNil(t, c)
}

func TestValidationDeployTask_JSONRoundTrip(t *testing.T) {
	orig := command.ValidationDeployTask{
		ReleaseID:            "rel_123",
		NodeID:               "node_456",
		ProjectName:          "dbt",
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
		ProjectName:          "svc",
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
	assert.Equal(t, c.ProjectName, spec.ProjectName)
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
		ProjectName:         "svc",
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
		ReleaseID: "r", NodeID: "model.a", ProjectName: "s", SchemaName: "sc",
		TableName: "a", NodeType: "dbt-model", ImageTag: "t", JobName: "j",
		CandidateSchema: "_candidate_r", CandidateArtifactURI: "s3://b/a.sql",
		ValidationOp: string(pkg_model.ValidationOpBuildFromProd), ProdSchema: "analytics",
	}
	spec := cmd.ToValidationJobSpec()
	assert.Equal(t, string(pkg_model.ValidationOpBuildFromProd), spec.ValidationOp)
	assert.Equal(t, "analytics", spec.ProdSchema)
}

func TestValidationDeployTask_ToValidationJobSpec_CarriesTheDependentsCheck(t *testing.T) {
	pf := &seedreload.DependentsCheck{Mode: seedreload.ReportMode, Family: warehouse.LateBinding, Relation: "analytics.fx"}
	spec := command.ValidationDeployTask{NodeID: "analytics.fx", DependentsCheck: pf}.ToValidationJobSpec()
	assert.Same(t, pf, spec.DependentsCheck)
}
