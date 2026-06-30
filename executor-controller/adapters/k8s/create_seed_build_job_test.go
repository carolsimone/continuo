package k8s

import (
	"context"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSeedBuildJob_TeamImageDbtSeedIntoCandidate(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	c := newValidationTestClient() // reuse the k8s fake-client constructor
	p := ValidationJobParams{
		JobName: "seedbuild-fx", ReleaseID: "r", NodeID: "seed.core.fx",
		ServiceName: "core", SchemaName: "analytics", TableName: "fx",
		NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "abc123",
		CandidateSchema: "_candidate_r", Namespace: "default",
	}
	require.NoError(t, c.CreateSeedBuildJob(context.Background(), p))
	job := fetchJob(t, c, p.Namespace, p.JobName)
	ctr := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "carolsimone/core:abc123", ctr.Image) // team image, not dbt-base
	assert.Equal(t, []string{"dbt", "seed", "--select", "fx"}, ctr.Command)
	assert.Equal(t, "_candidate_r", envByName(job.Spec.Template.Spec, "DBT_TARGET_SCHEMA"))
	assert.Equal(t, "seed_build", job.Spec.Template.Labels["mode"])
}

func TestCreateSeedBuildJob_EmptyImageTagErrors(t *testing.T) {
	c := newValidationTestClient()
	p := ValidationJobParams{JobName: "x", ServiceName: "core", TableName: "fx",
		NodeType: pkg_model.NodeTypeDbtSeed, CandidateSchema: "_candidate_r", Namespace: "default"}
	require.Error(t, c.CreateSeedBuildJob(context.Background(), p))
}
