package k8s

import (
	"context"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateQueryJob_StampsExecutorDeploymentID ties a production Job back to
// the deployment row accounting for its execution slot.
func TestCreateQueryJob_StampsExecutorDeploymentID(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j1", ServiceName: "wise", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1", Namespace: "default",
		ExecutorDeploymentID: "dep-1",
	}))
	job := fetchJob(t, c, "default", "j1")
	assert.Equal(t, "dep-1", job.Annotations[pkg_model.AnnotationExecutorDeploymentID])
	assert.Equal(t, "dep-1", job.Spec.Template.Annotations[pkg_model.AnnotationExecutorDeploymentID],
		"the pod template carries it too, so a pod alone identifies its deployment")
}

// TestCreateQueryJob_NoDeploymentIDStampsNoAnnotation keeps a Job dispatched
// without a deployment id free of an empty annotation.
func TestCreateQueryJob_NoDeploymentIDStampsNoAnnotation(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j2", ServiceName: "wise", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j2")
	_, present := job.Annotations[pkg_model.AnnotationExecutorDeploymentID]
	assert.False(t, present, "expected no executor-deployment-id annotation")
}

// TestCreateValidationJob_StampsExecutorDeploymentID covers the candidate legs:
// they reserve a slot exactly like a production Job, so their Jobs must name the
// row that holds it.
func TestCreateValidationJob_StampsExecutorDeploymentID(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateValidationJob(context.Background(), ValidationJobParams{
		JobName: "v1", ReleaseID: "rel1", NodeID: "wise.orders", ServiceName: "wise",
		SchemaName: "public", TableName: "orders", NodeType: pkg_model.NodeTypeDbtModel,
		ImageTag: "v1", CandidateSchema: "_cand", CandidateSQLURI: "s3://b/sql",
		Namespace: "default", ExecutorDeploymentID: "dep-v1",
	}))
	job := fetchJob(t, c, "default", "v1")
	assert.Equal(t, "dep-v1", job.Annotations[pkg_model.AnnotationExecutorDeploymentID])
}

// TestCreateSeedBuildJob_StampsExecutorDeploymentID — see the validation case.
func TestCreateSeedBuildJob_StampsExecutorDeploymentID(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateSeedBuildJob(context.Background(), ValidationJobParams{
		JobName: "s1", ReleaseID: "rel1", NodeID: "wise.fx", ServiceName: "wise",
		TableName: "fx", NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "v1",
		CandidateSchema: "_cand", Namespace: "default", ExecutorDeploymentID: "dep-s1",
	}))
	job := fetchJob(t, c, "default", "s1")
	assert.Equal(t, "dep-s1", job.Annotations[pkg_model.AnnotationExecutorDeploymentID])
}

// TestCreateCompileJob_StampsExecutorDeploymentID — see the validation case.
func TestCreateCompileJob_StampsExecutorDeploymentID(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "c1", ReleaseID: "rel1", NodeID: "wise", ServiceName: "wise",
		ImageTag: "v1", ManifestS3URI: "s3://b/k", Namespace: "default",
		ExecutorDeploymentID: "dep-c1",
	}))
	job := fetchJob(t, c, "default", "c1")
	assert.Equal(t, "dep-c1", job.Annotations[pkg_model.AnnotationExecutorDeploymentID])
}

// TestCreateQueryJob_ResolvedArgvUsedVerbatim pins the command of a task that
// was routed to a worker pool and then rolled back to a Job: the pool resolved
// its argv when it was queued, and re-resolving now could yield a different
// command than the one the task was admitted with.
func TestCreateQueryJob_ResolvedArgvUsedVerbatim(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	pinned := []string{"wise-dbt", "run", "--select", "orders", "--vars", "{pinned: true}"}
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j3", ServiceName: "wise", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1", Namespace: "default",
		ResolvedArgv: pinned,
	}))
	job := fetchJob(t, c, "default", "j3")
	assert.Equal(t, pinned, job.Spec.Template.Spec.Containers[0].Command)
}

// TestCreateQueryJob_NoResolvedArgvResolvesCommand keeps the ordinary path on
// the resolver: a Job dispatched with no pinned argv builds its command from the
// service dialect.
func TestCreateQueryJob_NoResolvedArgvResolvesCommand(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j4", ServiceName: "wise", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j4")
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "orders"},
		job.Spec.Template.Spec.Containers[0].Command)
}
