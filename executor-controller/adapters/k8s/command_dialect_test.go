package k8s

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/commandcfg"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
)

// newDialectTestClient builds a K8sClient whose resolver is loaded from the
// given dbt-commands.yaml content.
func newDialectTestClient(t *testing.T, yaml string) *K8sClient {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dbt-commands.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	resolver, err := commandcfg.Load(path, logger)
	require.NoError(t, err)
	c := &K8sClient{logger: logger, commands: resolver}
	c.setClientsetForTest(fake.NewSimpleClientset())
	return c
}

const wiseDialectYAML = `
services:
  wise:
    run: ["wise-dbt", "run", "--select", "{{ node }}"]
    seed_build: ["wise-dbt", "seed", "--select", "{{ node }}", "--schema", "{{ target_schema }}"]
    compile:
      command: ["wise-dbt", "compile", "--profiles-dir", "/project"]
      manifest_path: "/project/out dir/manifest.json"
`

func TestCreateQueryJob_UsesServiceDialect(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j1", ServiceName: "wise", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j1")
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "orders"},
		job.Spec.Template.Spec.Containers[0].Command)
}

func TestCreateQueryJob_UnknownServiceUsesBuiltin(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j2", ServiceName: "service-1", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j2")
	assert.Equal(t, []string{"dbt", "run", "--select", "orders"},
		job.Spec.Template.Spec.Containers[0].Command)
}

func TestCreateSeedBuildJob_UsesSeedBuildTemplate_AndKeepsEnv(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateSeedBuildJob(context.Background(), ValidationJobParams{
		JobName: "j3", ReleaseID: "rel1", NodeID: "wise.fx", ServiceName: "wise",
		TableName: "fx", NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "v1",
		CandidateSchema: "_candidate_rel1", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j3")
	spec := job.Spec.Template.Spec
	assert.Equal(t,
		[]string{"wise-dbt", "seed", "--select", "fx", "--schema", "_candidate_rel1"},
		spec.Containers[0].Command)
	assert.Equal(t, "_candidate_rel1", envByName(spec, "DBT_TARGET_SCHEMA"),
		"DBT_TARGET_SCHEMA stays injected even with a seed_build template")
}

func TestCreateCompileJob_UsesDialectAndQuotesManifestPath(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, wiseDialectYAML)
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "j4", ReleaseID: "rel1", NodeID: "wise", ServiceName: "wise",
		ImageTag: "v1", ManifestS3URI: "s3://b/k", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j4")
	line := job.Spec.Template.Spec.InitContainers[0].Command[2]
	assert.Equal(t,
		"wise-dbt compile --profiles-dir /project && cp '/project/out dir/manifest.json' /shared/manifest.json",
		line, "manifest path with a space must be shell-quoted")
}

func TestCreateCompileJob_DefaultLineByteIdentical(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, "")
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "j5", ReleaseID: "rel1", NodeID: "svc", ServiceName: "svc",
		ImageTag: "v1", ManifestS3URI: "s3://b/k", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j5")
	assert.Equal(t,
		"dbt compile --profiles-dir /project && cp /project/target/manifest.json /shared/manifest.json",
		job.Spec.Template.Spec.InitContainers[0].Command[2],
		"no config: compile line must be byte-identical to the plain-dbt form")
}
