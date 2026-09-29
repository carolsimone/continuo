package k8s

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/adapters/commandcfg"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
)

// newDialectTestClient builds a K8sClient whose resolver is loaded from the
// given dbt-commands.yaml content. An empty yaml means "no config file", which
// resolves every command to the built-in plain-dbt default.
func newDialectTestClient(t *testing.T, yaml string) *K8sClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	path := ""
	if yaml != "" {
		path = filepath.Join(t.TempDir(), "dbt-commands.yaml")
		require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))
	}
	resolver, err := commandcfg.Load(path, logger)
	require.NoError(t, err)
	c := &K8sClient{logger: logger, commands: resolver}
	c.setClientsetForTest(fake.NewSimpleClientset())
	return c
}

// customNameDialectYAML is a complete dialect config: a complete plain-dbt default
// plus a complete "customname" override whose run/seed_build/compile the tests assert.
const customNameDialectYAML = `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
services:
  customname:
    run:        ["customname-dbt", "run", "--select", "{{ node }}"]
    seed:       ["customname-dbt", "seed", "--select", "{{ node }}"]
    snapshot:   ["customname-dbt", "snapshot", "--select", "{{ node }}"]
    test:       ["customname-dbt", "test", "--select", "{{ node }}"]
    build:      ["customname-dbt", "build", "--select", "{{ node }}"]
    seed_build: ["customname-dbt", "seed", "--select", "{{ node }}", "--schema", "{{ target_schema }}"]
    parse:      ["customname-dbt", "parse"]
    compile:
      command: ["customname-dbt", "compile", "--profiles-dir", "/project"]
      manifest_path: "/project/out dir/manifest.json"
`

func TestCreateQueryJob_UsesServiceDialect(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, customNameDialectYAML)
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j1", ServiceName: "customname", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j1")
	assert.Equal(t, []string{"customname-dbt", "run", "--select", "orders"},
		job.Spec.Template.Spec.Containers[0].Command)
}

func TestCreateQueryJob_UnknownServiceUsesBuiltin(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, customNameDialectYAML)
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
	c := newDialectTestClient(t, customNameDialectYAML)
	require.NoError(t, c.CreateSeedBuildJob(context.Background(), ValidationJobParams{
		JobName: "j3", ReleaseID: "rel1", NodeID: "customname.fx", ServiceName: "customname",
		TableName: "fx", NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "v1",
		CandidateSchema: "_candidate_rel1", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j3")
	spec := job.Spec.Template.Spec
	assert.Equal(t,
		[]string{"customname-dbt", "seed", "--select", "fx", "--schema", "_candidate_rel1"},
		spec.Containers[0].Command)
	assert.Equal(t, "_candidate_rel1", envByName(spec, "DBT_TARGET_SCHEMA"),
		"DBT_TARGET_SCHEMA stays injected even with a seed_build template")
}

func TestCreateCompileJob_UsesDialectAndQuotesManifestPath(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, customNameDialectYAML)
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "j4", ReleaseID: "rel1", NodeID: "customname", ServiceName: "customname",
		ImageTag: "v1", ManifestS3URI: "s3://b/k", Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j4")
	line := job.Spec.Template.Spec.InitContainers[0].Command[2]
	assert.Equal(t,
		"customname-dbt compile --profiles-dir /project && cp '/project/out dir/manifest.json' /shared/manifest.json && chmod 644 /shared/manifest.json",
		line, "manifest path with a space must be shell-quoted")
}

func TestCreateQueryJob_TestOperation_RunsDbtTest(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, "")
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j6", ServiceName: "service-1", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "t1", Namespace: "default",
		Operation: pkg_model.OperationTest,
	}))
	job := fetchJob(t, c, "default", "j6")
	assert.Equal(t, []string{"dbt", "test", "--select", "orders"},
		job.Spec.Template.Spec.Containers[0].Command)
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
		"dbt compile --profiles-dir /project && cp /project/target/manifest.json /shared/manifest.json && chmod 644 /shared/manifest.json",
		job.Spec.Template.Spec.InitContainers[0].Command[2],
		"no config: compile line must be byte-identical to the plain-dbt form")
}

func TestCreateQueryJob_FullRefresh_UsesServiceTemplate(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, "")
	require.NoError(t, c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j7", ServiceName: "service-1", TableName: "orders",
		NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "t1", Namespace: "default",
		Operation: pkg_model.OperationFullRefresh,
	}))
	job := fetchJob(t, c, "default", "j7")
	assert.Equal(t, []string{"dbt", "run", "--full-refresh", "--select", "orders"},
		job.Spec.Template.Spec.Containers[0].Command)
}

// TestCreateQueryJob_FullRefresh_MissingKeyIsPermanentAndCreatesNoJob is the
// P2 regression: a seed full-refresh against a block that has no
// seed_full_refresh — customNameDialectYAML's customname override defines
// every required key but neither full_refresh nor seed_full_refresh — fails
// permanently and dispatches no Job. It never silently falls back to the
// block's plain seed command.
func TestCreateQueryJob_FullRefresh_MissingKeyIsPermanentAndCreatesNoJob(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, customNameDialectYAML) // its customname block has no seed_full_refresh
	err := c.CreateQueryJob(context.Background(), JobParams{
		JobName: "j8", ServiceName: "customname", TableName: "fx",
		NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "t1", Namespace: "default",
		Operation: pkg_model.OperationFullRefresh,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Contains(t, err.Error(), "services.customname defines no seed_full_refresh command")
	exists, existsErr := c.JobExists(context.Background(), "default", "j8")
	require.NoError(t, existsErr)
	assert.False(t, exists)
}
