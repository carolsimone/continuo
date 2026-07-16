package k8s

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/commandcfg"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
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

// wiseDialectYAML is a complete dialect config: a complete plain-dbt default
// plus a complete "wise" override whose run/seed_build/compile the tests assert.
const wiseDialectYAML = `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
services:
  wise:
    run:        ["wise-dbt", "run", "--select", "{{ node }}"]
    seed:       ["wise-dbt", "seed", "--select", "{{ node }}"]
    snapshot:   ["wise-dbt", "snapshot", "--select", "{{ node }}"]
    test:       ["wise-dbt", "test", "--select", "{{ node }}"]
    build:      ["wise-dbt", "build", "--select", "{{ node }}"]
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
		"wise-dbt compile --profiles-dir /project"+
			" && cp '/project/out dir/manifest.json' /shared/manifest.json"+
			" && if [ -x /continuo/bin/continuo-export-runtime-manifest ]; then"+
			" /continuo/bin/continuo-export-runtime-manifest"+
			" --manifest '/project/out dir/manifest.json'"+
			" --partial-parse '/project/out dir/partial_parse.msgpack'"+
			" --output-dir /shared --service-name wise --release-id rel1 --image-tag v1"+
			" --artifact-uri s3://b/partial_parse.msgpack"+
			` --controller-context "$CONTINUO_RUNTIME_CONTEXT_JSON";`+
			" else echo 'runtime exporter absent; manifest-only compatibility release'; fi",
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
	line := job.Spec.Template.Spec.InitContainers[0].Command[2]
	assert.True(t,
		strings.HasPrefix(line,
			"dbt compile --profiles-dir /project && cp /project/target/manifest.json /shared/manifest.json"),
		"no config: the compile and copy must stay byte-identical to the plain-dbt form")
	assert.Equal(t,
		"dbt compile --profiles-dir /project"+
			" && cp /project/target/manifest.json /shared/manifest.json"+
			" && if [ -x /continuo/bin/continuo-export-runtime-manifest ]; then"+
			" /continuo/bin/continuo-export-runtime-manifest"+
			" --manifest /project/target/manifest.json"+
			" --partial-parse /project/target/partial_parse.msgpack"+
			" --output-dir /shared --service-name svc --release-id rel1 --image-tag v1"+
			" --artifact-uri s3://b/partial_parse.msgpack"+
			` --controller-context "$CONTINUO_RUNTIME_CONTEXT_JSON";`+
			" else echo 'runtime exporter absent; manifest-only compatibility release'; fi",
		line)
}
