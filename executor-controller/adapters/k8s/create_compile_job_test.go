package k8s

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestCreateCompileJob_InitCompilesMainUploads(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	c := newValidationTestClient()
	p := ValidationJobParams{
		JobName: "compile-svc-rel", ReleaseID: "rel-1", NodeID: "core",
		ServiceName: "core", ImageTag: "abc123",
		ManifestS3URI: "s3://continuo/core/rel-1/manifest.json", Namespace: "default",
	}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	job := fetchJob(t, c, p.Namespace, p.JobName)
	spec := job.Spec.Template.Spec
	require.Len(t, spec.InitContainers, 1)
	assert.Equal(t, "carolsimone/core:abc123", spec.InitContainers[0].Image)          // team image
	assert.Contains(t, spec.InitContainers[0].Command[2], "dbt compile")
	assert.Contains(t, spec.InitContainers[0].Command[2], "/shared/manifest.json")
	assert.Equal(t, "carolsimone/s3-sidecar:latest", spec.Containers[0].Image)   // upload image
	assert.Equal(t, []string{"python", "/compile_uploader.py"}, spec.Containers[0].Command)
	assert.Equal(t, "/shared/manifest.json", envByName(spec, "COMPILE_MANIFEST_PATH"))
	assert.Equal(t, "s3://continuo/core/rel-1/manifest.json", envByName(spec, "MANIFEST_S3_URI"))
	assert.Equal(t, "/shared/partial_parse.msgpack", envByName(spec, "COMPILE_PARTIAL_PARSE_PATH"))
	assert.Equal(t, "/shared/runtime-manifest.json", envByName(spec, "COMPILE_RUNTIME_DESCRIPTOR_PATH"))
	assert.Equal(t, "compile", job.Spec.Template.Labels["mode"])
	// shared emptyDir mounted in both
	assert.Equal(t, "shared", spec.Volumes[0].Name)
	require.NotNil(t, spec.Volumes[0].EmptyDir)
}

// initEnvByName reads an env var off the compile initContainer, which carries a
// different set than the upload container envByName reads.
func initEnvByName(spec corev1.PodSpec, name string) string {
	for _, e := range spec.InitContainers[0].Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func TestCreateCompileJob_ExportsRuntimeArtifactsToSiblingURI(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, "")
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "j7", ReleaseID: "rel-1", NodeID: "core", ServiceName: "core",
		ImageTag: "abc123", ManifestS3URI: "s3://continuo/core/rel-1/manifest.json",
		Namespace: "default",
	}))
	job := fetchJob(t, c, "default", "j7")
	line := job.Spec.Template.Spec.InitContainers[0].Command[2]

	assert.Contains(t, line,
		"--artifact-uri s3://continuo/core/rel-1/partial_parse.msgpack",
		"the artifact uploads beside its manifest")
	assert.Contains(t, line, "--partial-parse /project/target/partial_parse.msgpack",
		"the partial parse derives beside the manifest by default")
	assert.Contains(t, line, "--service-name core")
	assert.Contains(t, line, "--release-id rel-1")
	assert.Contains(t, line, "--image-tag abc123")
	assert.Contains(t, line, "--output-dir /shared")
	assert.Contains(t, line, `--controller-context "$CONTINUO_RUNTIME_CONTEXT_JSON"`,
		"the context expands from the env, so it is never quoted into the line")
}

func TestCreateCompileJob_ExporterAbsentFallsBackToManifestOnly(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, "")
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "j8", ReleaseID: "rel-1", NodeID: "core", ServiceName: "core",
		ImageTag: "abc123", ManifestS3URI: "s3://continuo/core/rel-1/manifest.json",
		Namespace: "default",
	}))
	line := fetchJob(t, c, "default", "j8").Spec.Template.Spec.InitContainers[0].Command[2]

	assert.Contains(t, line, "if [ -x /continuo/bin/continuo-export-runtime-manifest ]; then",
		"a team image without the exporter still compiles")
	assert.Contains(t, line, "else echo 'runtime exporter absent; manifest-only compatibility release'; fi")
}

func TestCreateCompileJob_InitCarriesRuntimeContext(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newDialectTestClient(t, "")
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "j9", ReleaseID: "rel-1", NodeID: "core", ServiceName: "core",
		ImageTag: "abc123", ManifestS3URI: "s3://continuo/core/rel-1/manifest.json",
		Namespace: "default",
	}))
	spec := fetchJob(t, c, "default", "j9").Spec.Template.Spec

	var ctx map[string]any
	require.NoError(t, json.Unmarshal(
		[]byte(initEnvByName(spec, "CONTINUO_RUNTIME_CONTEXT_JSON")), &ctx),
		"the exporter reads this as JSON")
	assert.NotEmpty(t, ctx["command_dialect_sha256"])
}

func TestCreateCompileJob_CustomPartialParsePath(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	const yaml = `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:            ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path:      "/project/target/manifest.json"
    partial_parse_path: "/var/cache/pp dir/partial_parse.msgpack"
`
	c := newDialectTestClient(t, yaml)
	require.NoError(t, c.CreateCompileJob(context.Background(), ValidationJobParams{
		JobName: "j10", ReleaseID: "rel-1", NodeID: "core", ServiceName: "core",
		ImageTag: "abc123", ManifestS3URI: "s3://continuo/core/rel-1/manifest.json",
		Namespace: "default",
	}))
	line := fetchJob(t, c, "default", "j10").Spec.Template.Spec.InitContainers[0].Command[2]

	assert.Contains(t, line, `--partial-parse '/var/cache/pp dir/partial_parse.msgpack'`,
		"a declared partial-parse path is honoured and shell-quoted")
}

func TestCreateCompileJob_RespectsS3SidecarImageEnv(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("S3_SIDECAR_IMAGE", "ghcr.io/acme/s3-sidecar:v2")
	c := newValidationTestClient()
	p := ValidationJobParams{
		JobName: "compile-svc-rel", ReleaseID: "rel-1", NodeID: "core",
		ServiceName: "core", ImageTag: "abc123",
		ManifestS3URI: "s3://continuo/core/rel-1/manifest.json", Namespace: "default",
	}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	job := fetchJob(t, c, p.Namespace, p.JobName)
	// env override used verbatim, NOT DOCKERHUB_USERNAME-prefixed
	assert.Equal(t, "ghcr.io/acme/s3-sidecar:v2", job.Spec.Template.Spec.Containers[0].Image)
}

func TestCreateCompileJob_EmptyImageTagErrors(t *testing.T) {
	c := newValidationTestClient()
	p := ValidationJobParams{JobName: "x", ServiceName: "core", NodeID: "core",
		ManifestS3URI: "s3://b/k", Namespace: "default"}
	require.Error(t, c.CreateCompileJob(context.Background(), p))
}
