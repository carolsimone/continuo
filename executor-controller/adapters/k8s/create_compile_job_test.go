package k8s

import (
	"context"
	"strings"
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
	assert.Equal(t, "carolsimone/core:abc123", spec.InitContainers[0].Image) // team image
	assert.Contains(t, spec.InitContainers[0].Command[2], "dbt compile")
	assert.Contains(t, spec.InitContainers[0].Command[2], "/shared/manifest.json")
	// handoff file must be made world-readable: the init container runs the
	// team image (arbitrary uid/umask) but the upload container is forced to
	// runAsUser 65532, so a restrictive-mode file would EACCES on upload.
	assert.Contains(t, spec.InitContainers[0].Command[2], "&& chmod 644 /shared/manifest.json")
	assert.Equal(t, "carolsimone/s3-sidecar:latest", spec.Containers[0].Image) // upload image
	assert.Equal(t, []string{"python", "/compile_uploader.py"}, spec.Containers[0].Command)
	assert.Equal(t, "/shared/manifest.json", envByName(spec, "COMPILE_MANIFEST_PATH"))
	assert.Equal(t, "s3://continuo/core/rel-1/manifest.json", envByName(spec, "MANIFEST_S3_URI"))
	assert.Equal(t, "compile", job.Spec.Template.Labels["mode"])
	// shared emptyDir mounted in both
	assert.Equal(t, "shared", spec.Volumes[0].Name)
	require.NotNil(t, spec.Volumes[0].EmptyDir)
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

// TestBuildCompilePodSpec_ParseContainers verifies the parse-export/rehearsal
// leg that is added to the compile pod whenever CandidateSchema is populated:
// two extra team-image initContainers (parse-prod, parse-candidate) that run
// the resolved parse argv twice per context, and four PARSE_* env vars added
// to the upload container so it also ships the exported msgpacks to S3.
func TestBuildCompilePodSpec_ParseContainers(t *testing.T) {
	p := ValidationJobParams{
		ServiceName:         "core",
		ImageTag:            "abc123",
		ManifestS3URI:       "s3://continuo/core/rel-1/manifest.json",
		CandidateSchema:     "_candidate_rel1",
		ParseProdS3URI:      "s3://continuo/core/rel-1/parse/prod.msgpack",
		ParseCandidateS3URI: "s3://continuo/core/rel-1/parse/candidate.msgpack",
	}
	parseArgv := []string{"dbt", "parse"}
	partialParsePath := "target/partial_parse.msgpack"

	spec, err := buildCompilePodSpec(p, []string{"dbt", "compile"}, "target/manifest.json", parseArgv, partialParsePath)
	require.NoError(t, err)

	names := make([]string, len(spec.InitContainers))
	for i, ic := range spec.InitContainers {
		names[i] = ic.Name
	}
	assert.Equal(t, []string{"compile", "parse-prod", "parse-candidate"}, names)

	prod := spec.InitContainers[1]
	cand := spec.InitContainers[2]

	assert.Equal(t, dbtConnectionEnvVars(), prod.Env, "parse-prod env must be exactly dbtConnectionEnvVars(), no DBT_TARGET_SCHEMA")
	wantCandEnv := append(dbtConnectionEnvVars(), corev1.EnvVar{Name: "DBT_TARGET_SCHEMA", Value: p.CandidateSchema})
	assert.Equal(t, wantCandEnv, cand.Env)

	for ctx, ctr := range map[string]corev1.Container{"prod": prod, "candidate": cand} {
		require.Len(t, ctr.Command, 3)
		line := ctr.Command[2]
		assert.Equal(t, 2, strings.Count(line, "dbt parse"),
			"ctx %s: parse argv must appear twice (export + rehearsal)", ctx)
		assert.Contains(t, line, partialParsePath)
		assert.Contains(t, line, "Unable to do partial parsing")
		assert.Contains(t, line, "/shared/parse/"+ctx)
	}

	require.Len(t, spec.Containers, 1)
	assert.Equal(t, "/shared/parse/prod/partial_parse.msgpack", envByName(spec, "PARSE_PROD_LOCAL_PATH"))
	assert.Equal(t, p.ParseProdS3URI, envByName(spec, "PARSE_PROD_S3_URI"))
	assert.Equal(t, "/shared/parse/candidate/partial_parse.msgpack", envByName(spec, "PARSE_CANDIDATE_LOCAL_PATH"))
	assert.Equal(t, p.ParseCandidateS3URI, envByName(spec, "PARSE_CANDIDATE_S3_URI"))
}

// TestBuildCompilePodSpec_NoParseLegWhenCandidateSchemaEmpty verifies that
// with no CandidateSchema (older compile.requested messages predating the
// parse-free feature) the pod stays byte-identical in structure to the
// pre-feature two-container pod: a single "compile" initContainer and an
// upload container carrying no PARSE_* env.
func TestBuildCompilePodSpec_NoParseLegWhenCandidateSchemaEmpty(t *testing.T) {
	p := ValidationJobParams{
		ServiceName:   "core",
		ImageTag:      "abc123",
		ManifestS3URI: "s3://continuo/core/rel-1/manifest.json",
	}
	spec, err := buildCompilePodSpec(p, []string{"dbt", "compile"}, "target/manifest.json",
		[]string{"dbt", "parse"}, "target/partial_parse.msgpack")
	require.NoError(t, err)

	names := make([]string, len(spec.InitContainers))
	for i, ic := range spec.InitContainers {
		names[i] = ic.Name
	}
	assert.Equal(t, []string{"compile"}, names)

	require.Len(t, spec.Containers, 1)
	assert.Len(t, spec.Containers[0].Env, 6, "upload env must be the pre-feature 6 vars only")
	for _, e := range spec.Containers[0].Env {
		assert.False(t, strings.HasPrefix(e.Name, "PARSE_"), "no PARSE_* env when CandidateSchema is empty, got %s", e.Name)
	}
}
