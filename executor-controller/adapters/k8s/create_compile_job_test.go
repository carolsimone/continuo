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
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
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
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
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

// TestCreateCompileJob_SetsTTLSecondsAfterFinished verifies that a compile Job
// carries a TTLSecondsAfterFinished so Kubernetes garbage-collects it (and its
// pod) after it terminates, instead of leaving failed pods around forever.
func TestCreateCompileJob_SetsTTLSecondsAfterFinished(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	c := newValidationTestClient()
	p := ValidationJobParams{
		JobName: "compile-svc-rel-ttl", ReleaseID: "rel-1", NodeID: "core",
		ServiceName: "core", ImageTag: "abc123",
		ManifestS3URI: "s3://continuo/core/rel-1/manifest.json", Namespace: "default",
	}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.NotNil(t, job.Spec.TTLSecondsAfterFinished, "job must set TTLSecondsAfterFinished")
	assert.Equal(t, jobTTLSecondsAfterFinished, *job.Spec.TTLSecondsAfterFinished)
}

func TestCreateCompileJob_EmptyImageTagErrors(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
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
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
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

	assert.Equal(t, wantWarehouseEnvFrom(), prod.EnvFrom,
		"parse-prod connection comes from the warehouse Secret alone")
	assert.Empty(t, prod.Env, "parse-prod carries no inline env, in particular no DBT_TARGET_SCHEMA")
	assert.Equal(t, wantWarehouseEnvFrom(), cand.EnvFrom)
	assert.Equal(t, []corev1.EnvVar{{Name: "DBT_TARGET_SCHEMA", Value: p.CandidateSchema}}, cand.Env,
		"parse-candidate inline env is exactly DBT_TARGET_SCHEMA")

	for ctx, ctr := range map[string]corev1.Container{"prod": prod, "candidate": cand} {
		require.Len(t, ctr.Command, 3)
		line := ctr.Command[2]
		assert.Equal(t, 2, strings.Count(line, "dbt parse"),
			"ctx %s: parse argv must appear twice (export + rehearsal)", ctx)
		assert.Contains(t, line, partialParsePath)
		assert.Contains(t, line, "Unable to do partial parsing")
		assert.Contains(t, line, "/shared/parse/"+ctx)
		assert.Contains(t, line, "exit 42")
		assert.Contains(t, line, "exit 43")
		assert.Contains(t, line, "exit 44")
		assert.Contains(t, line, "exit 45")
		assert.Contains(t, line, "exit 46")
		assert.Contains(t, line, "partial parsing is DISABLED in this project")
		assert.Contains(t, line, "continuo parse-rehearsal FAILED")
		assert.Contains(t, line, "chmod 755")
		assert.Contains(t, line, "DBT_LOG_LEVEL=debug")
		assert.Contains(t, line, "skipping partial parsing")
	}

	require.Len(t, spec.Containers, 1)
	assert.Equal(t, "/shared/parse/prod/partial_parse.msgpack", envByName(spec, "PARSE_PROD_LOCAL_PATH"))
	assert.Equal(t, p.ParseProdS3URI, envByName(spec, "PARSE_PROD_S3_URI"))
	assert.Equal(t, "/shared/parse/candidate/partial_parse.msgpack", envByName(spec, "PARSE_CANDIDATE_LOCAL_PATH"))
	assert.Equal(t, p.ParseCandidateS3URI, envByName(spec, "PARSE_CANDIDATE_S3_URI"))
}

// TestBuildCompilePodSpec_NoParseLegWhenCandidateSchemaEmpty verifies that
// when CandidateSchema is empty the parse-export leg is disabled and the pod
// keeps the two-container layout: a single "compile" initContainer and an
// upload container carrying no PARSE_* env.
func TestBuildCompilePodSpec_NoParseLegWhenCandidateSchemaEmpty(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
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

func TestCreateCompileJob_SourceOverlayAddsFetcherAndCopiesOverProject(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	c := newValidationTestClient()
	p := ValidationJobParams{
		JobName: "compile-svc-shadow", ReleaseID: "shadow-rel-1-svc-a1", NodeID: "core",
		ServiceName: "core", ImageTag: "abc123",
		ManifestS3URI:    "s3://continuo/core/shadow-rel-1-svc-a1/manifest.json",
		SourceOverlayURI: "s3://continuo/core/shadow-rel-1-svc-a1/source-overlay.tar.gz",
		Namespace:        "default",
	}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec

	require.Len(t, spec.InitContainers, 2)
	overlay := spec.InitContainers[0]
	assert.Equal(t, "overlay", overlay.Name)
	assert.Equal(t, "carolsimone/s3-sidecar:latest", overlay.Image)
	assert.Equal(t, []string{"python", "/overlay_fetcher.py"}, overlay.Command)
	assert.Equal(t, "s3://continuo/core/shadow-rel-1-svc-a1/source-overlay.tar.gz", envOf(overlay, "SOURCE_OVERLAY_URI"))
	assert.Equal(t, "/shared/overlay", envOf(overlay, "OVERLAY_DEST"))
	assert.Equal(t, "shared", overlay.VolumeMounts[0].Name)

	compile := spec.InitContainers[1]
	assert.Equal(t, "compile", compile.Name)
	assert.True(t, strings.HasPrefix(compile.Command[2], "cp -R /shared/overlay/. ./ && "), compile.Command[2])
}

// TestBuildCompilePodSpec_SourceOverlayAppliesToParseContainers verifies that
// when a shadow release's overlay is set alongside CandidateSchema, the
// overlay copy prefix lands on the parse-prod and parse-candidate commands
// too — not just compile — so parse rehearsal exercises the overlaid source
// rather than the pristine checked-in project.
func TestBuildCompilePodSpec_SourceOverlayAppliesToParseContainers(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	p := ValidationJobParams{
		ServiceName:         "core",
		ImageTag:            "abc123",
		ManifestS3URI:       "s3://continuo/core/shadow-rel-1/manifest.json",
		SourceOverlayURI:    "s3://continuo/core/shadow-rel-1/source-overlay.tar.gz",
		CandidateSchema:     "candidate_x",
		ParseProdS3URI:      "s3://continuo/core/shadow-rel-1/parse/prod.msgpack",
		ParseCandidateS3URI: "s3://continuo/core/shadow-rel-1/parse/candidate.msgpack",
	}
	spec, err := buildCompilePodSpec(p, []string{"dbt", "compile"}, "target/manifest.json",
		[]string{"dbt", "parse"}, "target/partial_parse.msgpack")
	require.NoError(t, err)

	names := make([]string, len(spec.InitContainers))
	for i, ic := range spec.InitContainers {
		names[i] = ic.Name
	}
	require.Equal(t, []string{"overlay", "compile", "parse-prod", "parse-candidate"}, names)

	parseProd := spec.InitContainers[2]
	parseCandidate := spec.InitContainers[3]
	require.Len(t, parseProd.Command, 3)
	require.Len(t, parseCandidate.Command, 3)
	assert.True(t, strings.HasPrefix(parseProd.Command[2], "cp -R /shared/overlay/. ./ && "), parseProd.Command[2])
	assert.True(t, strings.HasPrefix(parseCandidate.Command[2], "cp -R /shared/overlay/. ./ && "), parseCandidate.Command[2])
}

func TestCreateCompileJob_NoOverlayKeepsSingleInitContainer(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	c := newValidationTestClient()
	p := ValidationJobParams{JobName: "compile-svc-rel", ReleaseID: "rel-1", NodeID: "core",
		ServiceName: "core", ImageTag: "abc123", ManifestS3URI: "s3://continuo/core/rel-1/manifest.json", Namespace: "default"}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec
	require.Len(t, spec.InitContainers, 1)
	assert.Equal(t, "compile", spec.InitContainers[0].Name)
	assert.False(t, strings.Contains(spec.InitContainers[0].Command[2], "/shared/overlay"))
}

// envOf returns the value of the named env var on one container ("" when absent).
func envOf(c corev1.Container, name string) string {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
