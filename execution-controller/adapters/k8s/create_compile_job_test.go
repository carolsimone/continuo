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

// TestCreateCompileJob_SourceOverlayStagesProjectInWorkdir verifies a
// verification run's compile Job fetches the proposed source into
// /shared/overlay and then runs dbt from a staged copy of the team project
// under /work — never writing the proposed files into the image's own
// project directory — and that the manifest it hands to the upload container
// is the one dbt wrote in that copy, not the configured path inside the
// image.
func TestCreateCompileJob_SourceOverlayStagesProjectInWorkdir(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	c := newValidationTestClient()
	p := ValidationJobParams{
		JobName: "compile-svc-verify", ReleaseID: "verify-rel-1-svc-a1", NodeID: "core",
		ServiceName: "core", ImageTag: "abc123",
		ManifestS3URI:    "s3://continuo/core/verify-rel-1-svc-a1/manifest.json",
		SourceOverlayURI: "s3://continuo/core/verify-rel-1-svc-a1/source-overlay.tar.gz",
		Namespace:        "default",
	}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec

	require.Len(t, spec.InitContainers, 2)
	overlay := spec.InitContainers[0]
	assert.Equal(t, "overlay", overlay.Name)
	assert.Equal(t, "carolsimone/s3-sidecar:latest", overlay.Image)
	assert.Equal(t, []string{"python", "/overlay_fetcher.py"}, overlay.Command)
	assert.Equal(t, "s3://continuo/core/verify-rel-1-svc-a1/source-overlay.tar.gz", envOf(overlay, "SOURCE_OVERLAY_URI"))
	assert.Equal(t, "/shared/overlay", envOf(overlay, "OVERLAY_DEST"))
	assert.Equal(t, "shared", overlay.VolumeMounts[0].Name)

	compile := spec.InitContainers[1]
	assert.Equal(t, "compile", compile.Name)
	assert.Equal(t, expectedStagePrefix+
		"dbt compile --profiles-dir /project"+
		" && cp /work/project/target/manifest.json /shared/manifest.json"+
		" && chmod 644 /shared/manifest.json", compile.Command[2])

	mounts := make([]string, len(compile.VolumeMounts))
	for i, m := range compile.VolumeMounts {
		mounts[i] = m.Name
	}
	assert.Equal(t, []string{"shared", "workdir-compile"}, mounts)
	assert.Equal(t, "/work", compile.VolumeMounts[1].MountPath)

	volumes := make([]string, len(spec.Volumes))
	for i, v := range spec.Volumes {
		volumes[i] = v.Name
	}
	assert.Equal(t, []string{"shared", "workdir-compile"}, volumes)
}

// TestBuildCompilePodSpec_SourceOverlayAppliesToParseContainers verifies that
// when a verification run's overlay is set alongside CandidateSchema, every
// team-image container stages the project into its own workdir — not just
// compile — so parse rehearsal exercises the overlaid source rather than the
// pristine checked-in project, and reads back the partial-parse artifact from
// the copy dbt actually wrote it into. Each staging container gets its own
// workdir emptyDir so one container's dbt output never leaks into the next
// container's project.
func TestBuildCompilePodSpec_SourceOverlayAppliesToParseContainers(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	p := ValidationJobParams{
		ServiceName:         "core",
		ImageTag:            "abc123",
		ManifestS3URI:       "s3://continuo/core/verify-rel-1/manifest.json",
		SourceOverlayURI:    "s3://continuo/core/verify-rel-1/source-overlay.tar.gz",
		CandidateSchema:     "candidate_x",
		ParseProdS3URI:      "s3://continuo/core/verify-rel-1/parse/prod.msgpack",
		ParseCandidateS3URI: "s3://continuo/core/verify-rel-1/parse/candidate.msgpack",
	}
	spec, err := buildCompilePodSpec(p, []string{"dbt", "compile"}, "/project/target/manifest.json",
		[]string{"dbt", "parse"}, "/project/target/partial_parse.msgpack")
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
	assert.True(t, strings.HasPrefix(parseProd.Command[2], expectedStagePrefix), parseProd.Command[2])
	assert.True(t, strings.HasPrefix(parseCandidate.Command[2], expectedStagePrefix), parseCandidate.Command[2])
	assert.Contains(t, parseProd.Command[2], "cp /work/project/target/partial_parse.msgpack /shared/parse/prod/partial_parse.msgpack",
		"the exported artifact must be read from the staged copy dbt wrote it into")
	assert.NotContains(t, parseProd.Command[2], "[ -f /project/target/partial_parse.msgpack ]",
		"nothing may be read back from the image's own project dir once dbt ran in the workdir")

	volumes := make([]string, len(spec.Volumes))
	for i, v := range spec.Volumes {
		volumes[i] = v.Name
	}
	assert.Equal(t, []string{"shared", "workdir-compile", "workdir-parse-prod", "workdir-parse-candidate"}, volumes,
		"each staging container needs its own workdir so dbt output does not leak between them")
	for _, ic := range spec.InitContainers[1:] {
		mounts := make([]string, len(ic.VolumeMounts))
		for i, m := range ic.VolumeMounts {
			mounts[i] = m.Name
		}
		assert.Equal(t, []string{"shared", "workdir-" + ic.Name}, mounts, "container %s", ic.Name)
	}
	assert.Len(t, spec.Containers[0].VolumeMounts, 1,
		"the upload container runs no dbt and needs no workdir")
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

	// A release without an overlay runs dbt in the image's own project dir,
	// exactly as it did before the overlay existed: no staging, no workdir.
	assert.Equal(t, "dbt compile --profiles-dir /project"+
		" && cp /project/target/manifest.json /shared/manifest.json"+
		" && chmod 644 /shared/manifest.json", spec.InitContainers[0].Command[2])
	assert.NotContains(t, spec.InitContainers[0].Command[2], "/work")
	require.Len(t, spec.Volumes, 1)
	assert.Equal(t, "shared", spec.Volumes[0].Name)
	require.Len(t, spec.InitContainers[0].VolumeMounts, 1)
	assert.Equal(t, "shared", spec.InitContainers[0].VolumeMounts[0].Name)
}

// TestBuildCompilePodSpec_NoOverlayParseCommandsUnchanged pins the parse-leg
// commands of an ordinary release: they read the partial-parse artifact back
// from the configured path inside the team image, with no staging prologue.
func TestBuildCompilePodSpec_NoOverlayParseCommandsUnchanged(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	p := ValidationJobParams{
		ServiceName:     "core",
		ImageTag:        "abc123",
		ManifestS3URI:   "s3://continuo/core/rel-1/manifest.json",
		CandidateSchema: "candidate_x",
	}
	spec, err := buildCompilePodSpec(p, []string{"dbt", "compile"}, "/project/target/manifest.json",
		[]string{"dbt", "parse"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)

	for _, ic := range spec.InitContainers {
		assert.NotContains(t, ic.Command[2], "/work", "container %s must not stage anything", ic.Name)
		assert.NotContains(t, ic.Command[2], "/shared/overlay", "container %s", ic.Name)
		mounts := make([]string, len(ic.VolumeMounts))
		for i, m := range ic.VolumeMounts {
			mounts[i] = m.Name
		}
		assert.Equal(t, []string{"shared"}, mounts, "container %s", ic.Name)
	}
	assert.Contains(t, spec.InitContainers[1].Command[2],
		"cp /project/target/partial_parse.msgpack /shared/parse/prod/partial_parse.msgpack")
	require.Len(t, spec.Volumes, 1)
	assert.Equal(t, "shared", spec.Volumes[0].Name)
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
