package k8s

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
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
	require.Len(t, spec.InitContainers, 2)
	assert.Equal(t, "carolsimone/core:abc123", spec.InitContainers[1].Image) // team image
	assert.Contains(t, spec.InitContainers[1].Command[2], "dbt compile")
	assert.Contains(t, spec.InitContainers[1].Command[2], "/shared/manifest.json")
	// handoff file must be made world-readable: the init container runs the
	// team image (arbitrary uid/umask) but the upload container is forced to
	// runAsUser 65532, so a restrictive-mode file would EACCES on upload.
	assert.Contains(t, spec.InitContainers[1].Command[2], "&& chmod 644 /shared/manifest.json")
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
// carries the client's finished-Job TTL as its TTLSecondsAfterFinished, so
// Kubernetes deletes it (and its pod) that long after it finishes instead of
// leaving finished pods around.
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
	assert.Equal(t, testJobTTLSeconds, *job.Spec.TTLSecondsAfterFinished)
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
	assert.Equal(t, []string{"extras-script", "compile", "parse-prod", "parse-candidate"}, names)

	prod := spec.InitContainers[2]
	cand := spec.InitContainers[3]

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
// keeps the short layout: the "extras-script" and "compile" initContainers and
// an upload container carrying no PARSE_* env.
func TestBuildCompilePodSpec_NoParseLegWhenCandidateSchemaEmpty(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	t.Setenv("JOB_S3_SECRET_NAME", "continuo-job-s3")
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
	assert.Equal(t, []string{"extras-script", "compile"}, names)

	require.Len(t, spec.Containers, 1)
	assert.Len(t, spec.Containers[0].Env, 8,
		"upload env is the manifest pair, the compile extras pair and the S3 credential vars")
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

	require.Len(t, spec.InitContainers, 3)
	overlay := spec.InitContainers[0]
	assert.Equal(t, "overlay", overlay.Name)
	assert.Equal(t, "carolsimone/s3-sidecar:latest", overlay.Image)
	assert.Equal(t, []string{"python", "/overlay_fetcher.py"}, overlay.Command)
	assert.Equal(t, "s3://continuo/core/verify-rel-1-svc-a1/source-overlay.tar.gz", envOf(overlay, "SOURCE_OVERLAY_URI"))
	assert.Equal(t, "/shared/overlay", envOf(overlay, "OVERLAY_DEST"))
	assert.Equal(t, "shared", overlay.VolumeMounts[0].Name)

	extras := spec.InitContainers[1]
	assert.Equal(t, "extras-script", extras.Name)
	require.Len(t, extras.VolumeMounts, 1)
	assert.Equal(t, "shared", extras.VolumeMounts[0].Name)

	compile := spec.InitContainers[2]
	assert.Equal(t, "compile", compile.Name)
	assert.Equal(t, expectedStagePrefix+
		"dbt compile --profiles-dir /project"+
		" && cp /work/project/target/manifest.json /shared/manifest.json"+
		" && chmod 644 /shared/manifest.json"+
		wantExtrasSuffix(`"$PWD"`, "core", "verify-rel-1-svc-a1"), compile.Command[2],
		"the extras script reads the staged project, the directory the compile ran in")

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
	require.Equal(t, []string{"overlay", "extras-script", "compile", "parse-prod", "parse-candidate"}, names)

	parseProd := spec.InitContainers[3]
	parseCandidate := spec.InitContainers[4]
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
	for _, ic := range spec.InitContainers[:2] {
		require.Len(t, ic.VolumeMounts, 1, "container %s", ic.Name)
		assert.Equal(t, "shared", ic.VolumeMounts[0].Name, "container %s mounts only shared", ic.Name)
	}
	for _, ic := range spec.InitContainers[2:] {
		mounts := make([]string, len(ic.VolumeMounts))
		for i, m := range ic.VolumeMounts {
			mounts[i] = m.Name
		}
		assert.Equal(t, []string{"shared", "workdir-" + ic.Name}, mounts, "container %s", ic.Name)
	}
	assert.Len(t, spec.Containers[0].VolumeMounts, 1,
		"the upload container runs no dbt and needs no workdir")
}

func TestCreateCompileJob_NoOverlayRunsTheExtrasScriptThenCompile(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	c := newValidationTestClient()
	p := ValidationJobParams{JobName: "compile-svc-rel", ReleaseID: "rel-1", NodeID: "core",
		ServiceName: "core", ImageTag: "abc123", ManifestS3URI: "s3://continuo/core/rel-1/manifest.json", Namespace: "default"}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec
	require.Len(t, spec.InitContainers, 2)
	assert.Equal(t, "extras-script", spec.InitContainers[0].Name)
	compile := spec.InitContainers[1]
	assert.Equal(t, "compile", compile.Name)
	assert.False(t, strings.Contains(compile.Command[2], "/shared/overlay"))

	// A release without an overlay runs dbt in the image's own project dir:
	// no staging, no workdir.
	assert.Equal(t, "dbt compile --profiles-dir /project"+
		" && cp /project/target/manifest.json /shared/manifest.json"+
		" && chmod 644 /shared/manifest.json"+
		wantExtrasSuffix(`"$PWD"`, "core", "rel-1"), compile.Command[2])
	assert.NotContains(t, compile.Command[2], "/work")
	require.Len(t, spec.Volumes, 1)
	assert.Equal(t, "shared", spec.Volumes[0].Name)
	require.Len(t, compile.VolumeMounts, 1)
	assert.Equal(t, "shared", compile.VolumeMounts[0].Name)
}

func TestCreateCompileJob_CopiesAndRunsTheExtrasScript(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	c := newValidationTestClient()
	p := ValidationJobParams{JobName: "compile-core-rel", ReleaseID: "rel-1", NodeID: "core", ServiceName: "core",
		ImageTag: "abc123", ManifestS3URI: "s3://continuo/core/rel-1/manifest.json", Namespace: "default"}
	require.NoError(t, c.CreateCompileJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec
	require.Len(t, spec.InitContainers, 2)
	assert.Equal(t, "extras-script", spec.InitContainers[0].Name)
	assert.Equal(t, "carolsimone/s3-sidecar:latest", spec.InitContainers[0].Image)
	assert.Equal(t, []string{"cp", "/compile_extras.py", "/shared/compile_extras.py"}, spec.InitContainers[0].Command)
	cmd := spec.InitContainers[1].Command[2]
	assert.Contains(t, cmd, "command -v python3")
	assert.Contains(t, cmd, "python3 /shared/compile_extras.py --manifest /shared/manifest.json --project-dir \"$PWD\" --service core --release-id rel-1 --out /shared/compile-extras.json")
	assert.Contains(t, cmd, "chmod 644 /shared/compile-extras.json")
	assert.Equal(t, "/shared/compile-extras.json", envByName(spec, "COMPILE_EXTRAS_LOCAL_PATH"))
	assert.Equal(t, "s3://continuo/core/rel-1/compile-extras.json", envByName(spec, "COMPILE_EXTRAS_S3_URI"))
}

func TestCompileExtrasURI_RefusesAURIThatNamesNoManifest(t *testing.T) {
	_, err := compileExtrasURI("s3://continuo/core/rel-1/contract.yaml")
	require.ErrorIs(t, err, events.ErrPermanent)
}

func TestCompileExtrasURI_ReplacesTheLastSegment(t *testing.T) {
	got, err := compileExtrasURI("s3://continuo/core/rel-1/manifest.json")
	require.NoError(t, err)
	assert.Equal(t, "s3://continuo/core/rel-1/compile-extras.json", got)
}

// wantExtrasSuffix is the exact text the compile command gains after the
// manifest hand-off: the python3 guard, the extras script run against the
// project directory (projectDirWord, a shell word) and the chmod that makes
// its output readable by the upload container.
func wantExtrasSuffix(projectDirWord, service, releaseID string) string {
	return ` && { command -v python3 >/dev/null 2>&1 || { echo "compile: the team image has no python3 on PATH; continuo reads the seeds with the Python that runs dbt" >&2; exit 1; }; }` +
		" && python3 /shared/compile_extras.py --manifest /shared/manifest.json --project-dir " + projectDirWord +
		" --service " + shellQuote(service) + " --release-id " + shellQuote(releaseID) +
		" --out /shared/compile-extras.json && chmod 644 /shared/compile-extras.json"
}

// extrasCommandOf builds the compile pod for compileArgv and manifestPath
// (staged under a source overlay when overlay is true) and returns the compile
// container's shell command.
func extrasCommandOf(t *testing.T, compileArgv []string, manifestPath string, overlay bool) string {
	t.Helper()
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	p := ValidationJobParams{ServiceName: "core", ImageTag: "abc123", ReleaseID: "rel-1",
		ManifestS3URI: "s3://continuo/core/rel-1/manifest.json"}
	if overlay {
		p.SourceOverlayURI = "s3://continuo/core/rel-1/source-overlay.tar.gz"
	}
	spec, err := buildCompilePodSpec(p, compileArgv, manifestPath, nil, "")
	require.NoError(t, err)
	for _, ic := range spec.InitContainers {
		if ic.Name == "compile" {
			return ic.Command[2]
		}
	}
	t.Fatal("no compile container")
	return ""
}

// A compile whose --project-dir and --target-path point at unrelated
// directories: the extras step reads the project dbt compiled, not a directory
// derived from where the manifest was written.
func TestBuildCompilePodSpec_ExtrasReadTheCompileProjectDir(t *testing.T) {
	cmd := extrasCommandOf(t,
		[]string{"dbt", "compile", "--project-dir", "/srv/dbt", "--target-path", "/tmp/dbt-target"},
		"/tmp/dbt-target/manifest.json", false)
	assert.True(t, strings.HasSuffix(cmd, wantExtrasSuffix("/srv/dbt", "core", "rel-1")), cmd)
}

func TestBuildCompilePodSpec_ExtrasReadTheEqualsFormProjectDir(t *testing.T) {
	cmd := extrasCommandOf(t,
		[]string{"dbt", "compile", "--project-dir=/srv/dbt", "--target-path=/tmp/dbt-target"},
		"/tmp/dbt-target/manifest.json", false)
	assert.True(t, strings.HasSuffix(cmd, wantExtrasSuffix("/srv/dbt", "core", "rel-1")), cmd)
}

// dbt keeps the last --project-dir an invocation carries.
func TestBuildCompilePodSpec_ExtrasReadTheLastProjectDir(t *testing.T) {
	cmd := extrasCommandOf(t,
		[]string{"dbt", "compile", "--project-dir", "/first", "--project-dir=/srv/dbt"},
		"/srv/dbt/target/manifest.json", false)
	assert.True(t, strings.HasSuffix(cmd, wantExtrasSuffix("/srv/dbt", "core", "rel-1")), cmd)
}

// Without --project-dir dbt compiles the project in the container's working
// directory (the image's WORKDIR, /project in the dbt base image), which the
// extras step reads through the shell's $PWD.
func TestBuildCompilePodSpec_ExtrasReadTheWorkingDirWithoutAProjectDir(t *testing.T) {
	cmd := extrasCommandOf(t, []string{"dbt", "compile", "--profiles-dir", "/project"},
		"/project/target/manifest.json", false)
	assert.True(t, strings.HasSuffix(cmd, wantExtrasSuffix(`"$PWD"`, "core", "rel-1")), cmd)
}

// Under a source overlay the shell has moved into the staged copy before dbt
// runs, so $PWD names the staged project.
func TestBuildCompilePodSpec_ExtrasReadTheStagedProjectUnderAnOverlay(t *testing.T) {
	cmd := extrasCommandOf(t, []string{"dbt", "compile", "--profiles-dir", "/project"},
		"/project/target/manifest.json", true)
	assert.True(t, strings.HasPrefix(cmd, expectedStagePrefix), cmd)
	assert.True(t, strings.HasSuffix(cmd, wantExtrasSuffix(`"$PWD"`, "core", "rel-1")), cmd)
}

// dbt resolves a relative --project-dir against its working directory, which
// is the shell's $PWD when the extras step runs.
func TestBuildCompilePodSpec_ExtrasResolveARelativeProjectDirAgainstTheWorkingDir(t *testing.T) {
	cmd := extrasCommandOf(t, []string{"dbt", "compile", "--project-dir", "sub dir/dbt"},
		"/project/target/manifest.json", false)
	assert.True(t, strings.HasSuffix(cmd, wantExtrasSuffix(`"$PWD"/'sub dir/dbt'`, "core", "rel-1")), cmd)
}

// The project-dir word the extras step receives expands, in a real shell, to
// the directory dbt reads.
func TestExtrasProjectDirWord_ExpandsAsDbtResolvesTheProjectDir(t *testing.T) {
	work := t.TempDir()
	for _, tc := range []struct {
		argv []string
		want string
	}{
		{[]string{"dbt", "compile"}, work},
		{[]string{"dbt", "compile", "--project-dir", "sub dir/dbt"}, work + "/sub dir/dbt"},
		{[]string{"dbt", "compile", "--project-dir=/srv/it's"}, "/srv/it's"},
	} {
		cmd := exec.Command("sh", "-c", "printf %s "+extrasProjectDirWord(tc.argv)) //nolint:gosec // G204: the shell runs the word under test, built from the fixed argv above
		cmd.Dir = work
		out, err := cmd.Output()
		require.NoError(t, err)
		assert.Equal(t, tc.want, string(out), "argv %v", tc.argv)
	}
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
	assert.Contains(t, spec.InitContainers[2].Command[2],
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
