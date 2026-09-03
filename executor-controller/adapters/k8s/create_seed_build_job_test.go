package k8s

import (
	"context"
	"encoding/json"
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

// TestCreateSeedBuildJob_SetsTTLSecondsAfterFinished verifies that a seed-build
// Job carries a TTLSecondsAfterFinished so Kubernetes garbage-collects it (and
// its pod) after it terminates, instead of leaving failed pods around forever.
func TestCreateSeedBuildJob_SetsTTLSecondsAfterFinished(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	c := newValidationTestClient()
	p := ValidationJobParams{
		JobName: "seedbuild-fx-ttl", ReleaseID: "r", NodeID: "seed.core.fx",
		ServiceName: "core", SchemaName: "analytics", TableName: "fx",
		NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "abc123",
		CandidateSchema: "_candidate_r", Namespace: "default",
	}
	require.NoError(t, c.CreateSeedBuildJob(context.Background(), p))
	job := fetchJob(t, c, p.Namespace, p.JobName)
	require.NotNil(t, job.Spec.TTLSecondsAfterFinished, "job must set TTLSecondsAfterFinished")
	assert.Equal(t, jobTTLSecondsAfterFinished, *job.Spec.TTLSecondsAfterFinished)
}

func TestCreateSeedBuildJob_EmptyImageTagErrors(t *testing.T) {
	c := newValidationTestClient()
	p := ValidationJobParams{JobName: "x", ServiceName: "core", TableName: "fx",
		NodeType: pkg_model.NodeTypeDbtSeed, CandidateSchema: "_candidate_r", Namespace: "default"}
	require.Error(t, c.CreateSeedBuildJob(context.Background(), p))
}

// seedOverlayParams returns seed-build params for a verification run
// verifying a proposed fix to a team's seed CSV.
func seedOverlayParams() ValidationJobParams {
	return ValidationJobParams{
		JobName: "seedbuild-fx-verify", ReleaseID: "verify-rel-1-core-a1", NodeID: "seed.core.fx",
		ServiceName: "core", SchemaName: "analytics", TableName: "fx",
		NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "abc123",
		CandidateSchema:  "_candidate_verify_rel_1",
		SourceOverlayURI: "s3://continuo/core/verify-rel-1-core-a1/source-overlay.tar.gz",
		Namespace:        "default",
	}
}

// TestCreateSeedBuildJob_SourceOverlayStagesProjectInWorkdir verifies the seed
// leg gets the same overlay treatment as the compile leg: an "overlay"
// initContainer fetches the proposed source into /shared/overlay, and the team
// image's seed command stages the project into the writable workdir emptyDir,
// lays the proposed files over the copy, and runs dbt from there. Without this,
// a proposed fix to a seed CSV is verified against the very file it replaces
// and can never pass.
func TestCreateSeedBuildJob_SourceOverlayStagesProjectInWorkdir(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	t.Setenv("S3_BUCKET", "")
	c := newValidationTestClient()
	p := seedOverlayParams()
	require.NoError(t, c.CreateSeedBuildJob(context.Background(), p))
	spec := fetchJob(t, c, p.Namespace, p.JobName).Spec.Template.Spec

	require.Len(t, spec.InitContainers, 1)
	overlay := spec.InitContainers[0]
	assert.Equal(t, "overlay", overlay.Name)
	assert.Equal(t, "carolsimone/s3-sidecar:latest", overlay.Image)
	assert.Equal(t, []string{"python", "/overlay_fetcher.py"}, overlay.Command)
	assert.Equal(t, p.SourceOverlayURI, envOf(overlay, "SOURCE_OVERLAY_URI"))
	assert.Equal(t, "/shared/overlay", envOf(overlay, "OVERLAY_DEST"))
	require.Len(t, overlay.VolumeMounts, 1)
	assert.Equal(t, "shared", overlay.VolumeMounts[0].Name)
	for _, e := range s3CredEnvVars() {
		assert.Equal(t, e.Value, envOf(overlay, e.Name), "overlay container must forward s3CredEnvVars()")
	}

	require.Len(t, spec.Containers, 1)
	team := spec.Containers[0]
	assert.Equal(t, []string{"sh", "-c", expectedStagePrefix + "dbt seed --select fx"}, team.Command,
		"the seed command must run against the staged copy, never against the image's own project dir")

	mounts := make([]string, len(team.VolumeMounts))
	for i, m := range team.VolumeMounts {
		mounts[i] = m.Name
	}
	assert.Contains(t, mounts, "shared", "the team container must see /shared to read the overlay")
	assert.Contains(t, mounts, "workdir-dbt-job", "the team container needs a writable workdir to stage into")
	for _, m := range team.VolumeMounts {
		if m.Name == "workdir-dbt-job" {
			assert.Equal(t, "/work", m.MountPath)
		}
	}

	names := make([]string, len(spec.Volumes))
	for i, v := range spec.Volumes {
		names[i] = v.Name
	}
	assert.Contains(t, names, "shared")
	assert.Contains(t, names, "workdir-dbt-job")
	for _, v := range spec.Volumes {
		if v.Name == "workdir-dbt-job" {
			assert.NotNil(t, v.EmptyDir, "the workdir must be an emptyDir")
		}
	}
}

// expectedStagePrefix is the shell prologue every team-image command carries
// when the Job runs under a source overlay. It stages the image's project
// directory (the shell's starting directory) into the workdir emptyDir at the
// same absolute path under /work, lays the proposed files over the copy, and
// moves the shell there — so the proposed fix is never written into the image's
// own project directory, whoever owns it.
const expectedStagePrefix = `mkdir -p "/work${PWD%/*}" && cp -R "$PWD" "/work${PWD%/*}/" && ` +
	`cp -R /shared/overlay/. "/work$PWD/" && cd "/work$PWD" && `

// TestBuildSeedBuildPodSpec_OverlayAndParseCacheCoexist verifies that when both
// the overlay and the parse-cache hydration apply, the overlay fetcher runs
// first and both volumes/mounts survive — neither leg overwrites the other's
// wiring.
func TestBuildSeedBuildPodSpec_OverlayAndParseCacheCoexist(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	t.Setenv("S3_BUCKET", "continuo")
	spec, err := buildSeedBuildPodSpec(seedOverlayParams(),
		[]string{"dbt", "seed", "--select", "fx"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)

	names := make([]string, len(spec.InitContainers))
	for i, ic := range spec.InitContainers {
		names[i] = ic.Name
	}
	require.Equal(t, []string{"overlay", "hydrate-parse-cache"}, names,
		"the overlay must be laid down before the parse cache is hydrated")

	volNames := make([]string, len(spec.Volumes))
	for i, v := range spec.Volumes {
		volNames[i] = v.Name
	}
	assert.ElementsMatch(t, []string{"shared", "workdir-dbt-job", "parse-cache"}, volNames)

	mountNames := make([]string, len(spec.Containers[0].VolumeMounts))
	for i, m := range spec.Containers[0].VolumeMounts {
		mountNames[i] = m.Name
	}
	assert.ElementsMatch(t, []string{"shared", "workdir-dbt-job", "parse-cache"}, mountNames)
	assert.Equal(t, []string{"sh", "-c", expectedStagePrefix + "dbt seed --select fx"},
		spec.Containers[0].Command)
}

// seedPodSpecWithoutOverlayJSON is the seed-build PodSpec a production release
// produced before the source-overlay feature existed, captured verbatim. A
// release without an overlay must still produce exactly this: the overlay leg
// is additive, never a rewrite of the ordinary seed Job.
const seedPodSpecWithoutOverlayJSON = `{"containers":[{"name":"dbt-job","image":"carolsimone/core:abc123","command":["dbt","seed","--select","fx"],"envFrom":[{"secretRef":{"name":"wh-secret"}}],"env":[{"name":"RELEASE_ID","value":"rel-1"},{"name":"NODE_ID","value":"seed.core.fx"},{"name":"SERVICE_NAME","value":"core"},{"name":"SCHEMA","value":"analytics"},{"name":"TABLE_NAME","value":"fx"},{"name":"JOB_NAME","value":"seedbuild-fx"},{"name":"DBT_TARGET_SCHEMA","value":"_candidate_rel_1"}],"resources":{},"imagePullPolicy":"IfNotPresent","securityContext":{"capabilities":{"drop":["ALL"]},"allowPrivilegeEscalation":false}}],"restartPolicy":"Never","securityContext":{"seccompProfile":{"type":"RuntimeDefault"}}}`

func TestBuildSeedBuildPodSpec_NoOverlayIsByteIdentical(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh-secret")
	t.Setenv("S3_BUCKET", "")
	p := ValidationJobParams{
		JobName: "seedbuild-fx", ReleaseID: "rel-1", NodeID: "seed.core.fx",
		ServiceName: "core", SchemaName: "analytics", TableName: "fx",
		NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "abc123",
		CandidateSchema: "_candidate_rel_1", Namespace: "default",
	}
	spec, err := buildSeedBuildPodSpec(p, []string{"dbt", "seed", "--select", "fx"}, "target/partial_parse.msgpack")
	require.NoError(t, err)

	got, err := json.Marshal(spec)
	require.NoError(t, err)
	assert.JSONEq(t, seedPodSpecWithoutOverlayJSON, string(got))
	assert.Equal(t, seedPodSpecWithoutOverlayJSON, string(got),
		"a seed Job without an overlay must be byte-identical to the pre-overlay spec")
}
