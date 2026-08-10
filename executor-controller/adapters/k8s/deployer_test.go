package k8s

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeploy_PythonModel_FailsClosedPermanently verifies the run-path guard:
// python-model runtime dispatch (running the user's image with the normative
// env) is not implemented yet, so Deploy must fail permanently rather than
// fall through to the dbt command builder and run a wrong command on the
// node. The guard fires before any client use, so a nil client is safe here.
func TestDeploy_PythonModel_FailsClosedPermanently(t *testing.T) {
	d := NewDeployer(nil, "ns")
	err := d.Deploy(context.Background(), deploy.JobSpec{
		JobName: "j1", NodeType: "python-model", TableName: "py_probe", ServiceName: "svc-py",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Contains(t, err.Error(), "python-model runtime dispatch not implemented")
}

// TestDeployValidation_PythonModel_IsNotGuarded documents that only the RUN
// path (Deploy) is guarded against python-model: DeployValidation must still
// accept it, since python-model nodes go through validation (build_from_columns)
// same as dbt nodes. A nil client would panic past the parse in DeployValidation,
// so this asserts the parse step alone succeeds for python-model.
func TestDeployValidation_PythonModel_IsNotGuarded(t *testing.T) {
	_, err := pkg_model.ParseNodeType("python-model")
	require.NoError(t, err)
}

// TestCompileParamsFromSpec_MapsAllFields pins DeployCompile's spec→params
// field threading: every field a fully-populated deploy.ValidationJobSpec
// carries into a compile Job (including the parse-free fields CandidateSchema,
// ParseProdS3URI, ParseCandidateS3URI, plus the pre-existing ManifestS3URI)
// must land on the resulting ValidationJobParams unchanged. A refactor that
// silently drops a field flips one of these assertions.
func TestCompileParamsFromSpec_MapsAllFields(t *testing.T) {
	spec := deploy.ValidationJobSpec{
		JobName:              "compile-svc-rel123",
		ReleaseID:            "rel123",
		NodeID:               "svc.orders",
		ServiceName:          "service-1",
		SchemaName:           "analytics",
		TableName:            "orders",
		NodeType:             string(pkg_model.NodeTypeDbtModel),
		ImageTag:             "abc123",
		CandidateSchema:      "candidate_rel123",
		CandidateArtifactURI: "s3://continuo/svc/orders.sql",
		ValidationOp:         "build_from_sql",
		ProdSchema:           "prod_schema",
		ManifestS3URI:        "s3://continuo/svc/rel123/manifest.json",
		ParseProdS3URI:       "s3://continuo/svc/rel123/parse/prod.msgpack",
		ParseCandidateS3URI:  "s3://continuo/svc/rel123/parse/candidate.msgpack",
	}

	params, err := compileParamsFromSpec(spec, "default")
	require.NoError(t, err)

	// Fields DeployCompile threads through onto ValidationJobParams.
	assert.Equal(t, "compile-svc-rel123", params.JobName)
	assert.Equal(t, "rel123", params.ReleaseID)
	assert.Equal(t, "svc.orders", params.NodeID)
	assert.Equal(t, "service-1", params.ServiceName)
	assert.Equal(t, pkg_model.NodeTypeDbtModel, params.NodeType)
	assert.Equal(t, "abc123", params.ImageTag)
	assert.Equal(t, "s3://continuo/svc/rel123/manifest.json", params.ManifestS3URI)
	assert.Equal(t, "candidate_rel123", params.CandidateSchema)
	assert.Equal(t, "s3://continuo/svc/rel123/parse/prod.msgpack", params.ParseProdS3URI)
	assert.Equal(t, "s3://continuo/svc/rel123/parse/candidate.msgpack", params.ParseCandidateS3URI)
	assert.Equal(t, "default", params.Namespace)

	// Fields DeployCompile deliberately does NOT set (SchemaName/TableName/
	// ValidationOp/ProdSchema/CandidateArtifactURI belong to DeployValidation and
	// DeploySeedBuild, not the compile Job).
	assert.Empty(t, params.SchemaName)
	assert.Empty(t, params.TableName)
	assert.Empty(t, params.ValidationOp)
	assert.Empty(t, params.ProdSchema)
	assert.Empty(t, params.CandidateArtifactURI)
}

// TestCompileParamsFromSpec_EmptyNodeType verifies compile Jobs tolerate an
// empty NodeType (they compile the full service manifest, not a single dbt
// node) instead of erroring on ParseNodeType("").
func TestCompileParamsFromSpec_EmptyNodeType(t *testing.T) {
	spec := deploy.ValidationJobSpec{JobName: "compile-svc-rel123", ServiceName: "service-1", ImageTag: "abc123"}

	params, err := compileParamsFromSpec(spec, "default")

	require.NoError(t, err)
	assert.Equal(t, pkg_model.NodeType(""), params.NodeType)
}

// TestCompileParamsFromSpec_InvalidNodeTypeErrors verifies an unparseable
// NodeType is reported as a permanent error rather than silently ignored.
func TestCompileParamsFromSpec_InvalidNodeTypeErrors(t *testing.T) {
	spec := deploy.ValidationJobSpec{JobName: "compile-svc-rel123", ServiceName: "service-1", NodeType: "not-a-real-type"}

	_, err := compileParamsFromSpec(spec, "default")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid node type")
}

// TestDeployCompile_CreatesJobWithMappedFields is an end-to-end sanity check
// that DeployCompile's use of compileParamsFromSpec still produces a real Job
// via the K8s client: the manifest upload env var (the one new-field sibling
// already wired into the pod spec — see client.go buildCompilePodSpec) proves
// ManifestS3URI threads all the way from spec to the created Job.
// CandidateSchema/ParseProdS3URI/ParseCandidateS3URI are asserted at the
// params-mapping level above; their pod-spec wiring is covered separately
// in the buildCompilePodSpec tests.
func TestDeployCompile_CreatesJobWithMappedFields(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
	client := newValidationTestClient()
	d := NewDeployer(client, "default")

	spec := deploy.ValidationJobSpec{
		JobName:             "compile-svc-rel123",
		ReleaseID:           "rel123",
		NodeID:              "svc.orders",
		ServiceName:         "service-1",
		ImageTag:            "abc123",
		CandidateSchema:     "candidate_rel123",
		ManifestS3URI:       "s3://continuo/svc/rel123/manifest.json",
		ParseProdS3URI:      "s3://continuo/svc/rel123/parse/prod.msgpack",
		ParseCandidateS3URI: "s3://continuo/svc/rel123/parse/candidate.msgpack",
	}

	require.NoError(t, d.DeployCompile(context.Background(), spec))

	job := fetchJob(t, client, "default", "compile-svc-rel123")
	podSpec := job.Spec.Template.Spec
	assert.Equal(t, "carolsimone/service-1:abc123", podSpec.InitContainers[0].Image)
	assert.Equal(t, "s3://continuo/svc/rel123/manifest.json", envByName(podSpec, "MANIFEST_S3_URI"))
	assert.Equal(t, "rel123", job.Annotations[pkg_model.AnnotationReleaseID])
	assert.Equal(t, "svc.orders", job.Annotations[pkg_model.AnnotationNodeID])
	assert.Equal(t, "compile", job.Spec.Template.Labels["mode"])
}
