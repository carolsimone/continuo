package k8s

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompileParamsFromSpec_MapsAllFields pins DeployCompile's spec→params
// field threading: every field a fully-populated deploy.ValidationJobSpec
// carries into a compile Job (including the parse-free fields CandidateSchema,
// ParseProdS3URI, ParseCandidateS3URI, plus the pre-existing ManifestS3URI)
// must land on the resulting ValidationJobParams unchanged. A refactor that
// silently drops a field flips one of these assertions.
func TestCompileParamsFromSpec_MapsAllFields(t *testing.T) {
	spec := deploy.ValidationJobSpec{
		JobName:             "compile-svc-rel123",
		ReleaseID:           "rel123",
		NodeID:              "svc.orders",
		ServiceName:         "service-1",
		SchemaName:          "analytics",
		TableName:           "orders",
		NodeType:            string(pkg_model.NodeTypeDbtModel),
		ImageTag:            "abc123",
		CandidateSchema:     "candidate_rel123",
		CandidateSQLURI:     "s3://continuo/svc/orders.sql",
		ValidationOp:        "build_from_sql",
		ProdSchema:          "prod_schema",
		ManifestS3URI:       "s3://continuo/svc/rel123/manifest.json",
		ParseProdS3URI:      "s3://continuo/svc/rel123/parse/prod.msgpack",
		ParseCandidateS3URI: "s3://continuo/svc/rel123/parse/candidate.msgpack",
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
	// ValidationOp/ProdSchema/CandidateSQLURI belong to DeployValidation and
	// DeploySeedBuild, not the compile Job).
	assert.Empty(t, params.SchemaName)
	assert.Empty(t, params.TableName)
	assert.Empty(t, params.ValidationOp)
	assert.Empty(t, params.ProdSchema)
	assert.Empty(t, params.CandidateSQLURI)
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
// params-mapping level above; their pod-spec wiring is Task 5's concern.
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
