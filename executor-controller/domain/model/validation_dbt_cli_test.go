package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidationCommand_Model verifies that model nodes run the runtime image's
// validation entrypoint instead of a dbt command — the runner fetches SQL from S3
// via CANDIDATE_SQL_URI and executes a CTAS from it.
func TestValidationCommand_Model(t *testing.T) {
	got := model.ValidationCommand(pkg_model.NodeTypeDbtModel, "orders")
	want := []string{"continuo-runtime", "validation-op"}
	require.Equal(t, want, got)
}

// TestValidationCommand_Snapshot verifies that snapshot nodes run the same
// validation entrypoint, consistent with the model path.
func TestValidationCommand_Snapshot(t *testing.T) {
	got := model.ValidationCommand(pkg_model.NodeTypeDbtSnapshot, "orders_snapshot")
	want := []string{"continuo-runtime", "validation-op"}
	require.Equal(t, want, got)
}

// TestValidationCommand_Seed verifies that seed nodes run the same validation
// entrypoint as models and snapshots. The operation (build_from_sql vs
// clone_from_prod) is selected by the VALIDATION_OP env var, not by a distinct
// seed command.
func TestValidationCommand_Seed(t *testing.T) {
	got := model.ValidationCommand(pkg_model.NodeTypeDbtSeed, "customers")
	want := []string{"continuo-runtime", "validation-op"}
	require.Equal(t, want, got)
}

// TestValidationCommand_PythonModel verifies that python nodes run the same
// validation entrypoint; VALIDATION_OP selects build_from_columns for them.
func TestValidationCommand_PythonModel(t *testing.T) {
	got := model.ValidationCommand(pkg_model.NodeTypePythonModel, "features")
	want := []string{"continuo-runtime", "validation-op"}
	require.Equal(t, want, got)
}

// TestValidationCommand_IsExplicitNotTheImageDefault asserts the command is never
// empty. The runtime image's default command runs the python-node harness, so an
// empty command would run the wrong program in a validation pod.
func TestValidationCommand_IsExplicitNotTheImageDefault(t *testing.T) {
	for _, nt := range []pkg_model.NodeType{
		pkg_model.NodeTypeDbtModel,
		pkg_model.NodeTypeDbtSeed,
		pkg_model.NodeTypeDbtSnapshot,
		pkg_model.NodeTypePythonModel,
	} {
		got := model.ValidationCommand(nt, "orders")
		require.NotEmpty(t, got, "node type %s must carry an explicit command", nt)
		assert.Equal(t, "validation-op", got[len(got)-1],
			"node type %s must select the validation subcommand", nt)
	}
}

// TestValidationCommand_NoTargetSchemaFlag asserts that no path emits
// --target-schema. dbt has no such flag; the candidate schema is delivered via
// DBT_TARGET_SCHEMA env and the generate_schema_name macro.
func TestValidationCommand_NoTargetSchemaFlag(t *testing.T) {
	for _, nt := range []pkg_model.NodeType{
		pkg_model.NodeTypeDbtModel,
		pkg_model.NodeTypeDbtSeed,
		pkg_model.NodeTypeDbtSnapshot,
	} {
		got := model.ValidationCommand(nt, "orders")
		assert.NotContains(t, got, "--target-schema",
			"node type %s must not emit --target-schema", nt)
	}
}
