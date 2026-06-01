package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/require"
)

func TestValidationDbtCommand_EmptyOnly(t *testing.T) {
	got := model.ValidationDbtCommand(pkg_model.NodeTypeDbtModel, "orders")
	want := []string{"dbt", "run", "--select", "orders", "--empty"}
	require.Equal(t, want, got)
}

func TestValidationDbtCommand_Seed(t *testing.T) {
	got := model.ValidationDbtCommand(pkg_model.NodeTypeDbtSeed, "customers")
	for _, a := range got {
		if a == "--defer" || a == "--state" {
			t.Fatalf("defer args must not appear: %v", got)
		}
	}
}

// The candidate schema is delivered via the DBT_TARGET_SCHEMA env var (read by
// each service's generate_schema_name macro), never as a CLI flag — dbt has no
// --target-schema option, so emitting one would make every validation job fail.
func TestValidationDbtCommand_NoTargetSchemaFlag(t *testing.T) {
	got := model.ValidationDbtCommand(pkg_model.NodeTypeDbtModel, "orders")
	require.NotContains(t, got, "--target-schema")
}
