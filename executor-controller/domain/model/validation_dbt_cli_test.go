package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/require"
)

func TestValidationDbtCommand_Model_WithDefer(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtModel, "orders",
		"s3://continuo/releases/prev/manifests/")
	want := []string{
		"dbt", "run", "--select", "orders",
		"--empty",
		"--defer", "--state", "s3://continuo/releases/prev/manifests/",
	}
	require.Equal(t, want, got)
}

func TestValidationDbtCommand_Seed_WithDefer(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtSeed, "country_codes",
		"s3://continuo/releases/prev/manifests/")
	want := []string{
		"dbt", "seed", "--select", "country_codes",
		"--empty",
		"--defer", "--state", "s3://continuo/releases/prev/manifests/",
	}
	require.Equal(t, want, got)
}

func TestValidationDbtCommand_Snapshot_WithDefer(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtSnapshot, "orders_snapshot",
		"s3://continuo/releases/prev/manifests/")
	want := []string{
		"dbt", "snapshot", "--select", "orders_snapshot",
		"--empty",
		"--defer", "--state", "s3://continuo/releases/prev/manifests/",
	}
	require.Equal(t, want, got)
}

// The candidate schema is delivered via the DBT_TARGET_SCHEMA env var (read by
// each service's generate_schema_name macro), never as a CLI flag — dbt has no
// --target-schema option, so emitting one would make every validation job fail.
func TestValidationDbtCommand_NoTargetSchemaFlag(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtModel, "orders", "")
	require.NotContains(t, got, "--target-schema")
}

func TestValidationDbtCommand_EmptyDeferURI_OmitsDeferAndState(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtModel, "orders", "")
	want := []string{
		"dbt", "run", "--select", "orders",
		"--empty",
	}
	require.Equal(t, want, got)
}
