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
		"_candidate_rel_123",
		"s3://continuo/releases/prev/manifests/")
	want := []string{
		"dbt", "run", "--select", "orders",
		"--empty", "--target-schema", "_candidate_rel_123",
		"--defer", "--state", "s3://continuo/releases/prev/manifests/",
	}
	require.Equal(t, want, got)
}

func TestValidationDbtCommand_Seed_WithDefer(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtSeed, "country_codes",
		"_candidate_rel_123",
		"s3://continuo/releases/prev/manifests/")
	want := []string{
		"dbt", "seed", "--select", "country_codes",
		"--empty", "--target-schema", "_candidate_rel_123",
		"--defer", "--state", "s3://continuo/releases/prev/manifests/",
	}
	require.Equal(t, want, got)
}

func TestValidationDbtCommand_Snapshot_WithDefer(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtSnapshot, "orders_snapshot",
		"_candidate_rel_123",
		"s3://continuo/releases/prev/manifests/")
	want := []string{
		"dbt", "snapshot", "--select", "orders_snapshot",
		"--empty", "--target-schema", "_candidate_rel_123",
		"--defer", "--state", "s3://continuo/releases/prev/manifests/",
	}
	require.Equal(t, want, got)
}

func TestValidationDbtCommand_EmptyDeferURI_OmitsDeferAndState(t *testing.T) {
	got := model.ValidationDbtCommand(
		pkg_model.NodeTypeDbtModel, "orders",
		"_candidate_rel_123",
		"")
	want := []string{
		"dbt", "run", "--select", "orders",
		"--empty", "--target-schema", "_candidate_rel_123",
	}
	require.Equal(t, want, got)
}
