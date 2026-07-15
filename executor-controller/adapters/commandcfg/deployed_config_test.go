package commandcfg

import (
	"path/filepath"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// The shipped Helm config is the source of truth for the Hetzner ConfigMap.
// This pins that it always loads and that finance resolves to the wise-dbt
// dialect for every operation while other services fall back to the default.
func TestDeployedConfigResolvesFinanceDialect(t *testing.T) {
	path := filepath.Join("..", "..", "..", "deploy", "app", "files", "dbt-commands.yaml")

	r, err := Load(path, testLogger())
	if err != nil {
		t.Fatalf("shipped deploy/app/files/dbt-commands.yaml must load: %v", err)
	}

	gotRun := r.NodeCommand("finance", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "fx_transactions_eur")
	assertArgv(t, "finance run", gotRun, []string{"wise-dbt", "run-model", "fx_transactions_eur"})

	gotSnap := r.NodeCommand("finance", pkg_model.OperationRun, pkg_model.NodeTypeDbtSnapshot, "fx_snap")
	assertArgv(t, "finance snapshot", gotSnap, []string{"wise-dbt", "capture-snapshot", "fx_snap"})

	gotTest := r.NodeCommand("finance", pkg_model.OperationTest, pkg_model.NodeTypeDbtModel, "fx_transactions_eur")
	assertArgv(t, "finance test", gotTest, []string{"wise-dbt", "test-model", "fx_transactions_eur"})

	gotBuild := r.NodeCommand("finance", pkg_model.OperationBuild, pkg_model.NodeTypeDbtModel, "fx_transactions_eur")
	assertArgv(t, "finance build", gotBuild, []string{"wise-dbt", "build-model", "fx_transactions_eur"})

	gotSeedBuild := r.SeedBuildCommand("finance", "seed_fx_rates_eur", "cand_schema")
	assertArgv(t, "finance seed_build", gotSeedBuild, []string{"wise-dbt", "load-seed", "seed_fx_rates_eur"})

	gotCompile, manifest := r.CompileCommand("finance")
	assertArgv(t, "finance compile", gotCompile, []string{"wise-dbt", "compile-project"})
	if manifest != "/project/target/manifest.json" {
		t.Fatalf("finance compile manifest_path = %q, want /project/target/manifest.json", manifest)
	}

	// A service with no override falls back to the default block (plain dbt).
	gotOther := r.NodeCommand("service-3", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "some_model")
	assertArgv(t, "service-3 run fallback", gotOther, []string{"dbt", "run", "--select", "some_model"})
}

func assertArgv(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}
