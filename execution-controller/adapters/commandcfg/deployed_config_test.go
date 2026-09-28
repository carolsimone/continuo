package commandcfg

import (
	"path/filepath"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// The shipped Helm config is the source of truth for the Hetzner ConfigMap.
// This pins that it always loads and that finance resolves to the customname-dbt
// dialect for every operation while other services fall back to the default; it
// also pins that both the default and finance blocks always full-refresh a
// seed load and define full_refresh for models.
func TestDeployedConfigResolvesFinanceDialect(t *testing.T) {
	path := filepath.Join("..", "..", "..", "deploy", "continuo", "files", "dbt-commands.yaml")

	r, err := Load(path, testLogger())
	if err != nil {
		t.Fatalf("shipped deploy/continuo/files/dbt-commands.yaml must load: %v", err)
	}

	gotRun := mustNodeCommand(t, r, "finance", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "fx_transactions_eur")
	assertArgv(t, "finance run", gotRun, []string{"customname-dbt", "run-model", "fx_transactions_eur"})

	gotSnap := mustNodeCommand(t, r, "finance", pkg_model.OperationRun, pkg_model.NodeTypeDbtSnapshot, "fx_snap")
	assertArgv(t, "finance snapshot", gotSnap, []string{"customname-dbt", "capture-snapshot", "fx_snap"})

	gotTest := mustNodeCommand(t, r, "finance", pkg_model.OperationTest, pkg_model.NodeTypeDbtModel, "fx_transactions_eur")
	assertArgv(t, "finance test", gotTest, []string{"customname-dbt", "test-model", "fx_transactions_eur"})

	gotBuild := mustNodeCommand(t, r, "finance", pkg_model.OperationBuild, pkg_model.NodeTypeDbtModel, "fx_transactions_eur")
	assertArgv(t, "finance build", gotBuild, []string{"customname-dbt", "build-model", "fx_transactions_eur"})

	gotSeedBuild := r.SeedBuildCommand("finance", "seed_fx_rates_eur", "cand_schema")
	assertArgv(t, "finance seed_build", gotSeedBuild, []string{"customname-dbt", "load-seed", "seed_fx_rates_eur"})

	gotCompile, manifest := r.CompileCommand("finance")
	assertArgv(t, "finance compile", gotCompile, []string{"customname-dbt", "compile-project"})
	if manifest != "/project/target/manifest.json" {
		t.Fatalf("finance compile manifest_path = %q, want /project/target/manifest.json", manifest)
	}

	gotSeed := mustNodeCommand(t, r, "finance", pkg_model.OperationRun, pkg_model.NodeTypeDbtSeed, "seed_fx_rates_eur")
	assertArgv(t, "finance seed", gotSeed, []string{"customname-dbt", "reload-seed", "seed_fx_rates_eur"})

	gotFR := mustNodeCommand(t, r, "finance", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtModel, "fx_transactions_eur")
	assertArgv(t, "finance full_refresh", gotFR, []string{"customname-dbt", "rebuild-model", "fx_transactions_eur"})

	// A service with no override falls back to the default block (plain dbt).
	gotOther := mustNodeCommand(t, r, "service-3", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "some_model")
	assertArgv(t, "service-3 run fallback", gotOther, []string{"dbt", "run", "--select", "some_model"})

	gotOtherSeed := mustNodeCommand(t, r, "service-3", pkg_model.OperationRun, pkg_model.NodeTypeDbtSeed, "s")
	assertArgv(t, "service-3 seed", gotOtherSeed, []string{"dbt", "seed", "--full-refresh", "--select", "s"})

	gotOtherFR := mustNodeCommand(t, r, "service-3", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtModel, "m")
	assertArgv(t, "service-3 full_refresh", gotOtherFR, []string{"dbt", "run", "--full-refresh", "--select", "m"})
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
