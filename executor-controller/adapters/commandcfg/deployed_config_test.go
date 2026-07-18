package commandcfg

import (
	"path/filepath"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// deployedConfigs are the shipped Helm dbt dialects. deploy/continuo is the
// self-contained install and the single source of truth for the dbt-commands
// ConfigMap; it describes the finance team and the wise-dbt wrapper, and the
// deployed config must resolve exactly as the in-repo dialect does — a block
// present in the repo but absent from the chart silently changes how finance runs.
var deployedConfigs = map[string]string{
	"deploy/continuo": filepath.Join("..", "..", "..", "deploy", "continuo", "files", "dbt-commands.yaml"),
}

// This pins that every shipped config loads and that finance resolves to the
// wise-dbt dialect for every operation while other services fall back to the
// default.
func TestDeployedConfigResolvesFinanceDialect(t *testing.T) {
	for name, path := range deployedConfigs {
		t.Run(name, func(t *testing.T) {
			assertFinanceDialect(t, name, path)
		})
	}
}

func assertFinanceDialect(t *testing.T, name, path string) {
	t.Helper()

	r, err := Load(path, testLogger())
	if err != nil {
		t.Fatalf("shipped %s/files/dbt-commands.yaml must load: %v", name, err)
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

	gotCompile := r.CompileCommand("finance")
	assertArgv(t, "finance compile", gotCompile.Argv, []string{"wise-dbt", "compile-project"})
	if gotCompile.ManifestPath != "/project/target/manifest.json" {
		t.Fatalf("finance compile manifest_path = %q, want /project/target/manifest.json", gotCompile.ManifestPath)
	}

	// The wise-dbt wrapper declares a reusable parse cache; a service with no
	// override makes no such claim.
	if got := r.WrapperCachePolicy("finance"); got != ports.WrapperCacheRequired {
		t.Fatalf("finance wrapper_cache = %q, want %q", got, ports.WrapperCacheRequired)
	}
	if got := r.WrapperCachePolicy("service-3"); got != ports.WrapperCacheOpaque {
		t.Fatalf("service-3 wrapper_cache = %q, want %q", got, ports.WrapperCacheOpaque)
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
