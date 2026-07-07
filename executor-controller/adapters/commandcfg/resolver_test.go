package commandcfg

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults_NodeCommand_MatchesPkgNodeType(t *testing.T) {
	r := Defaults()
	for _, nt := range []pkg_model.NodeType{
		pkg_model.NodeTypeDbtModel, pkg_model.NodeTypeDbtSeed, pkg_model.NodeTypeDbtSnapshot,
	} {
		assert.Equal(t, nt.Command("orders"), r.NodeCommand("any-service", nt, "orders"),
			"built-in default for %s must delegate to pkg NodeType.Command", nt)
	}
}

func TestDefaults_SeedBuildCommand_FallsBackToSeed(t *testing.T) {
	r := Defaults()
	assert.Equal(t, []string{"dbt", "seed", "--select", "fx"},
		r.SeedBuildCommand("any-service", "fx", "_candidate_rel1"),
		"without a seed_build template, seed-build uses the seed command (schema routed via DBT_TARGET_SCHEMA env)")
}

func TestDefaults_CompileCommand(t *testing.T) {
	argv, manifestPath := Defaults().CompileCommand("any-service")
	assert.Equal(t, []string{"dbt", "compile", "--profiles-dir", "/project"}, argv)
	assert.Equal(t, "/project/target/manifest.json", manifestPath)
}

// loadTestConfig loads a Resolver from inline YAML via a temp file.
func loadTestConfig(t *testing.T, content string) *Resolver {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dbt-commands.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	r, err := Load(path, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	require.NoError(t, err)
	return r
}

const precedenceYAML = `
default:
  run: ["default-dbt", "run", "--select", "{{ node }}"]
services:
  wise:
    run: ["wise-dbt", "run", "--select", "{{ node }}"]
    seed_build: ["wise-dbt", "seed", "--select", "{{ node }}", "--schema", "{{ target_schema }}"]
    compile:
      command: ["wise-dbt", "compile", "--profiles-dir", "/project"]
      manifest_path: "/project/out/manifest.json"
`

func TestResolver_ServiceOverrideBeatsDefault(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "orders"},
		r.NodeCommand("wise", pkg_model.NodeTypeDbtModel, "orders"))
	assert.Equal(t, []string{"default-dbt", "run", "--select", "orders"},
		r.NodeCommand("other-service", pkg_model.NodeTypeDbtModel, "orders"),
		"service not in services: falls to default")
}

func TestResolver_PerOperationFallthrough(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"dbt", "seed", "--select", "fx"},
		r.NodeCommand("wise", pkg_model.NodeTypeDbtSeed, "fx"),
		"wise overrides run only; seed falls through default (which has no seed) to built-in")
}

func TestResolver_SeedBuildTemplate_SubstitutesTargetSchema(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t,
		[]string{"wise-dbt", "seed", "--select", "fx", "--schema", "_candidate_rel1"},
		r.SeedBuildCommand("wise", "fx", "_candidate_rel1"))
}

func TestResolver_SeedBuildWithoutTemplate_FallsBackToSeed(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"dbt", "seed", "--select", "fx"},
		r.SeedBuildCommand("other-service", "fx", "_candidate_rel1"))
}

func TestResolver_CompileOverride(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	argv, mp := r.CompileCommand("wise")
	assert.Equal(t, []string{"wise-dbt", "compile", "--profiles-dir", "/project"}, argv)
	assert.Equal(t, "/project/out/manifest.json", mp)

	argv, mp = r.CompileCommand("other-service")
	assert.Equal(t, []string{"dbt", "compile", "--profiles-dir", "/project"}, argv,
		"no compile in default: built-in")
	assert.Equal(t, "/project/target/manifest.json", mp)
}

func TestResolver_SubstitutionInsideElementAndRepeated(t *testing.T) {
	r := loadTestConfig(t, `
default:
  run: ["wise-dbt", "run", "--select", "model:{{node}}", "--log-prefix", "{{ node }}-{{ node }}"]
`)
	assert.Equal(t,
		[]string{"wise-dbt", "run", "--select", "model:orders", "--log-prefix", "orders-orders"},
		r.NodeCommand("svc", pkg_model.NodeTypeDbtModel, "orders"),
		"tokens inside elements, without inner spaces, and repeated all substitute")
}

func TestResolver_TemplateNotMutatedAcrossCalls(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	first := r.NodeCommand("wise", pkg_model.NodeTypeDbtModel, "orders")
	second := r.NodeCommand("wise", pkg_model.NodeTypeDbtModel, "users")
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "orders"}, first)
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "users"}, second)
}
