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

func TestBuiltinDefault_IsComplete(t *testing.T) {
	d := builtinDefault()
	require.Empty(t, d.missingKeys(), "built-in default must define every key")
	require.NoError(t, validateOpSet("default", d), "built-in default must pass per-template validation")
}

func TestDefaults_NodeCommand_MatchesPkgNodeType(t *testing.T) {
	r := Defaults()
	for _, nt := range []pkg_model.NodeType{
		pkg_model.NodeTypeDbtModel, pkg_model.NodeTypeDbtSeed, pkg_model.NodeTypeDbtSnapshot,
	} {
		assert.Equal(t, nt.Command("orders"), r.NodeCommand("any-service", pkg_model.OperationRun, nt, "orders"),
			"built-in default for %s must delegate to pkg NodeType.Command", nt)
	}
}

func TestDefaults_TestAndBuildOperations(t *testing.T) {
	r := Defaults()
	assert.Equal(t, []string{"dbt", "test", "--select", "orders"},
		r.NodeCommand("svc_a", pkg_model.OperationTest, pkg_model.NodeTypeDbtModel, "orders"))
	assert.Equal(t, []string{"dbt", "build", "--select", "orders"},
		r.NodeCommand("svc_a", pkg_model.OperationBuild, pkg_model.NodeTypeDbtSeed, "orders"),
		"build is node-agnostic")
}

func TestDefaults_SeedBuildCommand(t *testing.T) {
	r := Defaults()
	assert.Equal(t, []string{"dbt", "seed", "--select", "fx"},
		r.SeedBuildCommand("any-service", "fx", "_candidate_rel1"),
		"built-in seed_build equals seed; schema routed via DBT_TARGET_SCHEMA env")
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
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	r, err := Load(path, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	require.NoError(t, err)
	return r
}

// precedenceYAML is a complete config: a complete default plus a complete
// "wise" override, so it satisfies the completeness contract.
const precedenceYAML = `
default:
  run:        ["default-dbt", "run", "--select", "{{ node }}"]
  seed:       ["default-dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["default-dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["default-dbt", "test", "--select", "{{ node }}"]
  build:      ["default-dbt", "build", "--select", "{{ node }}"]
  seed_build: ["default-dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:       ["default-dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
services:
  wise:
    run:        ["wise-dbt", "run", "--select", "{{ node }}"]
    seed:       ["wise-dbt", "seed", "--select", "{{ node }}"]
    snapshot:   ["wise-dbt", "snapshot", "--select", "{{ node }}"]
    test:       ["wise-dbt", "test", "--select", "{{ node }}"]
    build:      ["wise-dbt", "build", "--select", "{{ node }}"]
    seed_build: ["wise-dbt", "seed", "--select", "{{ node }}", "--schema", "{{ target_schema }}"]
    compile:
      command:       ["wise-dbt", "compile", "--profiles-dir", "/project"]
      manifest_path: "/project/out/manifest.json"
`

func TestResolver_ServiceOverrideBeatsDefault(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "orders"},
		r.NodeCommand("wise", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders"))
	assert.Equal(t, []string{"default-dbt", "run", "--select", "orders"},
		r.NodeCommand("other-service", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders"),
		"service not in services: falls to default")
}

func TestResolver_ServiceUsesOwnSeedAndTest(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"wise-dbt", "seed", "--select", "fx"},
		r.NodeCommand("wise", pkg_model.OperationRun, pkg_model.NodeTypeDbtSeed, "fx"),
		"a complete override uses its own seed, never a fallthrough")
	assert.Equal(t, []string{"wise-dbt", "test", "--select", "fx"},
		r.NodeCommand("wise", pkg_model.OperationTest, pkg_model.NodeTypeDbtModel, "fx"))
}

func TestResolver_SeedBuildTemplate_SubstitutesTargetSchema(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t,
		[]string{"wise-dbt", "seed", "--select", "fx", "--schema", "_candidate_rel1"},
		r.SeedBuildCommand("wise", "fx", "_candidate_rel1"))
}

func TestResolver_SeedBuildFromDefault(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"default-dbt", "seed", "--select", "fx"},
		r.SeedBuildCommand("other-service", "fx", "_candidate_rel1"),
		"service not in services: seed_build resolves from the complete default")
}

func TestResolver_Compile(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	argv, mp := r.CompileCommand("wise")
	assert.Equal(t, []string{"wise-dbt", "compile", "--profiles-dir", "/project"}, argv)
	assert.Equal(t, "/project/out/manifest.json", mp)

	argv, mp = r.CompileCommand("other-service")
	assert.Equal(t, []string{"default-dbt", "compile", "--profiles-dir", "/project"}, argv,
		"service not in services: compile resolves from the complete default")
	assert.Equal(t, "/project/target/manifest.json", mp)
}

func TestResolver_SubstitutionInsideElementAndRepeated(t *testing.T) {
	r := loadTestConfig(t, `
default:
  run:        ["wise-dbt", "run", "--select", "model:{{node}}", "--log-prefix", "{{ node }}-{{ node }}"]
  seed:       ["wise-dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["wise-dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["wise-dbt", "test", "--select", "{{ node }}"]
  build:      ["wise-dbt", "build", "--select", "{{ node }}"]
  seed_build: ["wise-dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:       ["wise-dbt", "compile"]
    manifest_path: "/p/m.json"
`)
	assert.Equal(t,
		[]string{"wise-dbt", "run", "--select", "model:orders", "--log-prefix", "orders-orders"},
		r.NodeCommand("svc", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders"),
		"tokens inside elements, without inner spaces, and repeated all substitute")
}

func TestResolver_TemplateNotMutatedAcrossCalls(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	first := r.NodeCommand("wise", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders")
	second := r.NodeCommand("wise", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "users")
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "orders"}, first)
	assert.Equal(t, []string{"wise-dbt", "run", "--select", "users"}, second)
}
