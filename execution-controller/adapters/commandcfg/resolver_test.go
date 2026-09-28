package commandcfg

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustNodeCommand resolves a node command that the test expects to succeed.
func mustNodeCommand(t *testing.T, r *Resolver, svc string, op pkg_model.Operation, nt pkg_model.NodeType, node string) []string {
	t.Helper()
	argv, err := r.NodeCommand(svc, op, nt, node)
	if err != nil {
		t.Fatalf("NodeCommand(%s, %q, %s, %s): %v", svc, op, nt, node, err)
	}
	return argv
}

// loadYAML loads a Resolver from inline YAML via a temp file.
func loadYAML(t *testing.T, body string) *Resolver {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dbt-commands.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	r, err := Load(p, testLogger())
	require.NoError(t, err)
	return r
}

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
		assert.Equal(t, nt.Command("orders"), mustNodeCommand(t, r, "any-service", pkg_model.OperationRun, nt, "orders"),
			"built-in default for %s must delegate to pkg NodeType.Command", nt)
	}
}

func TestDefaults_TestAndBuildOperations(t *testing.T) {
	r := Defaults()
	assert.Equal(t, []string{"dbt", "test", "--select", "orders"},
		mustNodeCommand(t, r, "svc_a", pkg_model.OperationTest, pkg_model.NodeTypeDbtModel, "orders"))
	assert.Equal(t, []string{"dbt", "build", "--select", "orders"},
		mustNodeCommand(t, r, "svc_a", pkg_model.OperationBuild, pkg_model.NodeTypeDbtSeed, "orders"),
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

func TestParseCommand_DefaultAndOverride(t *testing.T) {
	r := Defaults()
	// No --profiles-dir: the built-in parse argv must carry the same
	// parse-affecting flags as run/seed/snapshot/test/build/seed_build (none),
	// per validateParseContext.
	assert.Equal(t, []string{"dbt", "parse"}, r.ParseCommand("any-service"))
}

func TestPartialParsePath_DefaultsToManifestSibling(t *testing.T) {
	r := Defaults()
	// builtin manifest_path is /project/target/manifest.json
	assert.Equal(t, "/project/target/partial_parse.msgpack", r.PartialParsePath("any-service"))
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
// "customname" override, so it satisfies the completeness contract.
const precedenceYAML = `
default:
  run:        ["default-dbt", "run", "--select", "{{ node }}"]
  seed:       ["default-dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["default-dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["default-dbt", "test", "--select", "{{ node }}"]
  build:      ["default-dbt", "build", "--select", "{{ node }}"]
  seed_build: ["default-dbt", "seed", "--select", "{{ node }}"]
  parse:      ["default-dbt", "parse"]
  compile:
    command:       ["default-dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
services:
  customname:
    run:        ["customname-dbt", "run", "--select", "{{ node }}"]
    seed:       ["customname-dbt", "seed", "--select", "{{ node }}"]
    snapshot:   ["customname-dbt", "snapshot", "--select", "{{ node }}"]
    test:       ["customname-dbt", "test", "--select", "{{ node }}"]
    build:      ["customname-dbt", "build", "--select", "{{ node }}"]
    seed_build: ["customname-dbt", "seed", "--select", "{{ node }}", "--schema", "{{ target_schema }}"]
    parse:      ["customname-dbt", "parse"]
    compile:
      command:       ["customname-dbt", "compile", "--profiles-dir", "/project"]
      manifest_path: "/project/out/manifest.json"
`

func TestResolver_ServiceOverrideBeatsDefault(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"customname-dbt", "run", "--select", "orders"},
		mustNodeCommand(t, r, "customname", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders"))
	assert.Equal(t, []string{"default-dbt", "run", "--select", "orders"},
		mustNodeCommand(t, r, "other-service", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders"),
		"service not in services: falls to default")
}

func TestResolver_ServiceUsesOwnSeedAndTest(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"customname-dbt", "seed", "--select", "fx"},
		mustNodeCommand(t, r, "customname", pkg_model.OperationRun, pkg_model.NodeTypeDbtSeed, "fx"),
		"a complete override uses its own seed, never a fallthrough")
	assert.Equal(t, []string{"customname-dbt", "test", "--select", "fx"},
		mustNodeCommand(t, r, "customname", pkg_model.OperationTest, pkg_model.NodeTypeDbtModel, "fx"))
}

func TestResolver_SeedBuildTemplate_SubstitutesTargetSchema(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t,
		[]string{"customname-dbt", "seed", "--select", "fx", "--schema", "_candidate_rel1"},
		r.SeedBuildCommand("customname", "fx", "_candidate_rel1"))
}

func TestResolver_SeedBuildFromDefault(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	assert.Equal(t, []string{"default-dbt", "seed", "--select", "fx"},
		r.SeedBuildCommand("other-service", "fx", "_candidate_rel1"),
		"service not in services: seed_build resolves from the complete default")
}

func TestResolver_Compile(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	argv, mp := r.CompileCommand("customname")
	assert.Equal(t, []string{"customname-dbt", "compile", "--profiles-dir", "/project"}, argv)
	assert.Equal(t, "/project/out/manifest.json", mp)

	argv, mp = r.CompileCommand("other-service")
	assert.Equal(t, []string{"default-dbt", "compile", "--profiles-dir", "/project"}, argv,
		"service not in services: compile resolves from the complete default")
	assert.Equal(t, "/project/target/manifest.json", mp)
}

func TestResolver_SubstitutionInsideElementAndRepeated(t *testing.T) {
	r := loadTestConfig(t, `
default:
  run:        ["customname-dbt", "run", "--select", "model:{{node}}", "--log-prefix", "{{ node }}-{{ node }}"]
  seed:       ["customname-dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["customname-dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["customname-dbt", "test", "--select", "{{ node }}"]
  build:      ["customname-dbt", "build", "--select", "{{ node }}"]
  seed_build: ["customname-dbt", "seed", "--select", "{{ node }}"]
  parse:      ["customname-dbt", "parse"]
  compile:
    command:       ["customname-dbt", "compile"]
    manifest_path: "/p/m.json"
`)
	assert.Equal(t,
		[]string{"customname-dbt", "run", "--select", "model:orders", "--log-prefix", "orders-orders"},
		mustNodeCommand(t, r, "svc", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders"),
		"tokens inside elements, without inner spaces, and repeated all substitute")
}

func TestResolver_TemplateNotMutatedAcrossCalls(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	first := mustNodeCommand(t, r, "customname", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders")
	second := mustNodeCommand(t, r, "customname", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "users")
	assert.Equal(t, []string{"customname-dbt", "run", "--select", "orders"}, first)
	assert.Equal(t, []string{"customname-dbt", "run", "--select", "users"}, second)
}

const fullRefreshYAML = `
default:
  run:          ["dbt", "run", "--select", "{{ node }}"]
  seed:         ["dbt", "seed", "--full-refresh", "--select", "{{ node }}"]
  snapshot:     ["dbt", "snapshot", "--select", "{{ node }}"]
  test:         ["dbt", "test", "--select", "{{ node }}"]
  build:        ["dbt", "build", "--select", "{{ node }}"]
  seed_build:   ["dbt", "seed", "--select", "{{ node }}"]
  full_refresh: ["dbt", "run", "--full-refresh", "--select", "{{ node }}"]
  parse:        ["dbt", "parse"]
  compile:
    command:       ["dbt", "compile"]
    manifest_path: "/project/target/manifest.json"
services:
  customname:
    run:          ["customname-dbt", "run-model", "{{ node }}"]
    seed:         ["customname-dbt", "reload-seed", "{{ node }}"]
    snapshot:     ["customname-dbt", "capture-snapshot", "{{ node }}"]
    test:         ["customname-dbt", "test-model", "{{ node }}"]
    build:        ["customname-dbt", "build-model", "{{ node }}"]
    seed_build:   ["customname-dbt", "load-seed", "{{ node }}"]
    full_refresh: ["customname-dbt", "rebuild-model", "{{ node }}"]
    parse:        ["customname-dbt", "parse-project"]
    compile:
      command:       ["customname-dbt", "compile-project"]
      manifest_path: "/project/target/manifest.json"
  legacy:
    run:        ["legacy-dbt", "run", "{{ node }}"]
    seed:       ["legacy-dbt", "seed", "{{ node }}"]
    snapshot:   ["legacy-dbt", "snapshot", "{{ node }}"]
    test:       ["legacy-dbt", "test", "{{ node }}"]
    build:      ["legacy-dbt", "build", "{{ node }}"]
    seed_build: ["legacy-dbt", "seed", "{{ node }}"]
    parse:      ["legacy-dbt", "parse"]
    compile:
      command:       ["legacy-dbt", "compile"]
      manifest_path: "/project/target/manifest.json"
`

func TestNodeCommand_FullRefresh(t *testing.T) {
	r := loadYAML(t, fullRefreshYAML)

	assert.Equal(t, []string{"dbt", "run", "--full-refresh", "--select", "orders"},
		mustNodeCommand(t, r, "svc", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtModel, "orders"),
		"a service without an override uses the default full_refresh")
	assert.Equal(t, []string{"customname-dbt", "rebuild-model", "orders"},
		mustNodeCommand(t, r, "customname", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtModel, "orders"))
	assert.Equal(t, []string{"customname-dbt", "reload-seed", "fx"},
		mustNodeCommand(t, r, "customname", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtSeed, "fx"),
		"a seed full refresh resolves to the seed template")
	assert.Equal(t, []string{"legacy-dbt", "seed", "fx"},
		mustNodeCommand(t, r, "legacy", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtSeed, "fx"),
		"a seed full refresh needs no full_refresh key")
}

func TestNodeCommand_FullRefresh_ServiceBlockWithoutKeyIsPermanent(t *testing.T) {
	r := loadYAML(t, fullRefreshYAML)
	_, err := r.NodeCommand("legacy", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtModel, "orders")
	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.Contains(t, err.Error(), "services.legacy defines no full_refresh command",
		"must not fall through to the default block's plain dbt")
}

func TestNodeCommand_FullRefresh_UnsupportedNodeTypeIsPermanent(t *testing.T) {
	r := Defaults()
	for _, nt := range []pkg_model.NodeType{pkg_model.NodeTypeDbtSnapshot, pkg_model.NodeTypeDbtTest, pkg_model.NodeTypePythonModel} {
		_, err := r.NodeCommand("svc", pkg_model.OperationFullRefresh, nt, "x")
		assert.ErrorIs(t, err, pkgevents.ErrPermanent, "node type %s", nt)
	}
}

func TestBuiltinDefault_FullRefreshAndSeed(t *testing.T) {
	r := Defaults()
	assert.Equal(t, []string{"dbt", "run", "--full-refresh", "--select", "orders"},
		mustNodeCommand(t, r, "svc", pkg_model.OperationFullRefresh, pkg_model.NodeTypeDbtModel, "orders"))
	assert.Equal(t, []string{"dbt", "seed", "--full-refresh", "--select", "fx"},
		mustNodeCommand(t, r, "svc", pkg_model.OperationRun, pkg_model.NodeTypeDbtSeed, "fx"))
}
