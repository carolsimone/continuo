package commandcfg

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
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
	got := Defaults().CompileCommand("any-service")
	assert.Equal(t, ports.CompileCommand{
		Argv:             []string{"dbt", "compile", "--profiles-dir", "/project"},
		ManifestPath:     "/project/target/manifest.json",
		PartialParsePath: "/project/target/partial_parse.msgpack",
	}, got, "the built-in default omits partial_parse_path, so it derives beside manifest.json")
}

func TestDefaults_WrapperCachePolicy(t *testing.T) {
	assert.Equal(t, ports.WrapperCacheOpaque, Defaults().WrapperCachePolicy("any-service"),
		"plain dbt declares no wrapper, so its cache behaviour is unknown: opaque")
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
	assert.Equal(t, ports.CompileCommand{
		Argv:             []string{"wise-dbt", "compile", "--profiles-dir", "/project"},
		ManifestPath:     "/project/out/manifest.json",
		PartialParsePath: "/project/out/partial_parse.msgpack",
	}, r.CompileCommand("wise"))

	assert.Equal(t, ports.CompileCommand{
		Argv:             []string{"default-dbt", "compile", "--profiles-dir", "/project"},
		ManifestPath:     "/project/target/manifest.json",
		PartialParsePath: "/project/target/partial_parse.msgpack",
	}, r.CompileCommand("other-service"),
		"service not in services: compile resolves from the complete default")
}

// A wrapper that relocates its partial parse declares the path explicitly;
// without the declaration the path derives beside manifest.json.
func TestResolver_CompileCommand_ExplicitPartialParsePath(t *testing.T) {
	r := loadTestConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:            ["wise-dbt", "compile-project"]
    manifest_path:      "/project/target/manifest.json"
    partial_parse_path: "/var/cache/wise/pp.msgpack"
`)
	assert.Equal(t, "/var/cache/wise/pp.msgpack", r.CompileCommand("svc").PartialParsePath)
}

func TestResolver_CompileCommand_TemplateNotMutated(t *testing.T) {
	r := loadTestConfig(t, precedenceYAML)
	first := r.CompileCommand("wise")
	first.Argv[0] = "mutated"
	assert.Equal(t, "wise-dbt", r.CompileCommand("wise").Argv[0],
		"the returned argv must be a copy, never the stored template")
}

func TestResolver_WrapperCachePolicy(t *testing.T) {
	r := loadTestConfig(t, wrapperCacheYAML)
	assert.Equal(t, ports.WrapperCacheRequired, r.WrapperCachePolicy("wise"))
	assert.Equal(t, ports.WrapperCacheOpaque, r.WrapperCachePolicy("plain"),
		"an explicit opaque policy resolves to opaque")
	assert.Equal(t, ports.WrapperCacheOpaque, r.WrapperCachePolicy("no-worker-block"),
		"a service that declares no worker block defaults to opaque")
	assert.Equal(t, ports.WrapperCacheOpaque, r.WrapperCachePolicy("unknown-service"),
		"a service with no override at all defaults to opaque")
}

// A present-but-empty worker block resolves to opaque, not to the invalid
// empty policy value.
func TestResolver_WrapperCachePolicy_EmptyWorkerBlockIsOpaque(t *testing.T) {
	r := loadTestConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:       ["dbt", "compile"]
    manifest_path: "/project/target/manifest.json"
services:
  svc:
    run:        ["o-dbt", "run", "{{ node }}"]
    seed:       ["o-dbt", "seed", "{{ node }}"]
    snapshot:   ["o-dbt", "snapshot", "{{ node }}"]
    test:       ["o-dbt", "test", "{{ node }}"]
    build:      ["o-dbt", "build", "{{ node }}"]
    seed_build: ["o-dbt", "seed", "{{ node }}"]
    compile:
      command:       ["o-dbt", "compile"]
      manifest_path: "/project/target/manifest.json"
    worker: {}
`)
	assert.Equal(t, ports.WrapperCacheOpaque, r.WrapperCachePolicy("svc"))
}

// wrapperCacheYAML declares each wrapper-cache shape a service can take.
const wrapperCacheYAML = `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:       ["dbt", "compile"]
    manifest_path: "/project/target/manifest.json"
services:
  wise:
    run:        ["wise-dbt", "run", "{{ node }}"]
    seed:       ["wise-dbt", "seed", "{{ node }}"]
    snapshot:   ["wise-dbt", "snapshot", "{{ node }}"]
    test:       ["wise-dbt", "test", "{{ node }}"]
    build:      ["wise-dbt", "build", "{{ node }}"]
    seed_build: ["wise-dbt", "seed", "{{ node }}"]
    compile:
      command:       ["wise-dbt", "compile"]
      manifest_path: "/project/target/manifest.json"
    worker:
      wrapper_cache: required
  plain:
    run:        ["plain-dbt", "run", "{{ node }}"]
    seed:       ["plain-dbt", "seed", "{{ node }}"]
    snapshot:   ["plain-dbt", "snapshot", "{{ node }}"]
    test:       ["plain-dbt", "test", "{{ node }}"]
    build:      ["plain-dbt", "build", "{{ node }}"]
    seed_build: ["plain-dbt", "seed", "{{ node }}"]
    compile:
      command:       ["plain-dbt", "compile"]
      manifest_path: "/project/target/manifest.json"
    worker:
      wrapper_cache: opaque
  no-worker-block:
    run:        ["other-dbt", "run", "{{ node }}"]
    seed:       ["other-dbt", "seed", "{{ node }}"]
    snapshot:   ["other-dbt", "snapshot", "{{ node }}"]
    test:       ["other-dbt", "test", "{{ node }}"]
    build:      ["other-dbt", "build", "{{ node }}"]
    seed_build: ["other-dbt", "seed", "{{ node }}"]
    compile:
      command:       ["other-dbt", "compile"]
      manifest_path: "/project/target/manifest.json"
`

// RuntimeContext is hashed into a parse-context digest that decides whether a
// cached artifact may be reused, so identical config must always produce
// byte-identical JSON.
func TestResolver_RuntimeContext_Deterministic(t *testing.T) {
	first := loadTestConfig(t, wrapperCacheYAML).RuntimeContext("wise")
	for i := 0; i < 20; i++ {
		assert.Equal(t, first, loadTestConfig(t, wrapperCacheYAML).RuntimeContext("wise"),
			"identical config must serialize byte-identically on every call")
	}
	assert.NotEmpty(t, first)
}

// It must describe the service's whole command surface, so any change a team
// makes to what dbt actually runs invalidates a cached parse.
func TestResolver_RuntimeContext_CoversResolvedSurface(t *testing.T) {
	r := loadTestConfig(t, wrapperCacheYAML)
	ctx := r.RuntimeContext("wise")

	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(ctx), &got))
	assert.Equal(t, []string{
		"build", "compile", "manifest_path", "partial_parse_path", "run",
		"seed", "seed_build", "snapshot", "test", "wrapper_cache",
	}, sortedKeys(got), "the seven raw templates, both compile paths, and the wrapper policy")

	assert.Contains(t, ctx, `"wrapper_cache":"required"`)
	assert.Contains(t, ctx, `"partial_parse_path":"/project/target/partial_parse.msgpack"`)
	assert.Contains(t, ctx, `{{ node }}`, "templates are stored raw, not substituted")
}

func TestResolver_RuntimeContext_DiffersPerService(t *testing.T) {
	r := loadTestConfig(t, wrapperCacheYAML)
	assert.NotEqual(t, r.RuntimeContext("wise"), r.RuntimeContext("plain"),
		"different argv and wrapper policy must produce different context")
	assert.NotEqual(t, r.RuntimeContext("plain"), r.RuntimeContext("no-worker-block"),
		"differing only in wrapper policy still changes the context")
	assert.Equal(t, r.RuntimeContext("unknown-a"), r.RuntimeContext("unknown-b"),
		"two services that both fall back to the default share one context")
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
