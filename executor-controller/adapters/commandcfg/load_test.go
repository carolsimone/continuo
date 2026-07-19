package commandcfg

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}

// captureLogger returns a logger whose output lands in the returned buffer,
// so a test can assert on what Load warned about.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// writeConfig writes content as a dbt-commands.yaml in a temp dir and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dbt-commands.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// completeDefault is a complete default block reused by valid-config tests.
const completeDefault = `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
`

func TestLoad_EmptyPathUsesDefaults(t *testing.T) {
	r, err := Load("", testLogger())
	require.NoError(t, err)
	assert.Equal(t, []string{"dbt", "run", "--select", "t"},
		r.NodeCommand("svc", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "t"))
}

func TestLoad_MissingFileUsesDefaults(t *testing.T) {
	r, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"), testLogger())
	require.NoError(t, err)
	assert.Equal(t, []string{"dbt", "seed", "--select", "t"},
		r.NodeCommand("svc", pkg_model.OperationRun, pkg_model.NodeTypeDbtSeed, "t"))
}

func TestLoad_ValidCompleteFile(t *testing.T) {
	path := writeConfig(t, completeDefault+`
services:
  wise:
    run:        ["wise-dbt", "run", "--select", "{{ node }}"]
    seed:       ["wise-dbt", "seed", "--select", "{{ node }}"]
    snapshot:   ["wise-dbt", "snapshot", "--select", "{{ node }}"]
    test:       ["wise-dbt", "test", "--select", "{{ node }}"]
    build:      ["wise-dbt", "build", "--select", "{{ node }}"]
    seed_build: ["wise-dbt", "seed", "--select", "{{ node }}"]
    parse:      ["wise-dbt", "parse"]
    compile:
      command:       ["wise-dbt", "compile", "--profiles-dir", "/project"]
      manifest_path: "/project/target/manifest.json"
`)
	r, err := Load(path, testLogger())
	require.NoError(t, err)
	assert.Equal(t, []string{"wise-dbt", "test", "--select", "x"},
		r.NodeCommand("wise", pkg_model.OperationTest, pkg_model.NodeTypeDbtModel, "x"))
}

func TestLoad_FileWithoutDefaultIsError(t *testing.T) {
	path := writeConfig(t, `
services:
  wise:
    run:        ["wise-dbt", "run", "--select", "{{ node }}"]
    seed:       ["wise-dbt", "seed", "--select", "{{ node }}"]
    snapshot:   ["wise-dbt", "snapshot", "--select", "{{ node }}"]
    test:       ["wise-dbt", "test", "--select", "{{ node }}"]
    build:      ["wise-dbt", "build", "--select", "{{ node }}"]
    seed_build: ["wise-dbt", "seed", "--select", "{{ node }}"]
    compile:
      command:       ["wise-dbt", "compile"]
      manifest_path: "/p/m.json"
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default: required")
}

func TestLoad_IncompleteDefaultIsError(t *testing.T) {
	path := writeConfig(t, `
default:
  run: ["dbt", "run", "--select", "{{ node }}"]
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default: incomplete command set, missing")
}

// A partial service override — the original finance bug — is now a boot error.
func TestLoad_PartialServiceOverrideIsError(t *testing.T) {
	path := writeConfig(t, completeDefault+`
services:
  wise:
    run:  ["wise-dbt", "run", "--select", "{{ node }}"]
    seed: ["wise-dbt", "seed", "--select", "{{ node }}"]
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "services.wise: incomplete command set, missing")
	assert.Contains(t, err.Error(), "test")
	assert.Contains(t, err.Error(), "build")
	assert.Contains(t, err.Error(), "compile")
	assert.Contains(t, err.Error(), "snapshot")
	assert.Contains(t, err.Error(), "seed_build")
	assert.Contains(t, err.Error(), "parse")
}

func TestLoad_MissingParseKeyIsFatal(t *testing.T) {
	// The default block defines the seven old keys but no parse; parse is now
	// required, so this must fail completeness with the missing key named.
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing [parse]")
}

func TestLoad_PartialParsePathMustBeAbsolute(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  compile:
    command:             ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path:       "/p/target/manifest.json"
    partial_parse_path:  "relative/path"
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be an absolute path")
}

// PartialParsePath must live in the same directory as ManifestPath: the
// executor mounts the parse-cache emptyDir at dirname(PartialParsePath) in
// every run/seed/build pod for the team's image, and a directory that
// differs from the --target-path dbt actually writes both artifacts into
// would shadow the team's real project files (dbt_project.yml, models/, …)
// instead of just the disposable target dir.
func TestLoad_PartialParsePathSameDirAsManifestPathPasses(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  compile:
    command:             ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path:       "/project/target/manifest.json"
    partial_parse_path:  "/project/target/partial_parse.msgpack"
`)
	_, err := Load(path, testLogger())
	require.NoError(t, err)
}

func TestLoad_PartialParsePathDifferentDirFromManifestPathFails(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  compile:
    command:             ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path:       "/project/target/manifest.json"
    partial_parse_path:  "/project/partial_parse.msgpack"
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must live in the same directory as manifest_path")
}

func TestLoad_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "malformed yaml",
			yaml:    "default: [not a map",
			wantErr: "parse dbt commands config",
		},
		{
			// A typo'd top-level key is now tolerated (warn + ignore, see
			// TestLoad_UnknownFieldWarnsAndLoads) rather than a distinct
			// "field not found" error — but "default" was never actually
			// set, so completeness validation still catches it, just with a
			// different message.
			name:    "unknown top-level key leaves default unset",
			yaml:    "defualt:\n  run: [\"dbt\"]",
			wantErr: "default: required and must define every command",
		},
		{
			// Same tolerance applies inside an opSet: the typo'd key is
			// dropped, so "run" was never actually set and completeness
			// validation reports it missing.
			name:    "unknown operation key leaves run unset",
			yaml:    "default:\n  runn: [\"dbt\", \"run\", \"--select\", \"{{ node }}\"]",
			wantErr: "default: incomplete command set, missing",
		},
		{
			name:    "empty argv reported before completeness",
			yaml:    "default:\n  run: []",
			wantErr: "default.run: must not be empty",
		},
		{
			name:    "run missing node placeholder reported before completeness",
			yaml:    "default:\n  run: [\"dbt\", \"run\"]",
			wantErr: "default.run: missing required {{ node }} placeholder",
		},
		{
			name:    "unknown placeholder",
			yaml:    "default:\n  run: [\"dbt\", \"run\", \"--select\", \"{{ nodename }}\"]",
			wantErr: "default.run: unknown placeholder {{ nodename }}",
		},
		{
			name:    "target_schema outside seed_build",
			yaml:    "default:\n  run: [\"dbt\", \"run\", \"--select\", \"{{ node }}\", \"--schema\", \"{{ target_schema }}\"]",
			wantErr: "default.run: unknown placeholder {{ target_schema }}",
		},
		{
			name:    "compile with placeholder",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\", \"{{ node }}\"]\n    manifest_path: \"/p/m.json\"",
			wantErr: "default.compile.command: unknown placeholder {{ node }}",
		},
		{
			name:    "compile relative manifest_path",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\"]\n    manifest_path: \"target/manifest.json\"",
			wantErr: "default.compile.manifest_path: must be an absolute path",
		},
		{
			name:    "compile empty command",
			yaml:    "default:\n  compile:\n    command: []\n    manifest_path: \"/p/m.json\"",
			wantErr: "default.compile.command: must not be empty",
		},
		{
			name:    "compile without manifest_path",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\"]",
			wantErr: "default.compile.manifest_path: required",
		},
		{
			name:    "compile manifest_path with placeholder",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\"]\n    manifest_path: \"/project/{{ node }}/manifest.json\"",
			wantErr: "default.compile.manifest_path: placeholders are not allowed",
		},
		{
			name:    "seed_build missing node placeholder",
			yaml:    "default:\n  seed_build: [\"dbt\", \"seed\", \"--schema\", \"{{ target_schema }}\"]",
			wantErr: "default.seed_build: missing required {{ node }} placeholder",
		},
		{
			name:    "incomplete default lists missing keys",
			yaml:    "default:\n  run: [\"dbt\", \"run\", \"--select\", \"{{ node }}\"]",
			wantErr: "default: incomplete command set, missing",
		},
		{
			name:    "empty service is incomplete",
			yaml:    completeDefault + "services:\n  wise: {}",
			wantErr: "services.wise: incomplete command set, missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.yaml), testLogger())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// dbt folds --vars/--target/--profile/--profiles-dir/--project-dir/
// --no-partial-parse into its partial-parse validity check. If a block's
// run/seed/snapshot/test/build/seed_build argv carries a parse-affecting
// flag the parse argv doesn't (or a different value for one), the compile
// rehearsal validates a context the runtime dispatch never reproduces —
// the rehearsal can pass while every dispatched node still full-parses.
func TestLoad_ParseContextFlagsMatchAcrossBlock(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}", "--vars", "env:prod"]
  seed:       ["dbt", "seed", "--select", "{{ node }}", "--vars", "env:prod"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}", "--vars", "env:prod"]
  test:       ["dbt", "test", "--select", "{{ node }}", "--vars", "env:prod"]
  build:      ["dbt", "build", "--select", "{{ node }}", "--vars", "env:prod"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}", "--vars", "env:prod"]
  parse:      ["dbt", "parse", "--vars", "env:prod"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
`)
	_, err := Load(path, testLogger())
	require.NoError(t, err)
}

func TestLoad_ParseContextFlagMissingFromParseFails(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}", "--target", "prod"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default.run")
	assert.Contains(t, err.Error(), "--target")
}

func TestLoad_ParseContextFlagValueMismatchFails(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}", "--vars", "env:prod"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse", "--vars", "env:staging"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default.run")
	assert.Contains(t, err.Error(), "--vars")
}

func TestLoad_NoPartialParseFlagOnlyOnOneOpFails(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}", "--no-partial-parse"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
`)
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default.build")
	assert.Contains(t, err.Error(), "--no-partial-parse")
}

// Regression: builtinDefault() and both shipped dbt-commands.yaml files must
// keep run/seed/snapshot/test/build/seed_build's parse-affecting flags in
// sync with parse's. builtinDefault() itself is never routed through
// validate() (Load("") and Defaults() construct a Resolver directly), so it
// is checked explicitly here, mirroring TestBuiltinDefault_IsComplete.
func TestParseContextValidation_BuiltinAndShippedConfigsPass(t *testing.T) {
	require.NoError(t, validateParseContext("default", builtinDefault()),
		"built-in default must define matching parse-context flags across the block")

	for _, shipped := range []string{
		filepath.Join("..", "..", "..", "config", "dbt-commands.yaml"),
		filepath.Join("..", "..", "..", "deploy", "continuo", "files", "dbt-commands.yaml"),
	} {
		_, err := Load(shipped, testLogger())
		require.NoErrorf(t, err, "shipped config %s must load", shipped)
	}
}

// A field this build doesn't recognize (e.g. a newer chart's dbt-commands.yaml
// carrying an operation key an older rolling-deploy binary predates) must not
// crash-loop the executor: Load warns and proceeds with the field dropped.
func TestLoad_UnknownFieldWarnsAndLoads(t *testing.T) {
	path := writeConfig(t, `
default:
  run:        ["dbt", "run", "--select", "{{ node }}"]
  seed:       ["dbt", "seed", "--select", "{{ node }}"]
  snapshot:   ["dbt", "snapshot", "--select", "{{ node }}"]
  test:       ["dbt", "test", "--select", "{{ node }}"]
  build:      ["dbt", "build", "--select", "{{ node }}"]
  seed_build: ["dbt", "seed", "--select", "{{ node }}"]
  parse:      ["dbt", "parse"]
  future_key: ["not yet understood by this build"]
  compile:
    command:       ["dbt", "compile", "--profiles-dir", "/project"]
    manifest_path: "/project/target/manifest.json"
`)
	logger, buf := captureLogger()
	r, err := Load(path, logger)
	require.NoError(t, err)
	require.NotNil(t, r)
	assert.Equal(t, []string{"dbt", "run", "--select", "orders"},
		r.NodeCommand("any-service", pkg_model.OperationRun, pkg_model.NodeTypeDbtModel, "orders"))
	assert.Contains(t, buf.String(), "future_key", "warning must name the unknown field")
}

func TestLoad_MalformedYAMLStillFatalEvenWithUnknownFieldTolerance(t *testing.T) {
	path := writeConfig(t, "default: [not a map")
	_, err := Load(path, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse dbt commands config")
}
