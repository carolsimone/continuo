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

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
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
			name:    "unknown top-level key",
			yaml:    "defualt:\n  run: [\"dbt\"]",
			wantErr: "field defualt not found",
		},
		{
			name:    "unknown operation key",
			yaml:    "default:\n  runn: [\"dbt\", \"run\", \"--select\", \"{{ node }}\"]",
			wantErr: "field runn not found",
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
