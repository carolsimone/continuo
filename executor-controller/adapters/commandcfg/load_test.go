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
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestLoad_EmptyPathUsesDefaults(t *testing.T) {
	r, err := Load("", testLogger())
	require.NoError(t, err)
	assert.Equal(t, []string{"dbt", "run", "--select", "t"},
		r.NodeCommand("svc", pkg_model.NodeTypeDbtModel, "t"))
}

func TestLoad_MissingFileUsesDefaults(t *testing.T) {
	r, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"), testLogger())
	require.NoError(t, err)
	assert.Equal(t, []string{"dbt", "seed", "--select", "t"},
		r.NodeCommand("svc", pkg_model.NodeTypeDbtSeed, "t"))
}

func TestLoad_ValidFile(t *testing.T) {
	path := writeConfig(t, `
default:
  run: ["dbt", "run", "--select", "{{ node }}"]
services:
  wise:
    run: ["wise-dbt", "run", "--select", "{{ node }}"]
    seed_build: ["wise-dbt", "seed", "--select", "{{ node }}", "--schema", "{{ target_schema }}"]
    compile:
      command: ["wise-dbt", "compile", "--profiles-dir", "/project"]
      manifest_path: "/project/target/manifest.json"
`)
	_, err := Load(path, testLogger())
	require.NoError(t, err)
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
			name:    "empty argv",
			yaml:    "default:\n  run: []",
			wantErr: "default.run: must not be empty",
		},
		{
			name:    "run missing node placeholder",
			yaml:    "services:\n  wise:\n    run: [\"wise-dbt\", \"run\"]",
			wantErr: "services.wise.run: missing required {{ node }} placeholder",
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
			name:    "seed_build missing node placeholder",
			yaml:    "default:\n  seed_build: [\"dbt\", \"seed\", \"--schema\", \"{{ target_schema }}\"]",
			wantErr: "default.seed_build: missing required {{ node }} placeholder",
		},
		{
			name:    "compile with placeholder",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\", \"{{ node }}\"]\n    manifest_path: \"/p/m.json\"",
			wantErr: "default.compile.command: unknown placeholder {{ node }}",
		},
		{
			name:    "compile without manifest_path",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\"]",
			wantErr: "default.compile.manifest_path: required",
		},
		{
			name:    "compile relative manifest_path",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\"]\n    manifest_path: \"target/manifest.json\"",
			wantErr: "default.compile.manifest_path: must be an absolute path",
		},
		{
			name:    "compile manifest_path with placeholder",
			yaml:    "default:\n  compile:\n    command: [\"dbt\", \"compile\"]\n    manifest_path: \"/project/{{ node }}/manifest.json\"",
			wantErr: "default.compile.manifest_path: placeholders are not allowed",
		},
		{
			name:    "compile empty command",
			yaml:    "default:\n  compile:\n    command: []\n    manifest_path: \"/p/m.json\"",
			wantErr: "default.compile.command: must not be empty",
		},
		{
			name:    "service with no operations",
			yaml:    "services:\n  wise: {}",
			wantErr: "services.wise: no operations defined",
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
