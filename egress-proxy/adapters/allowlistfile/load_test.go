package allowlistfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "allowlist.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func TestLoad_ValidFile(t *testing.T) {
	a, err := Load(write(t, `
entries:
  - host: api.anthropic.com
    port: 443
  - host: gpu-box.internal
    port: 8000
    allowPrivate: true
`))
	require.NoError(t, err)
	assert.Equal(t, 2, a.Len())
	e, ok := a.Match("gpu-box.internal", 8000)
	require.True(t, ok)
	assert.True(t, e.AllowPrivate)
	e, ok = a.Match("api.anthropic.com", 443)
	require.True(t, ok)
	assert.False(t, e.AllowPrivate)
}

func TestLoad_RefusesInvalidFiles(t *testing.T) {
	for name, body := range map[string]string{
		"unknown field":     "entries:\n  - host: a.example.com\n    port: 443\n    private: true\n",
		"port too large":    "entries:\n  - host: a.example.com\n    port: 70000\n",
		"port wraps to one": "entries:\n  - host: a.example.com\n    port: 65537\n",
		"port negative":     "entries:\n  - host: a.example.com\n    port: -1\n",
		"port zero":         "entries:\n  - host: a.example.com\n    port: 0\n",
		"port missing":      "entries:\n  - host: a.example.com\n",
		"IP host":           "entries:\n  - host: 10.0.0.1\n    port: 443\n",
		"no entries":        "entries: []\n",
		"empty file":        "",
		"not YAML":          "entries: [\n",
		"second document":   "entries:\n  - host: a.example.com\n    port: 443\n---\nentries:\n  - host: b.example.com\n    port: 443\n",
		"trailing garbage":  "entries:\n  - host: a.example.com\n    port: 443\n---\n[\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(write(t, body))
			require.Error(t, err)
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	require.Error(t, err)
}

// A bad entry is reported by its index and host; the error carries nothing
// else from the file.
func TestLoad_BadEntryErrorNamesIndexAndHostOnly(t *testing.T) {
	_, err := Load(write(t, `
entries:
  - host: ok.example.com
    port: 443
  - host: secret-looking-host.internal
    port: 70000
    allowPrivate: true
`))
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "entry 1")
	assert.Contains(t, msg, "secret-looking-host.internal")
	assert.NotContains(t, msg, "ok.example.com")
	assert.NotContains(t, msg, "allowPrivate")
}
