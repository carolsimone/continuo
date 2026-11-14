package repofs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// seedCheckout writes files into a new directory and returns it.
func seedCheckout(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, c := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(t, os.WriteFile(full, []byte(c), 0o600))
	}
	return root
}

func readCheckout(t *testing.T, root, p string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Clean(filepath.Join(root, filepath.FromSlash(p)))) //nolint:gosec // G304: a file of the checkout the test made.
	require.NoError(t, err)
	return string(b)
}

func TestCheckoutPatcher_WritesTheFilesAndReturnsWhatEachPathHeld(t *testing.T) {
	root := seedCheckout(t, map[string]string{"svc/contract/a.yaml": "old a"})

	originals, err := CheckoutPatcher{}.Apply(root, []ports.ProposedFile{
		{Path: "svc/contract/a.yaml", Content: "new a"},
		{Path: "svc/contract/nested/new.yaml", Content: "brand new"},
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"svc/contract/a.yaml": "old a", "svc/contract/nested/new.yaml": ""}, originals,
		"a path the checkout does not have reads as empty")
	assert.Equal(t, "new a", readCheckout(t, root, "svc/contract/a.yaml"))
	assert.Equal(t, "brand new", readCheckout(t, root, "svc/contract/nested/new.yaml"), "missing directories are created")
}

// Every original is read before any file is written, so a path named twice
// reports the repository's own content, and the last file named wins.
func TestCheckoutPatcher_ReadsEveryOriginalBeforeWriting(t *testing.T) {
	root := seedCheckout(t, map[string]string{"a.yaml": "repo"})

	originals, err := CheckoutPatcher{}.Apply(root, []ports.ProposedFile{
		{Path: "a.yaml", Content: "first"},
		{Path: "a.yaml", Content: "second"},
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a.yaml": "repo"}, originals)
	assert.Equal(t, "second", readCheckout(t, root, "a.yaml"))
}

func TestCheckoutPatcher_APathThatCannotBeReadFailsBeforeAnythingIsWritten(t *testing.T) {
	// A directory where the file should go makes the read of the original fail.
	root := seedCheckout(t, map[string]string{"dir/inner.txt": "x", "b.yaml": "old b"})

	_, err := CheckoutPatcher{}.Apply(root, []ports.ProposedFile{
		{Path: "b.yaml", Content: "new b"},
		{Path: "dir", Content: "x"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "read dir before applying the fix")
	assert.Equal(t, "old b", readCheckout(t, root, "b.yaml"))
}

func TestCheckoutPatcher_APathOutsideTheCheckoutIsRefusedBeforeAnythingIsWritten(t *testing.T) {
	for _, p := range []string{"../escape.yaml", "a/../../escape.yaml", "/etc/escape.yaml", ""} {
		t.Run(p, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "checkout")
			require.NoError(t, os.MkdirAll(root, 0o750))

			_, err := CheckoutPatcher{}.Apply(root, []ports.ProposedFile{
				{Path: "inside.yaml", Content: "x"},
				{Path: p, Content: "x"},
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "is outside the checkout")
			_, statErr := os.Stat(filepath.Join(root, "inside.yaml"))
			assert.True(t, os.IsNotExist(statErr), "nothing is written")
			_, statErr = os.Stat(filepath.Join(parent, "escape.yaml"))
			assert.True(t, os.IsNotExist(statErr), "nothing is written outside")
		})
	}
}
