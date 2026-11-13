package repofs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// CheckoutPatcher writes proposed files over a repository checkout on local
// disk.
type CheckoutPatcher struct{}

var _ ports.CheckoutPatcher = CheckoutPatcher{}

// Apply writes each proposed file into the checkout at root and returns what
// each path held beforehand, keyed by the path as the file names it. Every
// path is checked to be local to the checkout before anything is read, and
// every original is read before anything is written, so the originals describe
// the repository's own content; a path the checkout does not have reads as
// empty, which diffs as a new file.
func (CheckoutPatcher) Apply(root string, files []ports.ProposedFile) (map[string]string, error) {
	for _, f := range files {
		if !filepath.IsLocal(filepath.FromSlash(f.Path)) {
			return nil, fmt.Errorf("proposed file %q is outside the checkout", f.Path)
		}
	}
	originals := make(map[string]string, len(files))
	for _, f := range files {
		full := filepath.Join(root, filepath.FromSlash(f.Path))
		existing, err := os.ReadFile(full) //nolint:gosec // G304: full is a path inside the checkout; Apply refused every path that is not local.
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read %s before applying the fix: %w", f.Path, err)
		}
		originals[f.Path] = string(existing)
	}
	for _, f := range files {
		full := filepath.Join(root, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			return nil, fmt.Errorf("create directory for %s: %w", f.Path, err)
		}
		if err := os.WriteFile(full, []byte(f.Content), 0o600); err != nil {
			return nil, fmt.Errorf("apply fix to %s: %w", f.Path, err)
		}
	}
	return originals, nil
}
