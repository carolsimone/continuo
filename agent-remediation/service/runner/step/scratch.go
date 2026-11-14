package step

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"sync"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// ErrScratchClosed is returned by PatchedCheckout once the Scratch is closed.
var ErrScratchClosed = errors.New("the scratch checkout is closed")

// ErrPathOutsideCheckout is returned by PatchedCheckout for a proposed file
// whose path is empty, absolute or resolves outside the checkout. Nothing is
// fetched or written for such a draft.
var ErrPathOutsideCheckout = errors.New("a proposed file's path is outside the checkout")

// Scratch is one pre-flight pass's patched checkout of one cluster: the
// repository at the trigger's commit with the draft's files written over it.
// The checkout is fetched and patched on the first PatchedCheckout call, every
// later call of the same pass shares it, and Close removes it. A new pass makes
// a new Scratch, so it always fetches again.
type Scratch struct {
	archive ports.RepoArchive
	patcher ports.CheckoutPatcher
	repo    string
	commit  string

	mu        sync.Mutex
	closed    bool
	root      string
	cleanup   func()
	originals map[string]string
}

// NewScratch builds the Scratch of one pass: archive fetches the repository at
// commit, and patcher writes the draft's files over the checkout. It fetches
// nothing until the first PatchedCheckout.
func NewScratch(archive ports.RepoArchive, patcher ports.CheckoutPatcher, repo, commit string) *Scratch {
	return &Scratch{archive: archive, patcher: patcher, repo: repo, commit: commit}
}

// PatchedCheckout returns the root of the checkout with files written into it
// and what each file held before, keyed by the path as the draft named it (a
// path the repository does not have reads as empty). The first call fetches the
// repository and applies files; every later call of the pass returns the same
// root and originals and applies nothing, whatever files it is given.
//
// A path outside the checkout is refused with ErrPathOutsideCheckout before
// anything is fetched. A failed fetch or apply leaves no checkout behind, so
// the next call starts over.
func (s *Scratch) PatchedCheckout(ctx context.Context, files []ports.ProposedFile) (root string, originals map[string]string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", nil, ErrScratchClosed
	}
	if s.root != "" {
		return s.root, maps.Clone(s.originals), nil
	}
	for _, f := range files {
		if !filepath.IsLocal(filepath.FromSlash(f.Path)) {
			return "", nil, fmt.Errorf("%w: %q", ErrPathOutsideCheckout, f.Path)
		}
	}

	root, cleanup, err := s.archive.Fetch(ctx, s.repo, s.commit)
	if err != nil {
		return "", nil, fmt.Errorf("fetch repo %s@%s: %w", s.repo, s.commit, err)
	}
	originals, err = s.patcher.Apply(root, files)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	s.root, s.cleanup, s.originals = root, cleanup, originals
	return root, maps.Clone(originals), nil
}

// Originals returns what the files of the pass held before they were applied,
// or nil when the pass made no checkout. It stays available after Close.
func (s *Scratch) Originals() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.originals == nil {
		return nil
	}
	return maps.Clone(s.originals)
}

// Close removes the checkout. It is safe to call more than once and when no
// checkout was made.
func (s *Scratch) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.cleanup != nil {
		s.cleanup()
		s.cleanup = nil
	}
}
