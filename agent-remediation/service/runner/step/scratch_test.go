package step_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// archive hands out a real directory per fetch, seeded with files, and removes
// it when the caller cleans up.
type archive struct {
	t        *testing.T
	seed     map[string]string
	err      error
	fetches  [][2]string
	dirs     []string
	cleanups int
}

func (a *archive) Fetch(_ context.Context, repo, ref string) (string, func(), error) {
	a.fetches = append(a.fetches, [2]string{repo, ref})
	if a.err != nil {
		return "", nil, a.err
	}
	dir := a.t.TempDir()
	for p, c := range a.seed {
		full := filepath.Join(dir, filepath.FromSlash(p))
		require.NoError(a.t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(a.t, os.WriteFile(full, []byte(c), 0o600))
	}
	a.dirs = append(a.dirs, dir)
	return dir, func() {
		a.cleanups++
		require.NoError(a.t, os.RemoveAll(dir))
	}, nil
}

var _ ports.RepoArchive = (*archive)(nil)

func read(t *testing.T, root, p string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(p)))
	require.NoError(t, err)
	return string(b)
}

func TestScratch_PatchedCheckoutAppliesTheFilesAndReturnsTheOriginals(t *testing.T) {
	a := &archive{t: t, seed: map[string]string{"svc/contract/a.yaml": "old a"}}
	s := step.NewScratch(a, "org/repo", "abc123")
	defer s.Close()

	root, originals, err := s.PatchedCheckout(context.Background(), []ports.ProposedFile{
		{Path: "svc/contract/a.yaml", Content: "new a"},
		{Path: "svc/contract/new.yaml", Content: "brand new"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"svc/contract/a.yaml": "old a", "svc/contract/new.yaml": ""}, originals)
	assert.Equal(t, "new a", read(t, root, "svc/contract/a.yaml"))
	assert.Equal(t, "brand new", read(t, root, "svc/contract/new.yaml"))
	assert.Equal(t, [][2]string{{"org/repo", "abc123"}}, a.fetches)
}

func TestScratch_ASecondCallInThePassReturnsTheSameCheckoutWithoutFetchingOrApplyingAgain(t *testing.T) {
	a := &archive{t: t, seed: map[string]string{"a.yaml": "old"}}
	s := step.NewScratch(a, "r", "c")
	defer s.Close()

	files := []ports.ProposedFile{{Path: "a.yaml", Content: "new"}}
	root1, originals1, err := s.PatchedCheckout(context.Background(), files)
	require.NoError(t, err)
	// The checkout holds the patched file now; applying again would capture it
	// as the original.
	root2, originals2, err := s.PatchedCheckout(context.Background(), files)
	require.NoError(t, err)

	assert.Equal(t, root1, root2)
	assert.Equal(t, originals1, originals2)
	assert.Equal(t, "old", originals2["a.yaml"])
	assert.Len(t, a.fetches, 1)
}

func TestScratch_ANewScratchFetchesAgain(t *testing.T) {
	a := &archive{t: t, seed: map[string]string{"a.yaml": "old"}}
	for range 2 {
		s := step.NewScratch(a, "r", "c")
		_, originals, err := s.PatchedCheckout(context.Background(), []ports.ProposedFile{{Path: "a.yaml", Content: "new"}})
		require.NoError(t, err)
		assert.Equal(t, "old", originals["a.yaml"])
		s.Close()
	}
	assert.Len(t, a.fetches, 2)
}

func TestScratch_CloseRemovesTheCheckoutAndIsSafeToRepeat(t *testing.T) {
	a := &archive{t: t, seed: map[string]string{"a.yaml": "old"}}
	s := step.NewScratch(a, "r", "c")
	root, _, err := s.PatchedCheckout(context.Background(), nil)
	require.NoError(t, err)

	s.Close()
	s.Close()

	_, statErr := os.Stat(root)
	assert.True(t, os.IsNotExist(statErr))
	assert.Equal(t, 1, a.cleanups)
}

func TestScratch_CloseWithNoCheckoutDoesNothing(t *testing.T) {
	a := &archive{t: t}
	s := step.NewScratch(a, "r", "c")
	s.Close()
	assert.Empty(t, a.fetches)
	assert.Nil(t, s.Originals())
}

func TestScratch_OriginalsAreKeptAfterClose(t *testing.T) {
	a := &archive{t: t, seed: map[string]string{"a.yaml": "old"}}
	s := step.NewScratch(a, "r", "c")
	_, _, err := s.PatchedCheckout(context.Background(), []ports.ProposedFile{{Path: "a.yaml", Content: "new"}})
	require.NoError(t, err)
	s.Close()

	assert.Equal(t, map[string]string{"a.yaml": "old"}, s.Originals())
}

func TestScratch_APatchedCheckoutAfterCloseIsRefused(t *testing.T) {
	a := &archive{t: t}
	s := step.NewScratch(a, "r", "c")
	s.Close()
	_, _, err := s.PatchedCheckout(context.Background(), nil)
	require.ErrorIs(t, err, step.ErrScratchClosed)
	assert.Empty(t, a.fetches)
}

func TestScratch_AFetchErrorIsReturnedWrappedAndTheNextCallTriesAgain(t *testing.T) {
	boom := errors.New("boom")
	a := &archive{t: t, err: boom}
	s := step.NewScratch(a, "r", "c")
	defer s.Close()

	_, _, err := s.PatchedCheckout(context.Background(), nil)
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "fetch repo r@c")
	assert.Nil(t, s.Originals())

	a.err = nil
	_, _, err = s.PatchedCheckout(context.Background(), nil)
	require.NoError(t, err)
	assert.Len(t, a.fetches, 2)
}

func TestScratch_ASourceNotFoundStaysMatchable(t *testing.T) {
	a := &archive{t: t, err: ports.ErrSourceNotFound}
	s := step.NewScratch(a, "r", "c")
	_, _, err := s.PatchedCheckout(context.Background(), nil)
	require.ErrorIs(t, err, ports.ErrSourceNotFound)
}

func TestScratch_APathOutsideTheCheckoutIsRefusedBeforeAnythingIsFetchedOrWritten(t *testing.T) {
	for _, p := range []string{"../escape.yaml", "a/../../escape.yaml", "/etc/escape.yaml", ""} {
		t.Run(p, func(t *testing.T) {
			a := &archive{t: t}
			s := step.NewScratch(a, "r", "c")
			defer s.Close()
			_, _, err := s.PatchedCheckout(context.Background(), []ports.ProposedFile{{Path: p, Content: "x"}})
			require.ErrorIs(t, err, step.ErrPathOutsideCheckout)
			assert.Empty(t, a.fetches)
		})
	}
}

func TestScratch_AFailedApplyRemovesTheCheckoutAndTheNextCallStartsOver(t *testing.T) {
	// A directory where the file should go makes the read of the original fail.
	a := &archive{t: t, seed: map[string]string{"dir/inner.txt": "x"}}
	s := step.NewScratch(a, "r", "c")
	defer s.Close()

	_, _, err := s.PatchedCheckout(context.Background(), []ports.ProposedFile{{Path: "dir", Content: "x"}})
	require.Error(t, err)
	assert.Equal(t, 1, a.cleanups)
	assert.Nil(t, s.Originals())

	root, originals, err := s.PatchedCheckout(context.Background(), []ports.ProposedFile{{Path: "dir/inner.txt", Content: "y"}})
	require.NoError(t, err)
	assert.Len(t, a.fetches, 2, "the second call fetches a new checkout")
	assert.Equal(t, map[string]string{"dir/inner.txt": "x"}, originals)
	assert.Equal(t, "y", read(t, root, "dir/inner.txt"))
}

func TestScratch_TheOriginalsAMapHandsOutAreCopies(t *testing.T) {
	a := &archive{t: t, seed: map[string]string{"a.yaml": "old"}}
	s := step.NewScratch(a, "r", "c")
	defer s.Close()
	files := []ports.ProposedFile{{Path: "a.yaml", Content: "new"}}

	_, first, err := s.PatchedCheckout(context.Background(), files)
	require.NoError(t, err)
	first["a.yaml"] = "tampered"
	_, second, err := s.PatchedCheckout(context.Background(), files)
	require.NoError(t, err)
	second["a.yaml"] = "tampered again"

	assert.Equal(t, map[string]string{"a.yaml": "old"}, s.Originals())
}
