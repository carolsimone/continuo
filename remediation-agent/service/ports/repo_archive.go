package ports

import "context"

// RepoArchive fetches a repository's full source tree at a specific commit as
// a local directory, for a caller that needs to search across many files
// rather than read one file at a time — unlike SourceReader, which streams
// individual files from the GitHub Contents API.
type RepoArchive interface {
	// Fetch downloads and extracts the repository tarball for repo at ref and
	// returns the local filesystem path to its root directory, with GitHub's
	// single generated top-level directory already stripped so rootDir's
	// contents are the repository root itself. A missing repo/ref maps to
	// ErrSourceNotFound.
	//
	// cleanup removes the extracted tree and must be called exactly once by
	// the caller when it is done with rootDir; calling it more than once is
	// safe and a no-op after the first call. On any error return, Fetch has
	// already removed anything it created on disk and returns a nil cleanup,
	// so the caller must not call cleanup on the error path.
	Fetch(ctx context.Context, repo, ref string) (rootDir string, cleanup func(), err error)
}
