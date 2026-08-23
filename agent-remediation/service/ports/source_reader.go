package ports

import (
	"context"
	"errors"
)

// ErrSourceNotFound signals the requested file/ref does not exist in the repo.
var ErrSourceNotFound = errors.New("source file not found")

// SourceReader fetches repository content at a specific ref. Read-only.
type SourceReader interface {
	// ReadFile returns a file's text. Missing file/ref → ErrSourceNotFound.
	ReadFile(ctx context.Context, repo, ref, path string) (string, error)
	// ListDir returns the repo-relative paths of the files (not directories)
	// directly under dir at ref. A missing directory → ErrSourceNotFound.
	ListDir(ctx context.Context, repo, ref, dir string) ([]string, error)
}
