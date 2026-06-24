package ports

import (
	"context"
	"errors"
)

// ErrSourceNotFound signals the requested file/ref does not exist in the repo.
var ErrSourceNotFound = errors.New("source file not found")

// SourceReader fetches a repository file's text at a specific ref. Read-only.
type SourceReader interface {
	ReadFile(ctx context.Context, repo, ref, path string) (string, error)
}
