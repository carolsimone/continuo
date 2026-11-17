package ports

import (
	"context"
	"errors"
)

// ErrSourceNotFound signals the requested file/ref does not exist in the repo.
var ErrSourceNotFound = errors.New("source file not found")

// MaxSourceBytes is the largest file a SourceReader returns. Bounding it keeps
// a reader's memory use and the text a model is shown in check.
const MaxSourceBytes = 1 << 20 // 1 MiB

// ErrSourceTooLarge signals the requested file is larger than MaxSourceBytes.
// It is a fact about the file, not a fault of the reader: reading it again
// returns the same error.
var ErrSourceTooLarge = errors.New("source file too large to read")

// SourceReader fetches repository content at a specific ref. Read-only.
type SourceReader interface {
	// ReadFile returns a file's text. Missing file/ref → ErrSourceNotFound; a
	// file over MaxSourceBytes → ErrSourceTooLarge.
	ReadFile(ctx context.Context, repo, ref, path string) (string, error)
	// ListDir returns the repo-relative paths of the files (not directories)
	// directly under dir at ref. A missing directory → ErrSourceNotFound.
	ListDir(ctx context.Context, repo, ref, dir string) ([]string, error)
}
