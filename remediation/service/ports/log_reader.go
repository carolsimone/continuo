// Package ports declares the technical collaborators the application layer
// depends on; adapters implement these.
package ports

import (
	"context"
	"errors"
)

// ErrLogNotFound signals that the object referenced by a log URI does not
// exist. Distinct from a transient read failure: callers treat not-found as
// "log unavailable" (classify unknown) rather than retrying.
var ErrLogNotFound = errors.New("log not found")

// LogReader fetches the textual contents of a dbt log given its storage URI.
type LogReader interface {
	Fetch(ctx context.Context, uri string) (string, error)
}
