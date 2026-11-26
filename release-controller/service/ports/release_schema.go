package ports

import "context"

// ReleaseSchema reads whether the release-controller database holds the
// schema its migrations build.
type ReleaseSchema interface {
	// Initialized reports whether the release schema's base tables
	// (release_checks, current_prod and service_prod) exist.
	Initialized(ctx context.Context) (bool, error)
}
