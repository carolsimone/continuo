package ports

import "context"

// ReleaseSchema reads whether the release-controller database holds the
// schema its migrations build.
type ReleaseSchema interface {
	// Initialized reports whether the tables of the release checks, the live
	// release and the production pointers (release_checks, live_release and
	// production_pointers) exist.
	Initialized(ctx context.Context) (bool, error)
}
