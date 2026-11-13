package ports

import "time"

// SeedReloadMetrics counts the seed loads that drop their table and ended, the
// seed loads the planner refused, and the tasks of a seed reload that a seed
// rule stopped, and times how long each view a seed load left missing or stale
// waited for its rebuild.
type SeedReloadMetrics interface {
	// SeedReloadEnded counts one seed load that drops its table (a seed task
	// with a preflight) and ended, or one the planner refused: action is load
	// or full_refresh, the operation the seed's task applied, and outcome is
	// loaded, stopped (its preflight stopped it), refused (no command can
	// apply it, so it never started a container) or failed.
	SeedReloadEnded(action, outcome string)

	// SeedReloadStopped counts one task of a seed reload, a seed's load or a
	// view's rebuild, that a seed rule stopped before it changed the
	// warehouse, by the failure reason it ended with.
	SeedReloadStopped(reason string)

	// ViewRebuilt observes after, how long a view a seed load left missing or
	// stale stayed so until a task of the view succeeded.
	ViewRebuilt(after time.Duration)
}
