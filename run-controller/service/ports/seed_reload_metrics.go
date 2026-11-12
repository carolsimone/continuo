package ports

// SeedReloadMetrics counts the seed loads that ended and the tasks of a seed
// reload that a seed rule stopped.
type SeedReloadMetrics interface {
	// SeedReloadEnded counts one seed load that ended: action is load or
	// full_refresh, the operation the seed's task applied, and outcome is
	// loaded, stopped (its preflight stopped it), refused (no command can
	// apply it, so it never started a container) or failed.
	SeedReloadEnded(action, outcome string)

	// SeedReloadStopped counts one task of a seed reload, a seed's load or a
	// view's rebuild, that a seed rule stopped before it changed the
	// warehouse, by the failure reason it ended with.
	SeedReloadStopped(reason string)
}
