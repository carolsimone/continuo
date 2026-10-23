package ports

// RunMetrics counts what state's reconcile loop does to runs.
type RunMetrics interface {
	// RunStalled counts one run failed as stalled.
	RunStalled()
}
