package ports

// StopMetrics counts the stops of a cancelled run's work.
type StopMetrics interface {
	// AttemptStopped counts an attempt reported stopped.
	AttemptStopped()
	// JobReaped counts a Job deleted because no row deploys it: its run is
	// fenced ("fenced_run"), or its row ended before it was deployed
	// ("ended_undeployed").
	JobReaped(reason string)
}
