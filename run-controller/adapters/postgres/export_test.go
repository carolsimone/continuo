package postgres

// The SQL of the run-planning hot paths, of the rebase preview's duration read
// and of the schedule graph's last-run read, for plan-shape tests.
const (
	ReadyCandidatesQueryForTest      = readyCandidatesQuery
	PendingDescendantsQueryForTest   = pendingDescendantsQuery
	LastSuccessDurationsQueryForTest = lastSuccessDurationsQuery
	LastRunsQueryForTest             = lastRunsQuery
)
