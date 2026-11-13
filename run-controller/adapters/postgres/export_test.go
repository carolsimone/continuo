package postgres

// The SQL of the run-planning hot paths and of the rebase preview's duration
// read, for plan-shape tests.
const (
	ReadyCandidatesQueryForTest      = readyCandidatesQuery
	PendingDescendantsQueryForTest   = pendingDescendantsQuery
	LastSuccessDurationsQueryForTest = lastSuccessDurationsQuery
)

// TranslateRunEventForTest is the outbox row an event maps to, for the
// payload tests.
var TranslateRunEventForTest = translateRunEvent
