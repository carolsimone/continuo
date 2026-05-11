package snapshot

// FQN is the fully-qualified identity of a Table node — used as a map key
// throughout the selector policies. Service / Schema / Table / ScheduleName
// together form the :Table graph identity (a single (service, schema, table,
// schedule) tuple addresses exactly one :Table node in Neo4j).
type FQN struct {
	Service      string
	Schema       string
	Table        string
	ScheduleName string
}
