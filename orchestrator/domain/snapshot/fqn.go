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

// fqnKeys returns the keys of an FQN set as a slice. Selectors snapshot the
// current rebase set before a batched reader call so they can grow the set from
// the result without mutating the map they are iterating.
func fqnKeys(set map[FQN]struct{}) []FQN {
	out := make([]FQN, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	return out
}
