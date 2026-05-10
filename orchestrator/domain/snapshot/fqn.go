package snapshot

// FQN is the fully-qualified identity of a Table node — used as a map key
// throughout the selector policies. Service / Schema / Table are all required.
type FQN struct {
	Service string
	Schema  string
	Table   string
}
