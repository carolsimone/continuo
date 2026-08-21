package ports

// NodeIdentity is the part of a python node's contract entry that says WHICH
// node the entry is, as opposed to what the node promises. Validation judges
// the promise — the declared reads, the output columns, the config — and a fix
// is free to change any of it. The fields here are the other half: change one
// and the entry describes a different node, so an edit that touches them is
// never a repair of the node it was asked to repair.
type NodeIdentity struct {
	// Schema and Table are the node's identity across the whole system: the
	// relation it writes, and the "<schema>.<table>" id every trigger, graph
	// node, and release verdict names it by.
	Schema string
	Table  string
	// Script is the repository path of the program that produces the relation.
	// Empty for a contract-only node kind (e.g. python-csv), which has none.
	Script string
	// Kind is the node's declared "kind" (e.g. python-model, python-csv). It
	// says which rules a fix may rely on — a python-model node's script must
	// keep performing every read its contract declares, while a python-csv
	// node has no script at all — so a fix that silently flips it changes
	// which of those rule sets governs the node without changing anything a
	// human reviewing the diff would necessarily notice. Comparing it as
	// identity refuses that regardless of which lane produced the answer.
	Kind string
	// Owner, Schedule, and Criticality are the operational fields a run is
	// scheduled and escalated by; they belong to whoever owns the node, not to
	// whatever is being repaired in it.
	Owner       string
	Schedule    string
	Criticality string
}

// NodeDeclaration is one entry of a contract document's "nodes:" list: which
// node the entry is, and the names it gives the upstream relations it reads.
//
// The two are kept apart because an edit is judged differently against each.
// Identity may not change at all. ReadKeys may gain names and the SQL behind
// any of them may be rewritten — that is most of what repairing a contract
// means — but a name that was there must still be there: validation checks only
// the reads a contract still declares, and the node's script, which this system
// does not edit, still performs the one that was dropped.
type NodeDeclaration struct {
	Identity NodeIdentity
	// ReadKeys are the keys of the entry's "reads:" mapping, in declaration
	// order. An entry declaring no reads has none.
	ReadKeys []string
}

// ContractInspector reads the node declarations out of one contract yaml
// document's text, without reference to any repository layout.
//
// It exists so the application layer can compare what a document declared
// before an edit with what it declares after one, and refuse an edit that
// removed, renamed, or re-identified a node, or that dropped one of the reads
// an entry declared — while the yaml deserialization itself stays in an adapter.
type ContractInspector interface {
	// Declarations parses yamlText as a contract document and returns every
	// node under its "nodes:" list, in declaration order. A document with no
	// such list yields no declarations and no error, since a yaml file that
	// declares nothing is not an error to a caller comparing declarations. Text
	// that is not valid yaml at all returns an error.
	Declarations(yamlText string) ([]NodeDeclaration, error)
}
