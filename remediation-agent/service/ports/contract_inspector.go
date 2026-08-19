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
	Script string
	// Owner, Schedule, and Criticality are the operational fields a run is
	// scheduled and escalated by; they belong to whoever owns the node, not to
	// whatever is being repaired in it.
	Owner       string
	Schedule    string
	Criticality string
}

// ContractInspector reads the node declarations out of one contract yaml
// document's text, without reference to any repository layout.
//
// It exists so the application layer can compare what a document declared
// before an edit with what it declares after one, and refuse an edit that
// removed, renamed, or re-identified a node — while the yaml deserialization
// itself stays in an adapter.
type ContractInspector interface {
	// Identities parses yamlText as a contract document and returns the
	// identity of every node under its "nodes:" list, in declaration order. A
	// document with no such list yields no identities and no error, since a
	// yaml file that declares nothing is not an error to a caller comparing
	// declarations. Text that is not valid yaml at all returns an error.
	Identities(yamlText string) ([]NodeIdentity, error)
}
