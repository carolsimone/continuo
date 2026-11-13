// Package seedreload decides what the load of a seed does to the objects that
// read it. A seed's load either leaves its table in place or drops and
// re-creates it. When it drops the table, the views built on it go with it
// (schema_bound engines) or turn stale (late_binding engines), so the plan
// names the views the run rebuilds and the nodes below them that wait for
// their schedule. The package is pure: it reads a Graph and returns a Plan.
package seedreload

import (
	"slices"
	"strings"
)

// Node is one node of the topology, as the plan needs it.
type Node struct {
	// Key is the node's id, "<schema>.<table>" lowercased.
	Key string
	// NodeType is the topology node type, for example "dbt-model" or "python-node".
	NodeType string
	// Materialized is the dbt materialization, empty on a node that has none.
	Materialized string
	// FullRefresh is the seed's +full_refresh setting, nil while it is unset.
	FullRefresh *bool
	// Schedule is the schedule the node belongs to.
	Schedule string
	// Relation is the node's warehouse relation, lowercased.
	Relation string
}

// Graph is the topology of one release. Both lists are sorted by key.
type Graph interface {
	Node(key string) (Node, bool)
	Upstreams(key string) []string
	Downstreams(key string) []string
}

// Change is what differs between the seed a release carries and the one its
// table last loaded.
type Change string

const (
	// ChangeRows is a load whose columns are the ones the table already has.
	ChangeRows Change = "rows"
	// ChangeColumns is a load whose column fingerprint differs from the loaded one.
	ChangeColumns Change = "columns"
)

// ChangeOf compares the column fingerprint a seed last loaded with the live
// one. An empty fingerprint on either side is a row change.
func ChangeOf(recorded, live string) Change {
	if recorded != "" && live != "" && recorded != live {
		return ChangeColumns
	}
	return ChangeRows
}

// Trigger is what asks for the load.
type Trigger string

const (
	// TriggerLoad is a release whose seed content changed.
	TriggerLoad Trigger = "load"
	// TriggerFullRefresh is an operator's request for a full refresh of the seed.
	TriggerFullRefresh Trigger = "full_refresh"
)

// NormalizeObjects trims, lowercases, de-duplicates and sorts a list of
// "<schema>.<name>" object names and drops the empty ones. The result is never nil.
func NormalizeObjects(names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		n = strings.ToLower(strings.TrimSpace(n))
		if n != "" {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}
