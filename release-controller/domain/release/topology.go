package release

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

type Topology []Node

type Node struct {
	UniqueID   string
	SchemaName string
	TableName  string
	// ResolvedRelationID is "<schema>.<resolved name>", lowercased: the
	// physical relation this node's build actually writes. UniqueID is keyed
	// on the DECLARED name; this is keyed on the RESOLVED one — a dbt node's
	// alias when it overrides one, else the same declared name. Two nodes
	// with different declared names but the same alias write the same
	// warehouse table, a collision UniqueID alone cannot see. Empty on a
	// payload from before this field existed; DuplicateClaims falls back to
	// UniqueID in that case.
	ResolvedRelationID string
	ServiceName        string
	NodeType           string
	ContentHash        string
	TestCount          int
	ImageTag           string
	UpstreamUniqueIDs  []string
	Schedule           string
	OriginalFilePath   string
	// SecretRef is the continuo-api-* Secret a python-api node's pod receives
	// as env vars; empty for every other node.
	SecretRef string
}

// WithoutTests returns a copy of the topology without dbt-test nodes. A test
// is validation-only: it is bind-checked in the candidate schema and kept in
// current_prod so an unchanged test is not re-checked next release, but it is
// never published to the orchestrator, which schedules and draws relations.
func (t Topology) WithoutTests() Topology {
	out := make(Topology, 0, len(t))
	for _, n := range t {
		if n.NodeType == string(pkg_model.NodeTypeDbtTest) {
			continue
		}
		out = append(out, n)
	}
	return out
}
