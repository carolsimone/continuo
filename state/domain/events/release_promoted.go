package events

// ReleasePromotedNode is one node of a promoted release's topology.
//
// This is a narrowed view of the release.promoted:v1 node: state only needs to
// recognise which nodes are seeds the release changed, so the fields that
// describe a node's place in the graph (upstreams, hashes, test counts) are not
// decoded here. orchestrator owns the full shape.
type ReleasePromotedNode struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	NodeType    string `json:"node_type"`
	Changed     bool   `json:"changed"`
}

// ReleasePromoted is the release.promoted:v1 payload as state reads it.
type ReleasePromoted struct {
	ReleaseID string                `json:"release_id"`
	Topology  []ReleasePromotedNode `json:"topology"`
}

// NodeTypeDBTSeed is the node_type a dbt seed carries in a promoted topology.
const NodeTypeDBTSeed = "dbt-seed"

// ChangedSeeds returns the nodes this release both changed and that are seeds —
// the work a promotion has to materialise into the production schema.
//
// An unchanged seed is skipped because its data is already built and its content
// hash is unchanged; rebuilding it would be a no-op that still costs a Job.
func (e ReleasePromoted) ChangedSeeds() []ReleasePromotedNode {
	out := make([]ReleasePromotedNode, 0, len(e.Topology))
	for _, n := range e.Topology {
		if n.Changed && n.NodeType == NodeTypeDBTSeed {
			out = append(out, n)
		}
	}
	return out
}
