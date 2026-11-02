// Package graphschema is the one table of the catalog graph's node labels,
// the properties that identify a node of each label within its tenant, and
// the relationship types that join them. The Neo4j schema, the export and
// import of a tenant's graph, and the Cypher tenant guard all read it.
package graphschema

// Label is a node label of the catalog graph.
type Label string

// The node labels of the catalog graph. Every node of every label carries
// tenant_id.
const (
	Node             Label = "Node"
	Meta             Label = "Meta"
	NodeVersion      Label = "NodeVersion"
	CodeUnit         Label = "CodeUnit"
	CodeUnitVersion  Label = "CodeUnitVersion"
	Rejection        Label = "Rejection"
	ErrorSignature   Label = "ErrorSignature"
	Proposal         Label = "Proposal"
	PullRequest      Label = "PullRequest"
	ProcessedMessage Label = "ProcessedMessage"
)

// labels lists every label with the properties that, beside tenant_id,
// identify one node of it.
var labels = []struct {
	label    Label
	identity []string
}{
	{Node, []string{"unique_id"}},
	{Meta, []string{"key"}},
	{NodeVersion, []string{"unique_id", "content_hash"}},
	{CodeUnit, []string{"unit_id"}},
	{CodeUnitVersion, []string{"unit_id", "checksum"}},
	{Rejection, []string{"release_id", "node_id"}},
	{ErrorSignature, []string{"signature"}},
	{Proposal, []string{"proposal_id"}},
	{PullRequest, []string{"proposal_id", "service"}},
	{ProcessedMessage, []string{"group", "message_id"}},
}

// Labels returns every node label of the catalog graph.
func Labels() []Label {
	out := make([]Label, 0, len(labels))
	for _, l := range labels {
		out = append(out, l.label)
	}
	return out
}

// Identity returns the properties that, beside tenant_id, identify one node of
// label l, in a fresh slice the caller may change. It returns nil for a label
// outside the schema.
func Identity(l Label) []string {
	for _, entry := range labels {
		if entry.label == l {
			return append([]string(nil), entry.identity...)
		}
	}
	return nil
}

// RelType is a relationship type of the catalog graph.
type RelType string

// The relationship types of the catalog graph. A relationship never joins
// nodes of two tenants.
const (
	RelCurrent      RelType = "CURRENT"
	RelDependsOn    RelType = "DEPENDS_ON"
	RelUsesCode     RelType = "USES_CODE"
	RelResolvedBy   RelType = "RESOLVED_BY"
	RelHasSignature RelType = "HAS_SIGNATURE"
	RelFailed       RelType = "FAILED"
	RelProposed     RelType = "PROPOSED"
	RelHasPR        RelType = "HAS_PR"
	RelEdited       RelType = "EDITED"
)

// RelTypes returns every relationship type of the catalog graph.
func RelTypes() []RelType {
	return []RelType{
		RelCurrent, RelDependsOn, RelUsesCode, RelResolvedBy, RelHasSignature,
		RelFailed, RelProposed, RelHasPR, RelEdited,
	}
}
