package remediation

import (
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// Trigger is the decoded heal trigger that starts one remediation: the
// release-level facts every fix needs, and one TriggerNode per failing node the
// classifier judged healable. One rejected release yields one trigger per
// remediation round, so a trigger names a whole failing set rather than a
// single node.
type Trigger struct {
	TenantID  string
	Source    string
	ReleaseID string
	// Round is the release's remediation round this trigger belongs to (1 for
	// the rejection itself).
	Round     int
	Repo      string
	CommitSHA string
	// CodeBundleURI locates the release's code-bundle document, which carries
	// every parsed node's raw source. Empty for a compile-stage and a
	// parse-stage rejection, both of which precede the successful parse that
	// produces the bundle; every later rejection (duplicate_table included)
	// carries it.
	CodeBundleURI string
	// Nodes is the release's healable failing set, one entry per node.
	Nodes []TriggerNode
}

// TriggerNode is one failing node inside a batched trigger, with the evidence
// its fix reads.
type TriggerNode struct {
	NodeID string
	// RelationID is the contested physical relation for a duplicate-relation
	// failure, distinct from NodeID (the target claimant's own unique_id): the
	// two differ whenever the target already carries an alias. Empty for every
	// other source.
	RelationID     string
	ErrorSignature string
	Category       string
	// Reason is the classifier's finer-grained reason within Category; with
	// Category it forms the fallback precedent-lookup key when the exact
	// signature has no recorded matches.
	Reason string
	// ErrorExcerpt is the classifier's key error line for this failure (capped
	// at 4 KiB).
	ErrorExcerpt         string
	DBTLogURI            string
	CandidateArtifactURI string
	// FilePath is the offending dbt-project-relative source path. For compile
	// failures it is extracted from the dbt log. For validation, seed_build,
	// and duplicate_table failures it is threaded from the candidate topology.
	// Empty on a validation or seed_build node falls back to the catalog
	// graph's node locator; a duplicate_table node has no such fallback.
	FilePath string
	// Service is the owning dbt service name for the failing node. Set for
	// validation, seed_build, and duplicate_table failures from the candidate
	// topology. Empty for compile failures, where NodeID acts as the service
	// discriminator.
	Service string
	// NodeType is the failing node's kind (dbt-model, dbt-seed, dbt-snapshot,
	// python-node, python-csv, or python-api), set on validation,
	// duplicate-relation and parse failures. It selects how the node is fixed:
	// a python-node, whose source is not a single readable file, is fixed in
	// the contract yaml declaring it.
	NodeType string
	// OtherService and OtherFilePath locate the competing node that also
	// produces the contested relation (RelationID), set on a duplicate-relation
	// failure.
	OtherService  string
	OtherFilePath string
	// ChangedAncestors are the node's transitive upstream ancestors whose
	// content changed in the rejected release, each with the location that
	// release's candidate topology declares for it. They let failures that
	// share a changed ancestor be grouped, fixed once at that ancestor, and
	// edited where THIS release holds it: an ancestor the release renamed or
	// moved is still at its old path in the promoted graph.
	ChangedAncestors []ChangedAncestor
}

// ChangedAncestor is one changed upstream of a failing node. FilePath and
// Service are the location the rejected release's candidate topology declares;
// both are empty on a rejection that carries no per-node topology, and the fix
// then falls back to the promoted graph. Depth is the ancestor's minimum
// upstream hop distance from the failing node (1 = direct upstream); 0 on a
// payload that carries none.
type ChangedAncestor struct {
	NodeID   string
	FilePath string
	Service  string
	Depth    int
}

// NodeIDs returns the trigger's failing node ids, sorted, so every derived
// order does not depend on the order the classifier listed the failures in.
// The trigger's own Nodes slice is left as given.
func (t Trigger) NodeIDs() []string {
	ids := make([]string, 0, len(t.Nodes))
	for _, n := range t.Nodes {
		ids = append(ids, n.NodeID)
	}
	sort.Strings(ids)
	return ids
}

// Services is the sorted set of services the trigger's failing nodes belong to.
func (t Trigger) Services() []string {
	names := make([]string, 0, len(t.Nodes))
	for _, n := range t.Nodes {
		names = append(names, n.Service)
	}
	return proposal.UnionServices(names)
}

// Failure is one cluster of failing nodes to fix: the trigger it came from, the
// cluster's shape, the node the fix edits, its member nodes and the proposal
// attempt it belongs to.
type Failure struct {
	Trigger      Trigger
	Kind         workflow.ClusterKind
	TargetNodeID string
	Target       TriggerNode
	Members      []TriggerNode
	// Attempt is the proposal attempt number of the try.
	Attempt int
}

// PrecedentSubject is the node whose failure precedents inform the fix: the
// first member of an upstream cluster (the target is a changed ancestor with no
// failure of its own), otherwise the target.
func (f Failure) PrecedentSubject() TriggerNode {
	if f.Kind != workflow.ClusterSingle && len(f.Members) > 0 {
		return f.Members[0]
	}
	return f.Target
}
