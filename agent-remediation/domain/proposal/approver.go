package proposal

// Where an approver's owner was read from.
const (
	// ApproverSourceBundle: the owner the release's code bundle records for the
	// node (a dbt node's config meta.owner, a python node's contract team.name).
	ApproverSourceBundle = "bundle"
	// ApproverSourceContractFile: the team.name of the node's ODCS contract file
	// in the repository, for a node the code bundle names no owner for.
	ApproverSourceContractFile = "contract_file"
	// ApproverSourceNone: no owner was found for the node.
	ApproverSourceNone = "none"
)

// Approver is who must approve what a remediation did with one node: the
// node's owner, as Source names where it was read from. Owner is empty when
// Source is ApproverSourceNone.
type Approver struct {
	NodeID string
	Owner  string
	Source string
}
