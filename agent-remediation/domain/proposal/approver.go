package proposal

import (
	"slices"
	"strings"
)

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

// MergeApprovers is the approvers existing and added hold together, sorted by
// node id, with one entry per node: an approver in added takes the place of the
// entry existing holds for its node. Neither slice is changed. It is nil when
// both are empty.
func MergeApprovers(existing, added []Approver) []Approver {
	byNode := make(map[string]Approver, len(existing)+len(added))
	for _, a := range existing {
		byNode[a.NodeID] = a
	}
	for _, a := range added {
		byNode[a.NodeID] = a
	}
	if len(byNode) == 0 {
		return nil
	}
	out := make([]Approver, 0, len(byNode))
	for _, a := range byNode {
		out = append(out, a)
	}
	slices.SortFunc(out, func(a, b Approver) int { return strings.Compare(a.NodeID, b.NodeID) })
	return out
}

// ApproversForNodes is the approvers that name one of nodes, in the order
// approvers holds them. It is nil when none does.
func ApproversForNodes(approvers []Approver, nodes []string) []Approver {
	var out []Approver
	for _, a := range approvers {
		if slices.Contains(nodes, a.NodeID) {
			out = append(out, a)
		}
	}
	return out
}
