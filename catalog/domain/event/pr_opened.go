package event

// PROpened mirrors the remediation.pr_opened:v1 wire payload agent-remediation
// emits when an operator opens a fix PR from a proposal.
type PROpened struct {
	ProposalID string
	ReleaseID  string
	NodeID     string
	// ResolvedNodeIDs is every failing node the one PR fixes. NodeID is the
	// representative of that set; a payload emitted before the field existed
	// carries only NodeID.
	ResolvedNodeIDs []string
	PrURL           string
	PrNumber        int
	OpenedBy        string
	OpenedAt        string // RFC3339
	// Service is the service whose fix this PR carries. One PR always targets
	// exactly one service, shared by every node in ResolvedNodeIDs.
	Service string
}

// ResolvedNodes returns the nodes the PR fixes: the resolved set when the
// payload carries one, otherwise the single representative node.
func (p PROpened) ResolvedNodes() []string {
	if len(p.ResolvedNodeIDs) > 0 {
		return p.ResolvedNodeIDs
	}
	return []string{p.NodeID}
}
