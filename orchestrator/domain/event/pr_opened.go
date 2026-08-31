package event

// PROpened mirrors the remediation.pr_opened:v1 wire payload agent-remediation
// emits when an operator opens a fix PR from a proposal.
type PROpened struct {
	ProposalID string `json:"proposal_id"`
	ReleaseID  string `json:"release_id"`
	NodeID     string `json:"node_id"`
	// ResolvedNodeIDs is every failing node the one PR fixes. NodeID is the
	// representative of that set; a payload emitted before the field existed
	// carries only NodeID.
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	PrURL           string   `json:"pr_url"`
	PrNumber        int      `json:"pr_number"`
	OpenedBy        string   `json:"opened_by"`
	OpenedAt        string   `json:"opened_at"` // RFC3339
	// Service is the service whose fix this PR carries. One PR always targets
	// exactly one service, shared by every node in ResolvedNodeIDs.
	Service string `json:"service"`
}

// ResolvedNodes returns the nodes the PR fixes: the resolved set when the
// payload carries one, otherwise the single representative node.
func (p PROpened) ResolvedNodes() []string {
	if len(p.ResolvedNodeIDs) > 0 {
		return p.ResolvedNodeIDs
	}
	return []string{p.NodeID}
}
