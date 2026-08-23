package event

// PROpened mirrors the remediation.pr_opened:v1 wire payload agent-remediation
// emits when an operator opens a fix PR from a proposal.
type PROpened struct {
	ProposalID string `json:"proposal_id"`
	ReleaseID  string `json:"release_id"`
	NodeID     string `json:"node_id"`
	PrURL      string `json:"pr_url"`
	PrNumber   int    `json:"pr_number"`
	OpenedBy   string `json:"opened_by"`
	OpenedAt   string `json:"opened_at"` // RFC3339
}
