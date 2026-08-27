package ports

import "context"

// ProposalSummary is the slice of a remediation attempt the retry decision needs.
type ProposalSummary struct {
	ID      string
	NodeID  string
	Attempt int
	Status  string // generating | verifying | proposed | skipped | failed | escalated
	PRState string // "" | opening | open | merged | rejected | failed
	PRURL   string
}

// ProposalReader lists the remediation attempts recorded for a release.
type ProposalReader interface {
	ListProposalsForRelease(ctx context.Context, releaseID string) ([]ProposalSummary, error)
}
