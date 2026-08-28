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
	// RemediationRound is the release's remediation round this attempt belongs
	// to. 0 on an attempt recorded before the field existed, which the retry
	// decision treats the same as round 1.
	RemediationRound int
}

// ProposalReader lists the remediation attempts recorded for a release.
type ProposalReader interface {
	ListProposalsForRelease(ctx context.Context, releaseID string) ([]ProposalSummary, error)
}
