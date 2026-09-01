package ports

import "context"

// ProposalPR is one owning service's pull request within a remediation attempt.
// A batched attempt opens one pull request per owning service, so an attempt
// carries a set of these rather than a single pull request.
type ProposalPR struct {
	Service string
	PRState string // "" | opening | open | merged | rejected | failed
	PRURL   string
}

// ProposalSummary is the slice of a remediation attempt the retry decision needs.
type ProposalSummary struct {
	ID      string
	NodeID  string
	Attempt int
	Status  string // generating | verifying | proposed | skipped | failed | escalated
	// PRState and PRURL mirror the attempt's first pull request in service
	// order. They are the whole-attempt view a reader that predates the
	// per-service split sees; PullRequests carries every owning service's pull
	// request and is what the retry decision aggregates over.
	PRState string // "" | opening | open | merged | rejected | failed
	PRURL   string
	// PullRequests is one entry per owning service's pull request. Empty for an
	// attempt that never entered the pull-request lifecycle and for a legacy row
	// that carried only the singular PRState/PRURL.
	PullRequests []ProposalPR
	// PRServices is the owning-service groups this attempt's fix splits into.
	// Empty on a legacy row, in which case the retry decision falls back to the
	// services named by the effective pull requests.
	PRServices []string
	// RemediationRound is the release's remediation round this attempt belongs
	// to. 0 on an attempt recorded before the field existed, which the retry
	// decision treats the same as round 1.
	RemediationRound int
}

// EffectivePRs is the attempt's per-service pull requests: PullRequests when it
// carries them, otherwise a single synthesized entry from the singular
// PRState/PRURL. A legacy row (recorded before the per-service split) thus
// yields exactly one pull request and behaves as it did before.
func (s ProposalSummary) EffectivePRs() []ProposalPR {
	if len(s.PullRequests) > 0 {
		return s.PullRequests
	}
	return []ProposalPR{{Service: "", PRState: s.PRState, PRURL: s.PRURL}}
}

// ProposalReader lists the remediation attempts recorded for a release.
type ProposalReader interface {
	ListProposalsForRelease(ctx context.Context, releaseID string) ([]ProposalSummary, error)
}
