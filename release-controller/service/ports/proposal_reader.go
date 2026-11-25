package ports

import "context"

// ProposalPR is one owning project's pull request within a remediation attempt.
// A batched attempt opens one pull request per owning project, so an attempt
// carries a set of these rather than a single pull request.
type ProposalPR struct {
	Project string
	PRState string // "" | opening | open | merged | rejected | failed
	PRURL   string
}

// ProposalSummary is the slice of a remediation attempt the retry decision needs.
type ProposalSummary struct {
	ID      string
	NodeID  string
	Attempt int
	Status  string // generating | verifying | proposed | skipped | failed | escalated
	// PRState and PRURL mirror the attempt's first pull request in project
	// order — the whole-attempt view, for a reader that reads one state per
	// attempt. PullRequests carries every owning project's pull request and is
	// what the retry decision aggregates over.
	PRState string // "" | opening | open | merged | rejected | failed
	PRURL   string
	// PullRequests is one entry per owning project's pull request. Empty for an
	// attempt that never entered the pull-request lifecycle and for one that
	// carries only the singular PRState/PRURL.
	PullRequests []ProposalPR
	// PRProjects is the owning-project groups this attempt's fix splits into.
	// Empty on an attempt that names none, in which case the retry decision
	// falls back to the projects named by the effective pull requests.
	PRProjects []string
	// RemediationRound is the release's remediation round this attempt belongs
	// to. 0 on an attempt that names no round, which the retry decision treats
	// the same as round 1.
	RemediationRound int
}

// EffectivePRs is the attempt's per-project pull requests: PullRequests when it
// carries them, otherwise a single synthesized entry from the singular
// PRState/PRURL. An attempt whose fix is not split by project therefore yields
// exactly one pull request, named by the empty project.
func (s ProposalSummary) EffectivePRs() []ProposalPR {
	if len(s.PullRequests) > 0 {
		return s.PullRequests
	}
	return []ProposalPR{{Project: "", PRState: s.PRState, PRURL: s.PRURL}}
}

// ProposalReader lists the remediation attempts recorded for a release.
type ProposalReader interface {
	ListProposalsForRelease(ctx context.Context, releaseID string) ([]ProposalSummary, error)
}
