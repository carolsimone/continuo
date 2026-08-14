// Read-side views over the case base, mirroring codeversion/read.go: reader
// rows first, then the rendered shape frontends receive.
package casebase

import "github.com/carolsimone/continuo/orchestrator/domain/codeversion"

// ProposalView is one fix PR attached to a precedent.
type ProposalView struct {
	ProposalID string
	PrURL      string
	PrNumber   int
	PrState    string
}

// PrecedentView is one reader-level precedent row: the rejection plus the
// resolving version and the version it superseded (next-older by promoted_at)
// so the service can render the resolution diff.
type PrecedentView struct {
	Rejection        Rejection
	ResolvingVersion *codeversion.VersionView
	PriorVersion     *codeversion.VersionView
	Proposals        []ProposalView
}

// Precedent is the rendered precedent entry served to frontends: the
// resolution rendered as a diff so every caller sees the same thing.
type Precedent struct {
	Rejection               Rejection
	Resolved                bool
	ResolvingVersion        *codeversion.VersionView
	ResolutionDiff          string
	ResolutionDiffTruncated bool
	Proposals               []ProposalView
}
