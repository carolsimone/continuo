package event

// PRClosedEdit is one file edit a closed remediation PR carried, as it stood at
// the terminal outcome. Amended reports whether a human changed this edit
// before the PR merged; Diff is always the edit's proposal-time unified diff —
// the precedent read renders the merged-truth diff instead when the edit was
// amended. For a rejected PR the edits list is empty.
type PRClosedEdit struct {
	Path         string
	TargetNodeID string
	Amended      bool
	Diff         string
}

// PRClosed mirrors the remediation.pr_closed:v1 wire payload agent-remediation
// emits when a fix PR reaches its terminal outcome on GitHub. Outcome is
// "merged" or "rejected". A merged PR draws the case-base provenance edges
// (RESOLVED_BY per resolved node, EDITED per edit); a rejected PR only stamps
// the PullRequest's terminal state.
type PRClosed struct {
	ProposalID string
	ReleaseID  string
	NodeID     string
	// ResolvedNodeIDs is every failing node this PR fixes. NodeID is the
	// representative of that set; a payload emitted before the field existed
	// carries only NodeID.
	ResolvedNodeIDs []string
	// Service is the owning-service group this PR covers; empty for a legacy
	// whole-proposal PR. Its DTO field is omitempty.
	Service  string
	PrURL    string
	PrNumber int
	// Outcome is "merged" or "rejected".
	Outcome  string
	ClosedAt string // RFC3339
	// Edits carries the per-file close detail on a merged PR, including which
	// edits a human amended before merge; empty for a rejected PR and for
	// legacy payloads emitted before the field existed. Its DTO field is omitempty.
	Edits []PRClosedEdit
}
