package event

// PRClosedEdit is one file edit a closed remediation PR carried, as it stood at
// the terminal outcome. Amended reports whether a human changed this edit
// before the PR merged; Diff is always the edit's proposal-time unified diff —
// the precedent read renders the merged-truth diff instead when the edit was
// amended. For a rejected PR the edits list is empty.
type PRClosedEdit struct {
	Path         string `json:"path"`
	TargetNodeID string `json:"target_node_id"`
	Amended      bool   `json:"amended"`
	Diff         string `json:"diff,omitempty"`
}

// PRClosed mirrors the remediation.pr_closed:v1 wire payload agent-remediation
// emits when a fix PR reaches its terminal outcome on GitHub. Outcome is
// "merged" or "rejected". A merged PR draws the case-base provenance edges
// (RESOLVED_BY per resolved node, EDITED per edit); a rejected PR only stamps
// the PullRequest's terminal state.
type PRClosed struct {
	ProposalID string `json:"proposal_id"`
	ReleaseID  string `json:"release_id"`
	NodeID     string `json:"node_id"`
	// ResolvedNodeIDs is every failing node this PR fixes. NodeID is the
	// representative of that set; a payload emitted before the field existed
	// carries only NodeID.
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	// Service is the owning-service group this PR covers; empty for a legacy
	// whole-proposal PR.
	Service  string `json:"service,omitempty"`
	PrURL    string `json:"pr_url"`
	PrNumber int    `json:"pr_number"`
	// Outcome is "merged" or "rejected".
	Outcome  string `json:"outcome"`
	ClosedAt string `json:"closed_at"` // RFC3339
	// Edits carries the per-file close detail on a merged PR, including which
	// edits a human amended before merge; empty for a rejected PR and for
	// legacy payloads emitted before the field existed.
	Edits []PRClosedEdit `json:"edits,omitempty"`
}
