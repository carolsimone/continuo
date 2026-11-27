package event

// PROpened mirrors the fix.pr_opened:v1 wire payload agent-remediation
// emits when an operator opens a fix PR from a fix.
type PROpened struct {
	FixID     string
	ReleaseID string
	NodeID    string
	// ResolvedNodeIDs is every failing node the one PR fixes. NodeID is the
	// representative of that set.
	ResolvedNodeIDs []string
	PrURL           string
	PrNumber        int
	OpenedBy        string
	OpenedAt        string // RFC3339
	// Project is the project whose fix this PR carries. One PR always targets
	// exactly one project, shared by every node in ResolvedNodeIDs.
	Project string
}
